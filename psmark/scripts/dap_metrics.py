#!/usr/bin/env python3
"""MQTT-DAP correctness metrics (paper 7.1) for one PSMark cell.

Reads the raw events every node wrote (nodes/<node>/results/*/raw_events) and, when
present, the broker's per-message metrics (broker_metrics.csv in the cell directory):

  Residual error: EAR, the share of deliveries the policy in force did not permit,
  and ERR, the share of messages it permitted throughout their delivery window that
  never arrived. The policy in force is what clients last sent: each subscriber's
  SP and each publisher topic's MP, recorded when they were sent, so delays in
  applying an update count as residual error. A DELETE or RESTRICT a publisher
  issued after a message also forbids it.

  Delivery Protection Rate (DPR): re-verifications at the broker's send-path gate
  over the deliveries it queued.

  Operational correctness: coverage (relevant subscribers that got the request),
  leakage (non-relevant subscribers that got it) and completion (requests whose
  requester got a final Success or Failure). A subscriber is relevant to a request
  if it received data from the requester under an SP sharing a purpose with the
  request's DAP-OpPFs (paper 6.1); PSMark scopes every operation to the requester's
  own topics and MPs. Coverage counts subscribers that had such data before the
  request was issued. A request is leaked if it reached a subscriber before any of
  the requester's data did, on the same connection.

Usage: dap_metrics.py <cell dir> [--json <file>]
"""
import argparse
import bisect
import csv
import glob
import json
import os
import sys
from collections import defaultdict


def name(value):
    """A client or node name as written by Erlang's ~p, unwrapped."""
    value = value.strip()
    if value.startswith("<<") and value.endswith(">>"):
        value = value[2:-2]
    return value.strip('"')


def split_collection(filters):
    """Split a purpose-filter collection on '|' or ',' outside braces."""
    out, depth, cur = [], 0, ""
    for ch in filters:
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        if ch in "|," and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += ch
    out.append(cur)
    return [f.strip() for f in out if f.strip()]


def expand(term):
    """The purposes one filter describes ('{a,b}' sets, '.' for the parent level)."""
    if "{" not in term:
        return [term]
    i = term.index("{")
    j = term.index("}", i)
    head, tail = term[:i], term[j + 1:]
    out = []
    for alt in term[i + 1:j].split(","):
        out += expand((head[:-1] if alt == "." else head + alt) + tail)
    return out


def purposes(filters):
    return frozenset(p for f in split_collection(filters or "") for p in expand(f))


def permits(mp, sp):
    """Paper 4.2: the MP permits every purpose of a non-empty SP, or is '*'."""
    return bool(sp) and mp is not None and ("*" in mp or sp <= mp)


class Timeline:
    """The value last set at or before a time."""

    def __init__(self):
        self.times, self.values = [], []

    def add(self, t, value):
        i = bisect.bisect_right(self.times, t)
        self.times.insert(i, t)
        self.values.insert(i, value)

    def at(self, t):
        i = bisect.bisect_right(self.times, t)
        return self.values[i - 1] if i else None

    def changes_between(self, t0, t1):
        return bisect.bisect_right(self.times, t1) - bisect.bisect_right(self.times, t0)


def topic_matches(filt, topic):
    f, t = filt.split("/"), topic.split("/")
    for i, part in enumerate(f):
        if part == "#":
            return True
        if i >= len(t) or (part != "+" and part != t[i]):
            return False
    return len(f) == len(t)


def read_rows(pattern):
    for f in glob.glob(pattern):
        with open(f, newline="") as fh:
            yield from csv.DictReader(fh)


def load(cell):
    raw = f"{cell}/nodes/*/results/*/raw_events"
    pubs = {}
    for r in read_rows(f"{raw}/*_pub_events.csv"):
        pubs[(name(r["SenderID"]), name(r["Topic"]), r["SeqId"])] = (name(r["SendingClient"]), int(r.get("PubTimeNs") or 0))
    recvs = []
    for r in read_rows(f"{raw}/*_recv_events.csv"):
        recvs.append((name(r["ReceiverID"]), name(r["ReceivingClient"]), (name(r["SenderID"]), name(r["Topic"]), r["SeqId"]),
                      int(r["PubTimeNs"]), int(r["RecvTimeNs"])))
    events = sorted(({**r, "TimeNs": int(r["TimeNs"])} for r in read_rows(f"{raw}/*_dap_events.csv")),
                    key=lambda r: r["TimeNs"])
    return pubs, recvs, events


def residual_error(pubs, recvs, events):
    sp = defaultdict(Timeline)
    mp = defaultdict(Timeline)
    ops = defaultdict(list)  # requester client -> [(time, type, topic filters, revoked purposes)]
    for e in events:
        if e["Event"] == "sp_subscribe":
            sp[e["Client"]].add(e["TimeNs"], purposes(e["Detail"]))
        elif e["Event"] == "mp_register":
            mp[(e["Client"], e["Topic"])].add(e["TimeNs"], purposes(e["Detail"]))
        elif e["Event"] == "op_issue" and e["OpType"] in ("DELETE", "RESTRICT"):
            ops[e["Client"]].append((e["TimeNs"], e["OpType"], split_collection(e["Topic"]), purposes(e["Detail"])))
    if not sp:
        return None
    op_times = {c: [o[0] for o in sorted(lst)] for c, lst in ops.items()}
    ops = {c: sorted(lst) for c, lst in ops.items()}

    def forbidden_by_op(client, topic, t0, t1, sub_sp):
        lst = ops.get(client, ())
        if not lst:
            return False
        times = op_times[client]
        for to, kind, tfs, pfs in lst[bisect.bisect_right(times, t0):bisect.bisect_right(times, t1)]:
            if any(f == "*" or topic_matches(f, topic) for f in tfs):
                if kind == "DELETE" or not pfs or "*" in pfs or (sub_sp & pfs):
                    return True
        return False

    def allowed(client, topic, tp, sub, t):
        s = sp[sub].at(t)
        return (permits(mp[(client, topic)].at(tp), s) and permits(mp[(client, topic)].at(t), s)
                and not forbidden_by_op(client, topic, tp, t, s or frozenset()))

    # A message's payload carries its publish time; PSMark records the publish event
    # only once the publish call returns, so that time is for unreceived messages.
    sent_at = {key: tp for _, _, key, tp, _ in recvs}
    delivered = erroneous_acc = 0
    got = defaultdict(set)
    last_recv = {}
    latencies = []
    for node, sub, key, tp, tr in recvs:
        if key not in pubs or sub not in sp:
            continue
        client = pubs[key][0]
        delivered += 1
        got[key].add(sub)
        last_recv[sub] = max(last_recv.get(sub, tr), tr)
        latencies.append(tr - tp)
        if not allowed(client, key[1], tp, sub, tr):
            erroneous_acc += 1

    latencies.sort()
    window = latencies[int(len(latencies) * 0.99)] if latencies else 0
    expected = erroneous_rej = racing = 0
    first_sp = {s: tl.times[0] for s, tl in sp.items()}
    for key, (client, recorded) in pubs.items():
        tp = sent_at.get(key, recorded)
        if not tp:
            continue
        for sub in sp:
            if not (first_sp[sub] <= tp <= last_recv.get(sub, 0)):
                continue  # not subscribed yet, or the run had ended for it
            if not allowed(client, key[1], tp, sub, tp):
                continue
            if (not allowed(client, key[1], tp, sub, tp + window)
                    or sp[sub].changes_between(tp, tp + window) or mp[(client, key[1])].changes_between(tp, tp + window)):
                racing += 1  # the policy changed while it was in flight: either outcome is valid
                continue
            expected += 1
            if sub not in got[key]:
                erroneous_rej += 1
    return {
        "delivered": delivered,
        "erroneous_acceptances": erroneous_acc,
        "EAR_pct": 100.0 * erroneous_acc / delivered if delivered else 0.0,
        "expected": expected,
        "erroneous_rejections": erroneous_rej,
        "ERR_pct": 100.0 * erroneous_rej / expected if expected else 0.0,
        "racing_excluded": racing,
        "delivery_window_ms": window / 1e6,
    }


def delivery_protection(cell):
    path = os.path.join(cell, "broker_metrics.csv")
    if not os.path.exists(path):
        return None
    queued = reverified = messages = bumped = 0
    for r in read_rows(path):
        messages += 1
        queued += int(r["num_subs_matched"])
        b = int(r["bump_count"])
        reverified += b
        bumped += b > 0
    return {"messages": messages, "queued_deliveries": queued, "reverifications": reverified,
            "messages_reverified": bumped, "DPR_pct": 100.0 * reverified / queued if queued else 0.0}


def operations(pubs, recvs, events):
    issued = [e for e in events if e["Event"] == "op_issue"]
    if not issued:
        return None
    node_of = {sub: node for node, sub, *_ in recvs}
    subscribers = set(node_of.values())
    sp = defaultdict(Timeline)
    for e in events:
        if e["Event"] == "sp_subscribe":
            sp[node_of.get(e["Client"], e["Client"])].add(e["TimeNs"], purposes(e["Detail"]))
    data = defaultdict(list)  # (requester, subscriber) -> sorted [(received, published, SP then)]
    for node, sub, key, tp, tr in recvs:
        if key in pubs:
            data[(pubs[key][0], node)].append((tr, tp, sp[node].at(tr) if node in sp else None))
    for lst in data.values():
        lst.sort(key=lambda x: x[0])

    def concerned(pfs, sub_sp):
        """Paper 6.1: the SP the data was received under shares a purpose with DAP-OpPFs."""
        return not pfs or "*" in pfs or sub_sp is None or bool(sub_sp & pfs)

    def surely_relevant(requester, t_issue, pfs):
        """Received the requester's data, under an SP the operation concerns, before it was issued."""
        return {s for s in subscribers
                if any(tr < t_issue and concerned(pfs, ssp) for tr, _, ssp in data.get((requester, s), ()))}

    def possibly_relevant(requester, t_issue, pfs):
        """Received, possibly later, such data the requester published before the operation."""
        return {s for s in subscribers
                if any(tp < t_issue and concerned(pfs, ssp) for _, tp, ssp in data.get((requester, s), ()))}

    issue_times = defaultdict(list)
    op_pfs = {}
    for e in issued:
        issue_times[(e["Client"], e["OpType"])].append(e["TimeNs"])
        op_pfs[(e["Client"], e["TimeNs"])] = purposes(e["Detail"])

    def issued_at(requester, op_type, t):
        times = issue_times.get((requester, op_type), [])
        i = bisect.bisect_right(times, t)
        return times[i - 1] if i else t

    pending, final_ids, finals, audits = {}, set(), 0, []
    for e in events:
        if e["Event"] != "op_notification" or e["Sender"] != "Broker":
            continue
        requester = e["Topic"].split("/", 1)[1]
        if e["Status"] == "Pending" and e["OpId"]:
            pending[e["OpId"]] = (requester, issued_at(requester, e["OpType"], e["TimeNs"]))
        elif e["Status"] in ("Success", "Failure"):
            if e["OpType"] == "AUDIT":
                audits.append((requester, issued_at(requester, "AUDIT", e["TimeNs"]), e["Status"], e["Detail"]))
                finals += 1
            elif e["OpId"]:
                if e["OpId"] not in final_ids:
                    final_ids.add(e["OpId"])
                    finals += 1
            else:
                finals += 1  # refused before getting an op id

    # A request reaching a subscriber after the requester's data, on the same
    # connection, was routed after that data: the subscriber was relevant.
    sent = defaultdict(set)
    leaked = 0
    for e in events:
        if e["Event"] == "op_request" and e["OpId"] in pending:
            requester = pending[e["OpId"]][0]
            sub = node_of.get(e["Client"], e["Client"])
            sent[e["OpId"]].add(sub)
            if not any(tr < e["TimeNs"] for tr, _, _ in data.get((requester, sub), ())):
                leaked += 1
    covered = relevant_slots = other_slots = 0
    for op_id, (requester, t_issue) in pending.items():
        rel = surely_relevant(requester, t_issue, op_pfs.get((requester, t_issue)))
        covered += len(rel & sent[op_id])
        relevant_slots += len(rel)
        other_slots += len(subscribers - rel)
    audit_ok = 0
    for requester, t_issue, status, detail in audits:
        listed = {node_of.get(x, x) for x in detail.split(",") if x} if status == "Success" else set()
        pfs = op_pfs.get((requester, t_issue))
        audit_ok += surely_relevant(requester, t_issue, pfs) <= listed <= possibly_relevant(requester, t_issue, pfs)
    by_type = defaultdict(int)
    for e in issued:
        by_type[e["OpType"]] += 1
    return {
        "issued": len(issued),
        "by_type": dict(by_type),
        "requesters": len({e["Client"] for e in issued}),
        "completed": finals,
        "completion_pct": 100.0 * finals / len(issued),
        "pending_at_end": len(set(pending) - final_ids),
        "coverage_pct": 100.0 * covered / relevant_slots if relevant_slots else None,
        "relevant_slots": relevant_slots,
        "leakage_pct": 100.0 * leaked / other_slots if other_slots else 0.0,
        "leaked": leaked,
        "audits": len(audits),
        "audits_listing_relevant": audit_ok,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("cell")
    ap.add_argument("--json")
    args = ap.parse_args()
    pubs, recvs, events = load(args.cell)
    result = {"cell": os.path.basename(os.path.normpath(args.cell)),
              "residual_error": residual_error(pubs, recvs, events),
              "delivery_protection": delivery_protection(args.cell),
              "operations": operations(pubs, recvs, events)}
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(result, fh, indent=1)
    re_, dp, op = result["residual_error"], result["delivery_protection"], result["operations"]
    print(result["cell"])
    if re_:
        print(f"  EAR {re_['EAR_pct']:.3f}% ({re_['erroneous_acceptances']}/{re_['delivered']} deliveries), "
              f"ERR {re_['ERR_pct']:.3f}% ({re_['erroneous_rejections']}/{re_['expected']} expected; "
              f"{re_['racing_excluded']} raced a policy change within {re_['delivery_window_ms']:.1f} ms)")
    if dp:
        print(f"  DPR {dp['DPR_pct']:.3f}% ({dp['reverifications']} re-verifications over {dp['queued_deliveries']} queued deliveries)")
    if op:
        cov = "-" if op["coverage_pct"] is None else f"{op['coverage_pct']:.1f}%"
        print(f"  operations {op['issued']} {op['by_type']} by {op['requesters']} requester(s): "
              f"completion {op['completion_pct']:.1f}%, coverage {cov} of {op['relevant_slots']}, "
              f"leakage {op['leakage_pct']:.1f}% ({op['leaked']}), pending at end {op['pending_at_end']}")
        if op["audits"]:
            print(f"  AUDIT answers listing exactly the relevant subscribers: {op['audits_listing_relevant']}/{op['audits']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
