#!/usr/bin/env python3
"""Tests for dap_metrics.delivery_protection. Run: python3 scripts/test_dap_metrics.py"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dap_metrics  # noqa: E402


def cell_with(lines):
    d = tempfile.mkdtemp()
    with open(os.path.join(d, "broker_metrics.csv"), "w") as f:
        f.write("\n".join(lines) + "\n")
    return d


class DeliveryProtection(unittest.TestCase):
    def test_counts_reevaluations_from_the_dap_broker(self):
        got = dap_metrics.delivery_protection(cell_with([
            "recv_time_ns,publisher_id,topic,num_subs_matched,processing_time_ns,reeval_count",
            "1,p,t,4,10,1",
            "2,p,t,4,10,0",
        ]))
        self.assertEqual(got["queued_deliveries"], 8)
        self.assertEqual(got["reverifications"], 1)
        self.assertEqual(got["messages_reverified"], 1)
        self.assertEqual(got["DPR_pct"], 12.5)

    def test_stock_broker_has_no_dpr(self):
        self.assertIsNone(dap_metrics.delivery_protection(cell_with([
            "recv_time_ns,publisher_id,topic,processing_time_ns",
            "1,p,t,10",
        ])))


MS = 10**6
TOPIC = "t"
KEY = ("n", TOPIC, "1")


def ev(t, event, client, detail="", topic=TOPIC, op=""):
    return {"TimeNs": t, "Event": event, "Client": client, "Topic": topic, "Detail": detail, "OpType": op}


def policy(sp="a", mp="a"):
    return [ev(0, "sp_subscribe", "sub", sp), ev(0, "mp_register", "pub", mp)]


def score(events, recv_at=None, offline=None, pub_t=100):
    """One message published at pub_t ms, among others that set a 20 ms delivery window
    and keep the subscriber active to the end of the run."""
    pubs, recvs = {KEY: ("pub", pub_t * MS)}, []
    for i in range(300):
        key, tp = ("n", TOPIC, f"o{i}"), (980 if i == 0 else 1) * MS
        pubs[key] = ("pub", tp)
        recvs.append(("s", "sub", key, tp, tp + 20 * MS))
    if recv_at is not None:
        recvs.append(("s", "sub", KEY, pub_t * MS, recv_at * MS))
    return dap_metrics.residual_error(pubs, recvs, sorted(events, key=lambda e: e["TimeNs"]), offline)


class WrongDeliveries(unittest.TestCase):
    def test_a_message_keeps_its_mp(self):
        got = score(policy() + [ev(101 * MS, "mp_register", "pub", "z"), ev(900 * MS, "mp_register", "pub", "a")], recv_at=200)
        self.assertEqual(got["erroneous_acceptances"], 0)

    def test_mp_at_publish_must_permit_the_sp(self):
        events = policy() + [ev(50 * MS, "mp_register", "pub", "z"), ev(150 * MS, "mp_register", "pub", "a")]
        self.assertEqual(score(events, recv_at=110)["erroneous_acceptances"], 1)

    def test_delivery_during_an_sp_change_is_not_wrong(self):
        got = score(policy() + [ev(105 * MS, "sp_subscribe", "sub", "z"), ev(900 * MS, "sp_subscribe", "sub", "a")], recv_at=110)
        self.assertEqual(got["erroneous_acceptances"], 0)

    def test_delete_only_covers_subscribers_in_its_purposes(self):
        got = score(policy() + [ev(120 * MS, "op_issue", "pub", "z", op="DELETE")], recv_at=500)
        self.assertEqual(got["erroneous_acceptances"], 0)

    def test_delete_covering_the_subscriber_is_wrong_after_the_window(self):
        got = score(policy() + [ev(120 * MS, "op_issue", "pub", "a", op="DELETE")], recv_at=500)
        self.assertEqual(got["erroneous_acceptances"], 1)

    def test_delete_inside_the_window_is_racing(self):
        got = score(policy() + [ev(105 * MS, "op_issue", "pub", "a", op="DELETE")], recv_at=110)
        self.assertEqual(got["erroneous_acceptances"], 0)
        self.assertEqual(got["racing_acceptances"], 1)

    def test_restrict_to_a_purpose_the_subscriber_uses_allows_it(self):
        got = score(policy(mp="a|b") + [ev(120 * MS, "op_issue", "pub", "a,b", op="RESTRICT")], recv_at=500)
        self.assertEqual(got["erroneous_acceptances"], 0)

    def test_restrict_with_no_registered_purposes_allows_everything(self):
        got = score(policy() + [ev(120 * MS, "op_issue", "pub", "*", topic="*", op="RESTRICT")], recv_at=500)
        self.assertEqual(got["erroneous_acceptances"], 0)

    def test_restrict_to_another_purpose_is_wrong(self):
        events = policy(sp="b", mp="a|b") + [ev(120 * MS, "op_issue", "pub", "a,b", op="RESTRICT")]
        self.assertEqual(score(events, recv_at=500)["erroneous_acceptances"], 1)


class MissedMessages(unittest.TestCase):
    OFFLINE = {"sub": [(50 * MS, 300 * MS)]}

    def test_missed_while_online_counts(self):
        self.assertEqual(score(policy(), pub_t=10)["erroneous_rejections"], 1)

    def test_sp_change_on_return_makes_it_racing(self):
        events = policy() + [ev(300 * MS, "sp_subscribe", "sub", "z")]
        got = score(events, offline=self.OFFLINE)
        self.assertEqual(got["erroneous_rejections"], 0)
        self.assertEqual(got["racing_excluded"], 1)

    def test_delete_while_offline_makes_it_racing(self):
        events = policy() + [ev(200 * MS, "op_issue", "pub", "a", op="DELETE")]
        self.assertEqual(score(events, offline=self.OFFLINE)["erroneous_rejections"], 0)

    def test_queued_message_lost_with_no_policy_change_counts(self):
        self.assertEqual(score(policy(), offline=self.OFFLINE)["erroneous_rejections"], 1)


if __name__ == "__main__":
    unittest.main()
