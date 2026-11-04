#!/usr/bin/env python3
"""Generate the Ex 3-7 scenarios from the test plan CSV, using the Ex 2 DAP files as templates.
Usage: gen_dynamic_scenarios.py <updated_cases.csv> <configs/DAP/testcases>. Prints the runner cells."""
import csv
import glob
import os
import re
import sys

EXPERIMENTS = {"Dynamism": "ex3_dynamism", "Operations: NC": "ex4_ops_nc", "Operations: C": "ex5_ops_c",
               "Bursty": "ex6_bursty", "Failures": "ex7_failures"}
SCALES = {"1 per P / 1 per S": "1p1s", "5 per P / 1 per S": "5p1s", "10 per P / 1 per S": "10p1s"}
ROTATION = {"No": None, "Pub Rotate": "pub", "Sub Rotate": "sub", "Both Rotate": "both"}
QUEUE = {"Formula * 1": 1000, "Formula * 1.5": 1500, "Formula * 2": 2000}


def expand(p):
    m = re.search(r"\{([^{}]*)\}", p)
    if not m:
        return [p]
    return [x for alt in m.group(1).split(",") for x in expand(p[:m.start()] + alt + p[m.end():])]


def purposes(text):
    block = re.search(r"\{device_purposes,.*?\]\},\s*\{node_subscription_purposes", text, re.S).group(0)
    found = {x for s in re.findall(r'"([^"]+)"', block) for x in expand(s)}
    return sorted(found)


def main(plan, root):
    templates = {os.path.basename(f)[:-9]: f for f in glob.glob(os.path.join(root, "ex2_scalability/DAP/**/*.scenario"), recursive=True)}
    cells = []
    for r in csv.DictReader(open(plan, encoding="utf-8-sig")):
        exp = EXPERIMENTS.get(r["Experiment Number"])
        if not exp:
            continue
        dev = r["Workload"].split()[0]
        scale, subs, qos = SCALES[r["Purpose Scale"]], r["# Subs"], r["QoS"]
        mode = ROTATION[r["Purpose Dynamism"]]
        rate = r["Op Input Rate"].split("/")[0] if r["Include Operations?"] == "Yes" else None
        queue = QUEUE[r["Queue Sizes"]]
        name = f"{exp.split('_')[0]}_{dev}dev_{subs}sub_{scale}_qos{qos}"
        name += f"_{mode}rotate" if mode else ""
        name += f"_{rate}ops" if rate else ""
        name += f"_q{queue}_dap" if r["Bursty"] == "Yes" or r["Device Connectivity"] == "Yes" else "_dap"
        text = open(templates[f"ex2_{dev}dev_{subs}sub_{scale}_qos2_dap"]).read()
        text = re.sub(r"\{name, \w+\}", f"{{name, {name}}}", text, 1)
        text = re.sub(r"\{default_qos, \d\}", f"{{default_qos, {qos}}}", text, 1)
        extra = []
        if mode:
            plist = ", ".join(f'"{p}"' for p in purposes(text))
            extra += ["{purpose_rotation_window_ms, 10000}", f"{{purpose_rotation_mode, {mode}}}", f"{{purpose_rotation_list, [{plist}]}}"]
        if rate:
            group = "no_queue" if r["Op Types"] == "Non-changing" else "queue"
            extra += ["{op_issue_interval_ms, 1000}", f"{{op_rate_per_s, {rate}}}", f"{{op_group, {group}}}"]
        if r["Bursty"] == "Yes":
            extra.append("{publish_burst, {20000, 40000, 1.5}}")
        if r["Device Connectivity"] == "Yes":
            extra.append("{subscriber_outage, {20000, 40000, 0.5}}")
        insert = "".join(f"        {e},\n" for e in extra)
        text, n = re.subn(r"(\{port, 18883\},\n)", lambda m: m.group(1) + insert, text, 1)
        assert n == 1, name
        if r["Bursty"] == "Yes" or r["Device Connectivity"] == "Yes":
            text = f"% max_queued_messages {queue}\n" + text
        out = os.path.join(root, exp, f"{dev}devs", f"{name}.scenario")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        open(out, "w").write(text)
        cells.append(f'  "dap:{name}"')
    print("\n".join(cells))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
