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


if __name__ == "__main__":
    unittest.main()
