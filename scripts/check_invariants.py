#!/usr/bin/env python3
"""Streaming checks for full-size outputs when no published answer is available."""
from pathlib import Path
import sys

import pyarrow.parquet as pq


def rows(folder):
    for file in sorted(folder.glob("*.parquet")):
        if file.name.startswith((".", "_")) or file.is_symlink():
            continue
        for batch in pq.ParquetFile(file).iter_batches(batch_size=65536):
            yield from batch.to_pylist()


def main():
    output = Path(sys.argv[1])
    user_sessions = user_revenue = user_rows = 0
    for r in rows(output / "user_daily"):
        assert 1 <= r["sessions"] <= r["events"], r
        assert r["revenue_cents"] >= 0, r
        assert (r["top_category"] is None) == (r["revenue_cents"] == 0), r
        user_sessions += r["sessions"]
        user_revenue += r["revenue_cents"]
        user_rows += 1
    campaign_sessions = campaign_revenue = assists = campaign_rows = 0
    keys = set()
    for r in rows(output / "campaign_daily"):
        key = r["campaign_id"], r["day"]
        assert key not in keys, key
        keys.add(key)
        assert 0 <= r["purchasing_sessions"] <= r["sessions"], r
        assert (r["revenue_cents"] > 0) == (r["purchasing_sessions"] > 0), r
        if r["campaign_id"] == "organic":
            assert r["channel"] == "organic" and r["assisted_sessions"] == 0, r
        else:
            assert r["assisted_sessions"] >= r["sessions"], r
        assert r["sessions"] > 0 or r["assisted_sessions"] > 0, r
        campaign_sessions += r["sessions"]
        campaign_revenue += r["revenue_cents"]
        assists += r["assisted_sessions"]
        campaign_rows += 1
    assert user_sessions == campaign_sessions, (user_sessions, campaign_sessions)
    assert user_revenue == campaign_revenue, (user_revenue, campaign_revenue)
    print(f"PASS: {user_rows:,} user rows; {campaign_rows:,} campaign rows; "
          f"{user_sessions:,} sessions; {user_revenue:,} cents; {assists:,} assists")


if __name__ == "__main__":
    main()
