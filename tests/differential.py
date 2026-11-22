#!/usr/bin/env python3
"""Independent, deliberately simple oracle for generated datasets and mutations."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
DAY = 86_400_000
WEEK = 7 * DAY


def records(path, width):
    # Do not use universal-newline translation: CR is data, not a terminator.
    for line in path.read_bytes().decode("utf-8").split("\n")[1:]:
        fields = line.split(",")
        if len(fields) != width:
            continue
        decoded = []
        for field in fields:
            if field.startswith('"'):
                if len(field) < 2 or not field.endswith('"'):
                    break
                field = field[1:-1]
            if '"' in field:
                break
            decoded.append(field)
        if len(decoded) == width:
            yield decoded


def parts(folder, width):
    for path in sorted(folder.glob("*.csv")):
        yield from records(path, width)


def timestamp(value):
    if re.fullmatch(r"[0-9]+", value):
        return int(value)
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})", value):
        return None
    if len(value) == 25 and (int(value[20:22]) > 23 or int(value[23:25]) > 59):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        delta = dt.astimezone(UTC) - EPOCH
        return delta.days * DAY + delta.seconds * 1000
    except (ValueError, OverflowError):
        return None


def reference(data):
    manifest = json.loads((data / "manifest.json").read_text())
    begin, end = (timestamp(manifest[k]) for k in ("window_start", "window_end"))
    products = dict(records(data / "products.csv", 2))
    campaigns = dict(records(data / "campaigns.csv", 2))
    parents = {}

    def root(x):
        parents.setdefault(x, x)
        path = []
        while parents[x] != x:
            path.append(x)
            x = parents[x]
        for node in path:
            parents[node] = x
        return x

    for a, b in parts(data / "links", 2):
        if a and b:
            x, y = root(a), root(b)
            parents[max(x, y)] = min(x, y)

    earliest = {}
    for row in parts(data / "events", 7):
        event_id = row[0]
        if event_id and (event_id not in earliest or row[3] < earliest[event_id][3]):
            earliest[event_id] = row
    events = defaultdict(list)
    for _, user, ts, ingest, kind, product, price in earliest.values():
        ts = timestamp(ts)
        if not user or not product or ts is None or not begin <= ts < end or kind not in ("view", "cart", "purchase"):
            continue
        revenue = 0
        if kind == "purchase":
            if not re.fullmatch(r"[0-9]+", price) or not 1 <= int(price) <= 10_000_000:
                continue
            revenue = int(price)
        if timestamp(ingest) - ts > DAY:
            continue
        events[root(user)].append((ts, revenue, products.get(product, "unknown")))

    clicks = defaultdict(list)
    unique_clicks = {}
    for row in parts(data / "clicks", 4):
        if row[0]:
            unique_clicks[row[0]] = row
    for click_id, user, campaign, ts in unique_clicks.values():
        ts = timestamp(ts)
        if user and campaign in campaigns and ts is not None and begin - WEEK <= ts < end:
            clicks[root(user)].append((ts, click_id, campaign))

    users = {}
    credits = defaultdict(lambda: [0, 0, 0, 0])
    for person, ev in events.items():
        ev.sort()
        sessions = []
        for event in ev:
            if not sessions or event[0] - sessions[-1][-1][0] > 1_800_000:
                sessions.append([])
            sessions[-1].append(event)
        for session in sessions:
            start = session[0][0]
            day = (EPOCH + timedelta(milliseconds=start)).date()
            totals = users.setdefault((person, day), [0, 0, 0, defaultdict(int)])
            totals[0] += 1
            totals[1] += len(session)
            revenue = sum(e[1] for e in session)
            totals[2] += revenue
            for _, price, category in session:
                if price:
                    totals[3][category] += price
            qualifying = [c for c in clicks[person] if start - WEEK <= c[0] <= start]
            campaign = min(qualifying, key=lambda c: (-c[0], c[1]))[2] if qualifying else "organic"
            credit = credits[campaign, day]
            credit[0] += 1
            credit[1] += revenue > 0
            credit[2] += revenue
            for campaign in {c[2] for c in qualifying}:
                credits[campaign, day][3] += 1
    user_rows = Counter()
    for (person, day), (sessions, count, revenue, cats) in users.items():
        top = min(cats, key=lambda c: (-cats[c], c)) if cats else None
        user_rows[(person, day, sessions, count, revenue, top)] += 1
    campaign_rows = Counter((c, "organic" if c == "organic" else campaigns[c], d, *values) for (c, d), values in credits.items())
    return {"user_daily": user_rows, "campaign_daily": campaign_rows}


def actual(output, table):
    names = {
        "user_daily": ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
        "campaign_daily": ["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"],
    }[table]
    rows = Counter()
    for file in (output / table).glob("*.parquet"):
        for row in pq.read_table(file).to_pylist():
            rows[tuple(row[k] for k in names)] += 1
    return rows


def mutate(data):
    # Add an invalid earliest copy, CR data, unsigned overflow, Unicode digits,
    # quotes, byte-order ties, late boundary cases, and an unused canonical ID.
    manifest = json.loads((data / "manifest.json").read_text())
    start = manifest["window_start"]
    ms = timestamp(start)
    product = next(records(data / "products.csv", 2))[0]
    campaign = next(records(data / "campaigns.csv", 2))[0]
    with (data / "links" / "mutations.csv").open("w", newline="") as f:
        f.write('user_id_a,user_id_b\n" zz ",zz\nzz,aa_unused\nzz,é\n')
    with (data / "events" / "mutations.csv").open("w", newline="") as f:
        f.write("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")
        for i, (ts, price, ingest) in enumerate([
            (str(ms), "000250", start),
            (str(ms + 1), "٢٥٠", start),
            (str(ms + 2), "250\r", start),
            ("0" * 30 + str(ms + 3), "300", start),
            ("9" * 80, "400", start),
            (start + "\r", "500", start),
            (str(ms - DAY), "600", start),
        ]):
            f.write(f"mutation_{i},zz,{ts},{ingest},purchase,{product},{price}\n")
        f.write(f"mutation_bad,zz,bad,{start},view,{product},\n")
        later = (EPOCH + timedelta(milliseconds=ms + 1000)).isoformat().replace("+00:00", "Z")
        f.write(f"mutation_bad,zz,{start},{later},purchase,{product},99999\n")
    with (data / "clicks" / "mutations.csv").open("w", newline="") as f:
        f.write("click_id,user_id,campaign_id,ts\n")
        f.write(f"tie_é,é,{campaign},{ms}\n")
        f.write(f"tie_A,zz,{campaign},{ms}\n")
        f.write(f'"tie_A","zz","{campaign}","{ms}"\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", nargs="+", type=int, default=[2, 3, 4, 5, 29, 128, 2028, 9999])
    parser.add_argument("--events", type=int, default=20000)
    parser.add_argument("--no-mutations", action="store_true")
    parser.add_argument("--window-days", type=int)
    args = parser.parse_args()
    generator = ROOT / "tools/generator/target/release/generate"
    for seed in args.seeds:
        with tempfile.TemporaryDirectory() as tmp:
            data, out = Path(tmp) / "data", Path(tmp) / "output"
            subprocess.run([str(generator), "--seed", str(seed), "--events", str(args.events), "--clicks", str(args.events // 4), "--threads", "2", "--output", str(data)], check=True)
            if args.window_days is not None:
                manifest = json.loads((data / "manifest.json").read_text())
                end = EPOCH + timedelta(milliseconds=timestamp(manifest["window_start"]), days=args.window_days)
                manifest["window_end"] = end.isoformat().replace("+00:00", "Z")
                (data / "manifest.json").write_text(json.dumps(manifest))
            if not args.no_mutations:
                mutate(data)
            expected = reference(data)
            subprocess.run([str(ROOT / "build/shopping")], env=dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out), NCPU="8"), check=True, capture_output=True)
            for table, rows in expected.items():
                got = actual(out, table)
                if got != rows:
                    raise AssertionError((seed, table, "missing", list((rows-got).items())[:5], "extra", list((got-rows).items())[:5]))
            print(f"seed {seed}: exact match")


if __name__ == "__main__":
    main()
