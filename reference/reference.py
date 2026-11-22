#!/usr/bin/env python3
"""
Slow, literal reference implementation of rules.md. Used only to cross-check
the fast C++ pipeline on many generated datasets. Works on raw bytes so that
every comparison is byte-wise, exactly as the rules say.

Usage: reference.py [DATA_DIR OUTPUT_DIR]   (or env DATA_DIR / OUTPUT_DIR)
Writes OUTPUT_DIR/user_daily/part-0.parquet and campaign_daily/part-0.parquet
(needs pyarrow), or CSVs with --csv.
"""
import bisect
import datetime as dt
import glob
import json
import os
import sys

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS
DIG = b"0123456789"


def is_digits(b):
    return len(b) > 0 and all(c in DIG for c in b)


def parse_line(line, nfields):
    """Return list of fields (bytes, b'' = null) or None if malformed."""
    if line == b"":
        return None
    parts = line.split(b",")
    if len(parts) != nfields:
        return None
    out = []
    for p in parts:
        if b'"' in p:
            if len(p) >= 2 and p[0:1] == b'"' and p[-1:] == b'"' and b'"' not in p[1:-1]:
                out.append(p[1:-1])
            else:
                return None
        else:
            out.append(p)
    return out


def read_csv_dir(paths, nfields):
    for path in paths:
        with open(path, "rb") as f:
            data = f.read()
        lines = data.split(b"\n")
        if lines and lines[-1] == b"":
            lines.pop()  # trailing newline
        for line in lines[1:]:  # first line is the header
            row = parse_line(line, nfields)
            if row is not None:
                yield row


def is_leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


MDAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def days_from_civil(y, m, d):
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def parse_ts(b):
    """Return epoch ms (int, may be huge) or None if bad."""
    if is_digits(b):
        return int(b)
    n = len(b)
    if n not in (20, 25):
        return None
    s = b
    def num(a, z):
        seg = s[a:z]
        return int(seg) if is_digits(seg) else None
    if s[4:5] != b"-" or s[7:8] != b"-" or s[10:11] != b"T" or s[13:14] != b":" or s[16:17] != b":":
        return None
    y, mo, d, h, mi, se = num(0, 4), num(5, 7), num(8, 10), num(11, 13), num(14, 16), num(17, 19)
    if None in (y, mo, d, h, mi, se):
        return None
    if not (1 <= mo <= 12):
        return None
    md = 29 if (mo == 2 and is_leap(y)) else MDAYS[mo - 1]
    if not (1 <= d <= md) or h > 23 or mi > 59 or se > 59:
        return None
    off = 0
    if n == 20:
        if s[19:20] != b"Z":
            return None
    else:
        sign = s[19:20]
        if sign not in (b"+", b"-") or s[22:23] != b":":
            return None
        oh, om = num(20, 22), num(23, 25)
        if oh is None or om is None or oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        if sign == b"-":
            off = -off
    ms = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000
    return ms - off


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    as_csv = "--csv" in sys.argv
    data = args[0] if len(args) >= 2 else os.environ.get("DATA_DIR", "/data")
    out = args[1] if len(args) >= 2 else os.environ.get("OUTPUT_DIR", "/output")

    with open(os.path.join(data, "manifest.json")) as f:
        man = json.load(f)
    ws = parse_ts(man["window_start"].encode())
    we = parse_ts(man["window_end"].encode())

    products = {}
    for row in read_csv_dir([os.path.join(data, "products.csv")], 2):
        products[row[0]] = row[1]
    campaigns = {}
    for row in read_csv_dir([os.path.join(data, "campaigns.csv")], 2):
        campaigns[row[0]] = row[1]

    # ---- links: union-find, name = smallest id
    parent = {}

    def find(x):
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while parent.get(x, x) != root:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        # keep the smaller id as root so the root is the person's name
        if rb < ra:
            ra, rb = rb, ra
        parent[rb] = ra
        parent.setdefault(ra, ra)

    for a, b in read_csv_dir(sorted(glob.glob(os.path.join(data, "links", "*.csv"))), 2):
        if a == b"" or b == b"":
            continue
        parent.setdefault(a, a)
        parent.setdefault(b, b)
        union(a, b)

    def person(u):
        return find(u) if u in parent else u

    # ---- events
    best = {}  # event_id -> (ingest_ms, row)
    for row in read_csv_dir(sorted(glob.glob(os.path.join(data, "events", "*.csv"))), 7):
        eid = row[0]
        if eid == b"":
            continue
        ing = parse_ts(row[3])
        assert ing is not None, row
        cur = best.get(eid)
        if cur is None or ing < cur[0]:
            best[eid] = (ing, row)

    per_person = {}
    for eid, (ing, row) in best.items():
        _, uid, tsb, _, etype, pid, price = row
        if uid == b"" or pid == b"":
            continue
        ts = parse_ts(tsb)
        if ts is None or not (ws <= ts < we):
            continue
        if etype not in (b"view", b"cart", b"purchase"):
            continue
        pc = 0
        if etype == b"purchase":
            if not is_digits(price):
                continue
            pc = int(price)
            if not (1 <= pc <= 10_000_000):
                continue
        if ing - ts > DAY_MS:
            continue
        cat = products.get(pid, b"unknown")
        per_person.setdefault(person(uid), []).append((ts, etype == b"purchase", pc, cat))

    # ---- clicks
    clicks = {}
    for cid, uid, camp, tsb in read_csv_dir(sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))), 4):
        if cid == b"" or cid in clicks:
            continue
        clicks[cid] = (uid, camp, tsb)
    pclicks = {}
    for cid, (uid, camp, tsb) in clicks.items():
        if uid == b"" or camp not in campaigns:
            continue
        ts = parse_ts(tsb)
        if ts is None or not (ws - WEEK_MS <= ts < we):
            continue
        pclicks.setdefault(person(uid), []).append((ts, cid, camp))
    for v in pclicks.values():
        v.sort()

    # ---- sessions
    user_daily = {}      # (user, day) -> [sessions, events, revenue, {cat: cents}]
    camp_daily = {}      # (camp, day) -> [sessions, purchasing, revenue, assisted]
    for p, evs in per_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > GAP_MS:
                sessions.append([e[0], 0, 0, {}, False])
            s = sessions[-1]
            s[1] += 1
            if e[1]:
                s[2] += e[2]
                s[3][e[3]] = s[3].get(e[3], 0) + e[2]
                s[4] = True
            prev = e[0]
        cl = pclicks.get(p, [])
        cts = [c[0] for c in cl]
        for start, nev, rev, cats, purch in sessions:
            day = start // DAY_MS
            ud = user_daily.setdefault((p, day), [0, 0, 0, {}])
            ud[0] += 1
            ud[1] += nev
            ud[2] += rev
            for c, v in cats.items():
                ud[3][c] = ud[3].get(c, 0) + v
            lo = bisect.bisect_left(cts, start - WEEK_MS)
            hi = bisect.bisect_right(cts, start)
            window = cl[lo:hi]
            if window:
                latest = window[-1][0]
                credit = min(c[1] for c in window if c[0] == latest)
                credit_camp = next(c[2] for c in window if c[0] == latest and c[1] == credit)
                for camp in {c[2] for c in window}:
                    camp_daily.setdefault((camp, day), [0, 0, 0, 0])[3] += 1
            else:
                credit_camp = b"organic"
            cd = camp_daily.setdefault((credit_camp, day), [0, 0, 0, 0])
            cd[0] += 1
            cd[1] += 1 if purch else 0
            cd[2] += rev

    def day_str(d):
        return (dt.date(1970, 1, 1) + dt.timedelta(days=d)).isoformat()

    def top(cats):
        if not cats:
            return None
        m = max(cats.values())
        return min(c for c, v in cats.items() if v == m)

    urows = [(u.decode("utf-8", "surrogateescape"), day, v[0], v[1], v[2],
              None if top(v[3]) is None else top(v[3]).decode("utf-8", "surrogateescape"))
             for (u, day), v in user_daily.items()]
    crows = [(c.decode(), (campaigns.get(c, b"organic")).decode(), day, v[0], v[1], v[2], v[3])
             for (c, day), v in camp_daily.items()]

    os.makedirs(os.path.join(out, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(out, "campaign_daily"), exist_ok=True)
    if as_csv:
        with open(os.path.join(out, "user_daily.csv"), "w", encoding="utf-8", newline="") as f:
            f.write("user_id,day,sessions,events,revenue_cents,top_category\n")
            for r in sorted(urows, key=lambda r: (r[0], r[1])):
                f.write(f"{r[0]},{day_str(r[1])},{r[2]},{r[3]},{r[4]},{r[5] or ''}\n")
        with open(os.path.join(out, "campaign_daily.csv"), "w", encoding="utf-8", newline="") as f:
            f.write("campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
            for r in sorted(crows, key=lambda r: (r[0], r[2])):
                f.write(f"{r[0]},{r[1]},{day_str(r[2])},{r[3]},{r[4]},{r[5]},{r[6]}\n")
        return
    import pyarrow as pa
    import pyarrow.parquet as pq
    epoch = dt.date(1970, 1, 1)
    ut = pa.table({
        "user_id": pa.array([r[0] for r in urows], pa.string()),
        "day": pa.array([epoch + dt.timedelta(days=r[1]) for r in urows], pa.date32()),
        "sessions": pa.array([r[2] for r in urows], pa.int64()),
        "events": pa.array([r[3] for r in urows], pa.int64()),
        "revenue_cents": pa.array([r[4] for r in urows], pa.int64()),
        "top_category": pa.array([r[5] for r in urows], pa.string()),
    })
    ct = pa.table({
        "campaign_id": pa.array([r[0] for r in crows], pa.string()),
        "channel": pa.array([r[1] for r in crows], pa.string()),
        "day": pa.array([epoch + dt.timedelta(days=r[2]) for r in crows], pa.date32()),
        "sessions": pa.array([r[3] for r in crows], pa.int64()),
        "purchasing_sessions": pa.array([r[4] for r in crows], pa.int64()),
        "revenue_cents": pa.array([r[5] for r in crows], pa.int64()),
        "assisted_sessions": pa.array([r[6] for r in crows], pa.int64()),
    })
    pq.write_table(ut, os.path.join(out, "user_daily", "part-0.parquet"))
    pq.write_table(ct, os.path.join(out, "campaign_daily", "part-0.parquet"))


if __name__ == "__main__":
    main()
