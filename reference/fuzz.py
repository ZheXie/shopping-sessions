#!/usr/bin/env python3
"""
Differential fuzzer: writes small, very dirty random datasets and checks that
the fast pipeline and reference.py produce identical tables.

Usage: fuzz.py FAST_BINARY [ITERATIONS] [SEED]
Needs polars (for scripts/compare.py-style loading) via pyarrow/polars.
"""
import os
import random
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REF = os.path.join(HERE, "reference.py")

WS = "2026-10-01T00:00:00Z"
WE = "2026-10-15T00:00:00Z"
WS_MS = 1790812800000  # 2026-10-01T00:00:00Z
DAY = 86_400_000


def iso(ms, rnd):
    import datetime as dt
    form = rnd.random()
    if form < 0.4 and ms % 1000 == 0:
        t = dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc)
        return t.strftime("%Y-%m-%dT%H:%M:%SZ")
    if form < 0.55 and ms % 1000 == 0:
        off = rnd.choice([-600, -300, -1, 0, 1, 330, 720, 23 * 60 + 59])
        t = dt.datetime.fromtimestamp(ms / 1000 + off * 60, dt.timezone.utc)
        sign = "-" if off < 0 else "+"
        a = abs(off)
        return t.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{a // 60:02d}:{a % 60:02d}"
    if form < 0.6:
        return rnd.choice(["", "2026-10-03 10:00:00Z", "2026-10-03T10:00:00z", "2026-02-30T10:00:00Z",
                           "2026-10-03T24:00:00Z", "2026-10-03T10:00:60Z", "2026-13-03T10:00:00Z",
                           "2026-10-03T10:00:00+0200", "2026-10-03T10:00:00+24:00", "1.79e12", "-5",
                           "2026-10-03T10:00:00.000Z", "2028-02-29T10:00:00Z", "2027-02-29T10:00:00Z",
                           "0" * 25 + "1", "9" * 30, "86400000", "2026-10-03T10:00:00-00:00", "abc",
                           "1791071890000.0", "٢٥٠", "２０２６-10-03T10:00:00Z"])
    return ("0" * rnd.randint(0, 2)) + str(ms)


def field(v, rnd):
    r = rnd.random()
    if r < 0.2:
        return f'"{v}"'
    if r < 0.21:
        return f'"{v}'  # unbalanced -> malformed line
    if r < 0.215:
        return f'{v}"x'
    return v


def line(fields, rnd, allow_cr=True):
    s = ",".join(field(f, rnd) for f in fields)
    r = rnd.random()
    if r < 0.01:
        s += ",extra"
    elif r < 0.02:
        s = s.rsplit(",", 1)[0]
    elif r < 0.03 and allow_cr:
        s += "\r"
    elif r < 0.035:
        s = ""
    return s


def gen(d, rnd):
    os.makedirs(os.path.join(d, "events"))
    os.makedirs(os.path.join(d, "clicks"))
    os.makedirs(os.path.join(d, "links"))
    with open(os.path.join(d, "manifest.json"), "w") as f:
        f.write(f'{{"window_start": "{WS}", "window_end": "{WE}"}}\n')
    cats = ["shoes", "books", "Books", "home", "unknownish", "a b"]
    nprod = rnd.randint(1, 12)
    with open(os.path.join(d, "products.csv"), "w", newline="") as f:
        f.write("product_id,category\n")
        for i in range(1, nprod + 1):
            f.write(f"p{i},{rnd.choice(cats)}\n")
        if rnd.random() < 0.5:
            f.write("p_x,shoes\n")
    ncamp = rnd.randint(1, 6)
    with open(os.path.join(d, "campaigns.csv"), "w", newline="") as f:
        f.write("campaign_id,channel\n")
        for i in range(1, ncamp + 1):
            f.write(f"c_{i},{rnd.choice(['search', 'social', 'email'])}\n")
    users = [f"u{i}" for i in range(rnd.randint(1, 8))] + ["U1", " u1", "u1 ", "", "user_id_a",
                                                          "a_very_long_user_identifier_over_16"]
    prods = [f"p{i}" for i in range(0, nprod + 3)] + ["P1", " p1", "", "p01", "p_x"]
    camps = [f"c_{i}" for i in range(0, ncamp + 2)] + ["C_1", " c_1", "c_01"]
    types = ["view"] * 6 + ["cart"] * 3 + ["purchase"] * 4 + ["Purchase", "cart ", "", "buy"]
    eids = [f"e{i}" for i in range(rnd.randint(5, 60))] + ["e1 ", "E1", "a_very_long_event_identifier_xx"]
    base = WS_MS + rnd.randint(0, 3) * DAY + rnd.randint(0, 20) * 3_600_000

    def rts():
        r = rnd.random()
        if r < 0.05:
            return base - rnd.randint(0, 3) * DAY - rnd.choice([0, 1, 1000])
        if r < 0.1:
            return WS_MS + 14 * DAY - rnd.choice([0, 1, 1000])
        return base + rnd.choice([0, 1, 1000, 60_000]) * rnd.randint(0, 200) + rnd.choice([0, 1_800_000, 1_800_001, 0])

    # data contract: copies of an event with equal ingest_ts are identical
    seen_ing = {}
    for fi in range(rnd.randint(1, 3)):
        with open(os.path.join(d, "events", f"part-{fi}.csv"), "w", newline="") as f:
            f.write("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")
            for _ in range(rnd.randint(0, 80)):
                ts = rts()
                ing = ts + rnd.choice([0, 1000, -1000, DAY, DAY + 1000, 2 * DAY, rnd.randint(0, DAY)])
                ing = (ing // 1000) * 1000
                import datetime as dt
                ingt = dt.datetime.fromtimestamp(ing / 1000, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                price = rnd.choice(["", "0", "1", "250", "0250", "10000000", "10000001", "19.99", "-5", "1000\r",
                                    str(rnd.randint(1, 50000))])
                eid = rnd.choice(eids)
                vals = [eid, rnd.choice(users), iso(ts, rnd), ingt, rnd.choice(types), rnd.choice(prods), price]
                key = (eid, ingt)
                if key in seen_ing:
                    vals = list(seen_ing[key])
                else:
                    seen_ing[key] = tuple(vals)
                f.write(line(vals, rnd) + "\n")
            if rnd.random() < 0.3:
                f.write("e_last,u1,2026-10-03T10:00:00Z,2026-10-03T10:00:00Z,view,p1,")  # no final newline
    # data contract: copies of a click have identical values
    clicks = {}
    for fi in range(rnd.randint(1, 2)):
        with open(os.path.join(d, "clicks", f"part-{fi}.csv"), "w", newline="") as f:
            f.write("click_id,user_id,campaign_id,ts\n")
            for i in range(rnd.randint(0, 40)):
                ts = rts() - rnd.choice([0, 1, 3_600_000, 7 * DAY, 7 * DAY + 1, 2 * DAY])
                cid = rnd.choice([f"k{fi}_{i}", f"k{rnd.randint(0, 5)}", "K1", "k_long_click_identifier_000"])
                if cid not in clicks:  # a trailing \r is part of the value, so fixed per click
                    t = iso(ts, rnd) + ("\r" if rnd.random() < 0.02 else "")
                    clicks[cid] = [cid, rnd.choice(users), rnd.choice(camps), t]
                f.write(line(clicks[cid], rnd, allow_cr=False) + "\n")
    for fi in range(rnd.randint(1, 2)):
        with open(os.path.join(d, "links", f"part-{fi}.csv"), "w", newline="") as f:
            f.write("user_id_a,user_id_b\n")
            for _ in range(rnd.randint(0, 10)):
                f.write(line([rnd.choice(users), rnd.choice(users)], rnd) + "\n")
    if rnd.random() < 0.2:
        open(os.path.join(d, "events", "part-empty.csv"), "w").write("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")


def main():
    fast = sys.argv[1]
    iters = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    seed = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    compare = os.environ.get("COMPARE")  # path to scripts/compare.py
    sys.path.insert(0, os.path.dirname(compare))
    import compare as cmp  # noqa
    from run_fixtures import SCHEMAS, load_actual  # noqa
    fails = 0
    for it in range(iters):
        rnd = random.Random(seed * 100000 + it)
        tmp = tempfile.mkdtemp()
        try:
            d = os.path.join(tmp, "data")
            gen(d, rnd)
            outs = []
            for name, cmd in (("ref", [sys.executable, REF]), ("fast", [fast])):
                o = os.path.join(tmp, name)
                env = dict(os.environ, SS_CHUNK=str(rnd.choice([1, 7, 64, 1 << 23])), SS_RBUF=str(rnd.choice([64, 300, 1 << 20])))
                p = subprocess.run(cmd + [d, o], capture_output=True, text=True, env=env)
                if p.returncode != 0:
                    raise RuntimeError(f"{name} exit {p.returncode}: {p.stderr[-500:]}")
                outs.append(o)
            for table, schema in SCHEMAS.items():
                a = sorted(load_actual(os.path.join(outs[0], table), schema).rows(), key=str)
                b = sorted(load_actual(os.path.join(outs[1], table), schema).rows(), key=str)
                if a != b:
                    raise RuntimeError(f"{table} differs\n ref  {a[:6]}\n fast {b[:6]}")
        except Exception as e:  # noqa
            fails += 1
            keep = f"/tmp/fuzzfail_{seed}_{it}"
            shutil.rmtree(keep, ignore_errors=True)
            shutil.copytree(os.path.join(tmp, "data"), keep)
            print(f"iteration {it}: FAIL ({e}); data kept in {keep}")
            if fails >= 3:
                break
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    print(f"{iters} iterations, {fails} failures")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
