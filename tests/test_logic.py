"""Offline tests. No network needed. Run: python tests/test_logic.py"""
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import fetch_data as f  # noqa: E402

ET = ZoneInfo("America/New_York")
D = dt.date.fromisoformat
FAILS = []


def check(name, got, want):
    ok = got == want
    print(("PASS " if ok else "FAIL ") + f"{name}: got {got!r}" + ("" if ok else f", want {want!r}"))
    if not ok:
        FAILS.append(name)


def plan(event, now_str, label="Bullish"):
    now = dt.datetime.fromisoformat(now_str).replace(tzinfo=ET)
    e, x, _ = f.plan_trade(D(event) if event else None, label, today=now.date(), now=now)
    return e.isoformat(), x.isoformat() if x else None


# trading calendar
check("thanksgiving is closed", f.is_trading_day(D("2026-11-26")), False)
check("good friday 2027 closed", f.is_trading_day(D("2027-03-26")), False)
check("normal wednesday open", f.is_trading_day(D("2026-09-23")), True)

# entry floor
check("floor before 3pm = today", plan(None, "2026-09-23 10:00")[0], "2026-09-23")
check("floor after 3pm = next day", plan(None, "2026-09-23 16:00")[0], "2026-09-24")
check("floor friday evening = monday", plan(None, "2026-09-25 16:00")[0], "2026-09-28")
check("floor saturday = monday", plan(None, "2026-09-26 11:00")[0], "2026-09-28")

# entry/exit around events
check("normal: 2 days before, exit day before", plan("2026-10-07", "2026-09-23 10:00"), ("2026-10-05", "2026-10-06"))
check("skips thanksgiving", plan("2026-11-30", "2026-09-23 10:00"), ("2026-11-25", "2026-11-27"))
check("skips MLK day 2027", plan("2027-01-19", "2026-09-23 10:00"), ("2027-01-14", "2027-01-15"))
check("event tomorrow: enter now, exit today", plan("2026-09-24", "2026-09-23 10:00"), ("2026-09-23", "2026-09-23"))
check("event tomorrow after close: no exit", plan("2026-09-24", "2026-09-23 16:00"), ("2026-09-24", None))
check("past event", plan("2026-09-20", "2026-09-23 10:00"), ("2026-09-23", None))
_, _, note = f.plan_trade(None, "Bearish", today=D("2026-09-23"), now=dt.datetime(2026, 9, 23, 10, tzinfo=ET))
check("bearish note warns about shorting", "custodial" in note, True)

# scoring
check("bullish blend", f.score_stock(1.8, 0.25, 0.8, 0.08, 0.04)[1], "Bullish")
check("bearish blend", f.score_stock(4.0, -0.10, -0.9, -0.12, -0.06)[1], "Bearish")
check("only news, neutral", f.score_stock(None, None, 0.0, None, None), (0.0, "Neutral"))
check("sentiment counts", f.headline_sentiment(["Stock surges after earnings beat", "Analyst downgrade"])[1:], (2, 1))
check("fda tag", f.tag_event(["FDA advisory committee votes"]), "FDA")
check("no tag", f.tag_event(["nothing here"]), None)

# downside + ratings
check("downside picks tighter", f.downside_estimate(100, 90, 0.03), -0.06)
check("downside ignores target above price", f.downside_estimate(100, 110, None), None)
check("finnhub rec mean", f.rec_mean_from_counts({"strongBuy": 2, "buy": 2, "hold": 0, "sell": 0, "strongSell": 0}), 1.5)

# news parsers
y = f.parse_yahoo_news([{"content": {"title": "A", "pubDate": "2026-09-22T10:00:00Z", "provider": {"displayName": "Reuters"},
                                     "canonicalUrl": {"url": "https://x.com"}}},
                        {"title": "B", "link": "https://y.com", "providerPublishTime": 1790000000, "publisher": "AP"}])
check("yahoo news both formats", [n["title"] for n in y], ["A", "B"])
fh = f.parse_finnhub_news([{"headline": "old", "datetime": 1}, {"headline": "new", "datetime": 1790000000, "url": "u", "source": "s"}])
check("finnhub news newest first", fh[0]["title"], "new")

# FDA sponsor regex
m = f.SPONSOR_RE.search("The committee will discuss NDA 123, for drugx tablets, submitted by Acme Therapeutics, Inc., for the treatment of...")
check("sponsor regex", m.group(1) if m else None, "Acme Therapeutics")

# key masking in errors
f.DEBUG["sources"].clear()
f.get_json("http://127.0.0.1:9/x?token=SECRET123", name="masktest", tries=1)
check("key masked in debug", "SECRET123" in json.dumps(f.DEBUG), False)

# mock end-to-end run writes valid files
r = subprocess.run([sys.executable, str(ROOT / "scripts" / "fetch_data.py"), "--mock"], capture_output=True, text=True)
check("mock run exit code", r.returncode, 0)
for name in ("stocks.json", "calendar.json", "debug.json"):
    data = json.loads((ROOT / "site" / "data" / name).read_text())
    check(f"mock {name} valid", bool(data), True)

print(f"\n{len(FAILS)} failed" if FAILS else "\nAll tests passed")
sys.exit(1 if FAILS else 0)
