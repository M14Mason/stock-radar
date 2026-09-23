"""Stock Radar data fetcher.

Runs in GitHub Actions. Writes site/data/{stocks,calendar,debug}.json.

Sources (every one fails soft, the run never crashes on a single source):
  - yfinance (Yahoo, no key): movers, price history, beta, analyst targets, news, earnings
  - Finnhub (free key): big earnings this week, FDA advisory meetings, and a full
    fallback for quotes/news/ratings if Yahoo blocks the GitHub runner
  - FRED (free key): scheduled US economic release dates
  - Last good deploy: if fresh data fails, the previous live data is reused and marked stale

Usage:
  python scripts/fetch_data.py          normal run
  python scripts/fetch_data.py --mock   write fake data (for testing the UI offline)
"""
import json
import os
import re
import sys
import time
import threading
import traceback
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

try:
    import yfinance as yf
except ImportError:
    yf = None
try:
    import holidays
except ImportError:
    holidays = None

# ---------------- config ----------------
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "site" / "data"
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
FRED_KEY = os.getenv("FRED_API_KEY", "").strip()
PAGES_URL = os.getenv("PAGES_URL", "").rstrip("/")
ET = ZoneInfo("America/New_York")
ET_NOW = dt.datetime.now(ET)
TODAY = ET_NOW.date()

MAX_STOCKS = 30
MIN_PRICE = 5.0             # skip penny stocks
MIN_AVG_VOLUME = 1_000_000  # skip illiquid stocks
ENTRY_LEAD_DAYS = 2         # enter N trading days before the catalyst
EVENT_WINDOW_DAYS = 45      # only treat events this close as "upcoming"
WORKERS = 4
MIN_OK_STOCKS = 5

FOMC_DECISIONS = ["2026-10-28", "2026-12-09"]  # verified on federalreserve.gov 2026-09-23
FRED_RELEASES = {10: ("CPI", "high"), 50: ("Jobs Report", "high"), 53: ("GDP", "high"),
                 54: ("PCE / Personal Income", "high"), 46: ("PPI", "medium"),
                 9: ("Retail Sales", "medium"), 180: ("Jobless Claims", "low")}

BULL_WORDS = {"beat", "beats", "surge", "surges", "soar", "soars", "upgrade", "upgraded", "upgrades",
              "raises", "raised", "record", "approval", "approved", "approves", "wins", "jump", "jumps",
              "rally", "rallies", "strong", "outperform", "breakthrough", "positive", "gains", "tops"}
BEAR_WORDS = {"miss", "misses", "plunge", "plunges", "downgrade", "downgraded", "downgrades", "cuts",
              "lawsuit", "probe", "recall", "rejects", "rejected", "falls", "drop", "drops", "warning",
              "weak", "halt", "halted", "delay", "delayed", "investigation", "crl", "slump", "tumbles",
              "layoffs", "fraud", "sinks", "underperform", "subpoena", "bankruptcy"}
EVENT_TAGS = [
    ("FDA", r"\b(fda|pdufa|advisory committee|adcom|phase (?:1|2|3|i|ii|iii)|clinical trial|nda|bla)\b"),
    ("Earnings", r"\b(earnings|eps|quarterly results|guidance)\b"),
    ("M&A", r"\b(acquire|acquires|acquisition|merger|buyout|takeover)\b"),
    ("Analyst", r"\b(upgrade|downgrade|price target|initiates)\b"),
]

# ---------------- debug log ----------------
DEBUG = {
    "started": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    "market_date": TODAY.isoformat(),
    "keys_present": {"finnhub": bool(FINNHUB_KEY), "fred": bool(FRED_KEY)},  # never the values
    "versions": {"python": sys.version.split()[0],
                 "yfinance": getattr(yf, "__version__", None),
                 "holidays": getattr(holidays, "__version__", None)},
    "sources": {}, "warnings": [], "symbols_failed": {}, "symbols_skipped": {},
}
_dbg_lock = threading.Lock()


def warn(msg):
    with _dbg_lock:
        DEBUG["warnings"].append(msg)
    print(f"::warning::{msg}")


def source(name, ok, count=0, ms=0, error=None, note=None):
    with _dbg_lock:
        s = DEBUG["sources"].setdefault(name, {"ok": 0, "fail": 0, "items": 0, "ms": 0, "last_error": None})
        s["ok" if ok else "fail"] += 1
        s["items"] += count
        s["ms"] += int(ms)
        if error:
            s["last_error"] = str(error)[:300]
        if note:
            s["note"] = note


class Timer:
    def __enter__(self):
        self.t = time.perf_counter()
        return self

    def __exit__(self, *a):
        self.ms = (time.perf_counter() - self.t) * 1000


# ---------------- http ----------------
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "stock-radar/2.0 (+github actions)"
_fh_lock = threading.Lock()
_fh_last = [0.0]


def get_json(url, params=None, name="http", tries=3):
    """GET with retry on network errors, 429 and 5xx. Returns None on failure."""
    err = None
    start = time.perf_counter()
    for i in range(tries):
        try:
            r = SESSION.get(url, params=params, timeout=20)
            if r.status_code == 429 or r.status_code >= 500:
                err = f"HTTP {r.status_code}"
                time.sleep(2 ** i)
                continue
            if r.status_code in (401, 403):
                err = f"HTTP {r.status_code} (bad key or plan does not include this endpoint)"
                break
            r.raise_for_status()
            data = r.json()
            source(name, True, ms=(time.perf_counter() - start) * 1000)
            return data
        except Exception as e:  # noqa: BLE001
            err = re.sub(r"(token|api_key)=[^&\s]+", r"\1=***", str(e))  # never log keys
            time.sleep(2 ** i)
    source(name, False, ms=(time.perf_counter() - start) * 1000, error=err)
    return None


def finnhub(path, **params):
    """Finnhub free tier = 60 calls/min. Space calls >= 1.05s apart across threads."""
    if not FINNHUB_KEY:
        return None
    with _fh_lock:
        wait = 1.05 - (time.time() - _fh_last[0])
        if wait > 0:
            time.sleep(wait)
        _fh_last[0] = time.time()
    params["token"] = FINNHUB_KEY
    return get_json(f"https://finnhub.io/api/v1/{path}", params, name=f"finnhub:{path.split('/')[-1]}")


# ---------------- trading calendar ----------------
if holidays:
    NYSE_HOLIDAYS = holidays.NYSE(years=range(TODAY.year - 1, TODAY.year + 3))
else:
    NYSE_HOLIDAYS = {}
    warn("holidays package missing: market holidays not skipped in entry dates")


def is_trading_day(d):
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def add_trading_days(d, n):
    step = 1 if n >= 0 else -1
    left = abs(n)
    while left:
        d += dt.timedelta(days=step)
        if is_trading_day(d):
            left -= 1
    return d


def next_trading_day(d):
    while not is_trading_day(d):
        d += dt.timedelta(days=1)
    return d


def entry_floor(now=None):
    """Earliest realistic entry: today if before 3pm ET on a trading day, else next trading day."""
    now = now or ET_NOW
    d = now.date()
    if now.hour >= 15 or not is_trading_day(d):
        d += dt.timedelta(days=1)
    return next_trading_day(d)


def plan_trade(event_date, expectation, today=None, now=None):
    """Returns (entry_date, exit_date, note). Run-up strategy: in N days before, out the day before."""
    today = today or TODAY
    floor = entry_floor(now)
    bear = expectation == "Bearish"
    bear_note = " Bearish setup: a long entry fights the signal. Shorting needs margin, which custodial accounts do not allow."
    if event_date is None:
        return floor, None, "No dated catalyst. Earliest entry shown; news momentum only." + (bear_note if bear else "")
    if event_date <= today:
        return floor, None, "Event already passed. Earliest entry shown." + (bear_note if bear else "")
    exit_d = add_trading_days(event_date, -1)
    target = add_trading_days(event_date, -ENTRY_LEAD_DAYS)
    if target < floor:
        if exit_d < floor:
            return floor, None, "Event is next trading day. Too late for a run-up entry; holding means gap risk." + (bear_note if bear else "")
        return floor, exit_d, "Event is close. Earliest entry shown." + (bear_note if bear else "")
    return target, exit_d, (f"{ENTRY_LEAD_DAYS} trading days before the event to catch the run-up; "
                            "exit the day before to avoid the gap.") + (bear_note if bear else "")


# ---------------- scoring ----------------
def clip(x, lo=-1.0, hi=1.0):
    return max(lo, min(hi, x))


def headline_sentiment(headlines):
    bull = bear = 0
    for h in headlines:
        words = set(re.findall(r"[a-z]+", h.lower()))
        bull += len(words & BULL_WORDS)
        bear += len(words & BEAR_WORDS)
    total = bull + bear
    return ((bull - bear) / total if total else 0.0), bull, bear


def tag_event(headlines):
    text = " ".join(headlines).lower()
    for tag, pat in EVENT_TAGS:
        if re.search(pat, text):
            return tag
    return None


def score_stock(rec_mean, upside, news_score, trend, mom5):
    """40% analyst, 30% news, 30% technicals. Missing parts dropped, weights renormalized."""
    parts = []
    a = []
    if rec_mean is not None:
        a.append(clip((3 - rec_mean) / 2))
    if upside is not None:
        a.append(clip(upside / 0.30))
    if a:
        parts.append((0.40, sum(a) / len(a)))
    parts.append((0.30, news_score))
    if trend is not None:
        parts.append((0.30, 0.6 * clip(trend / 0.10) + 0.4 * clip((mom5 or 0) / 0.05)))
    score = sum(w * v for w, v in parts) / sum(w for w, _ in parts)
    label = "Bullish" if score > 0.2 else "Bearish" if score < -0.2 else "Neutral"
    return round(score, 2), label


def downside_estimate(price, tgt_low, atr_pct):
    """Tighter (closer to zero) of: lowest analyst target, 2x ATR below price. Always negative."""
    downs = []
    if tgt_low and tgt_low < price:
        downs.append(tgt_low / price - 1)
    if atr_pct:
        downs.append(-2 * atr_pct)
    return max(downs) if downs else None


def rec_mean_from_counts(r):
    """Finnhub counts -> Yahoo-style 1 (strong buy) .. 5 (strong sell)."""
    w = {"strongBuy": 1, "buy": 2, "hold": 3, "sell": 4, "strongSell": 5}
    n = sum(r.get(k, 0) for k in w)
    return round(sum(r.get(k, 0) * v for k, v in w.items()) / n, 2) if n else None


# ---------------- news ----------------
def parse_yahoo_news(raw):
    items = []
    for n in raw or []:
        c = n.get("content") if isinstance(n, dict) and "content" in n else n
        if not isinstance(c, dict) or not c.get("title"):
            continue
        link = ((c.get("canonicalUrl") or {}).get("url")) or ((c.get("clickThroughUrl") or {}).get("url")) or c.get("link") or ""
        pub = c.get("pubDate") or c.get("providerPublishTime") or ""
        if isinstance(pub, (int, float)):
            pub = dt.datetime.fromtimestamp(pub, dt.timezone.utc).isoformat()
        prov = (c.get("provider") or {}).get("displayName") or c.get("publisher") or ""
        items.append({"title": c["title"], "url": link, "published": str(pub)[:10], "source": prov})
    return items[:4]


def parse_finnhub_news(raw):
    items = []
    for n in sorted(raw or [], key=lambda x: x.get("datetime", 0), reverse=True):
        if not n.get("headline"):
            continue
        pub = dt.datetime.fromtimestamp(n.get("datetime", 0), dt.timezone.utc).date().isoformat()
        items.append({"title": n["headline"], "url": n.get("url", ""), "published": pub, "source": n.get("source", "")})
    return items[:4]


# ---------------- per-stock builders ----------------
def pick_event(earn, fda, heads):
    """Soonest dated catalyst within the window; else an undated news tag."""
    cands = []
    if earn and 0 <= (earn - TODAY).days <= EVENT_WINDOW_DAYS:
        cands.append((earn, "Earnings", None))
    if fda and 0 <= (fda["date"] - TODAY).days <= EVENT_WINDOW_DAYS:
        cands.append((fda["date"], "FDA meeting", fda["title"]))
    if cands:
        return min(cands, key=lambda c: c[0])
    tag = tag_event(heads)
    return (None, f"{tag} news", None) if tag else (None, None, None)


def finish(sym, why, name, price, chg, beta, rec_mean, rec_key, upside, downside, targets,
           news, trend, mom5, earn, fda, data_source):
    heads = [n["title"] for n in news]
    news_score, nb, nr = headline_sentiment(heads)
    score, label = score_stock(rec_mean, upside, news_score, trend, mom5)
    ev_date, ev_type, ev_detail = pick_event(earn, fda, heads)
    entry, exit_d, note = plan_trade(ev_date, label)
    rr = round(upside / abs(downside), 2) if upside is not None and downside else None
    return {
        "symbol": sym, "name": name or sym, "why": why, "data_source": data_source,
        "price": round(float(price), 2), "change_pct": round(float(chg or 0), 2),
        "beta": round(float(beta), 2) if beta is not None else None,
        "event_type": ev_type, "event_date": ev_date.isoformat() if ev_date else None, "event_detail": ev_detail,
        "recent_news": news,
        "expectation": label, "score": score,
        "score_parts": {"analyst_rating": rec_mean, "analyst_recommendation": rec_key,
                        "news_bull": nb, "news_bear": nr,
                        "trend_vs_50d": round(trend, 3) if trend is not None else None,
                        "momentum_5d": round(mom5, 3) if mom5 is not None else None},
        "upside_pct": round(upside * 100, 1) if upside is not None else None,
        "downside_pct": round(downside * 100, 1) if downside is not None else None,
        "reward_risk": rr, "targets": targets,
        "entry_date": entry.isoformat(), "exit_date": exit_d.isoformat() if exit_d else None, "entry_note": note,
        "next_earnings": earn.isoformat() if earn else None,
    }


def yahoo_earnings(tk):
    try:
        for d in (tk.calendar or {}).get("Earnings Date") or []:
            d = d if isinstance(d, dt.date) else dt.date.fromisoformat(str(d)[:10])
            if d >= TODAY:
                return d
    except Exception as e:  # noqa: BLE001
        source("yahoo:calendar", False, error=e)
    return None


def build_yahoo(sym, why, fda):
    with Timer() as t:
        tk = yf.Ticker(sym)
        info = tk.info or {}
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        if not price:
            raise ValueError("Yahoo returned no price")
        if price < MIN_PRICE:
            return "skip", f"price ${price} < ${MIN_PRICE}"
        if (info.get("averageVolume") or 0) < MIN_AVG_VOLUME:
            return "skip", f"avg volume {info.get('averageVolume')} < {MIN_AVG_VOLUME:,}"
        hist = tk.history(period="6mo", auto_adjust=True)
        trend = mom5 = atr_pct = None
        if len(hist) >= 55:
            c, h, l = hist["Close"], hist["High"], hist["Low"]
            trend = float(c.iloc[-1] / c.tail(50).mean() - 1)
            mom5 = float(c.iloc[-1] / c.iloc[-6] - 1)
            prev = c.shift(1)
            tr = pd.concat([h - l, (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)  # true range
            atr_pct = float(tr.tail(14).mean() / c.iloc[-1])
        lo, mean, hi = info.get("targetLowPrice"), info.get("targetMeanPrice"), info.get("targetHighPrice")
        upside = (mean / price - 1) if mean else None
        downside = downside_estimate(price, lo, atr_pct)
        news = parse_yahoo_news(tk.get_news(count=8))
        earn = yahoo_earnings(tk)
    source("yahoo:ticker", True, 1, t.ms)
    return "ok", finish(sym, why, info.get("shortName"), price, info.get("regularMarketChangePercent"),
                        info.get("beta"), info.get("recommendationMean"), info.get("recommendationKey"),
                        upside, downside, {"low": lo, "mean": mean, "high": hi}, news, trend, mom5,
                        earn, fda, "yahoo")


def build_finnhub(sym, why, fda):
    """Fallback when Yahoo fails. No price targets or history on the free tier."""
    q = finnhub("quote", symbol=sym)
    if not q or not q.get("c"):
        raise ValueError("Finnhub returned no quote")
    price = q["c"]
    if price < MIN_PRICE:
        return "skip", f"price ${price} < ${MIN_PRICE}"
    metric = (finnhub("stock/metric", symbol=sym, metric="all") or {}).get("metric", {})
    vol_m = metric.get("10DayAverageTradingVolume")
    if vol_m is not None and vol_m * 1e6 < MIN_AVG_VOLUME:
        return "skip", f"avg volume {vol_m}M < {MIN_AVG_VOLUME:,}"
    prof = finnhub("stock/profile2", symbol=sym) or {}
    recs = finnhub("stock/recommendation", symbol=sym) or []
    rec_mean = rec_mean_from_counts(recs[0]) if recs else None
    news = parse_finnhub_news(finnhub("company-news", symbol=sym,
                                      **{"from": (TODAY - dt.timedelta(days=7)).isoformat(), "to": TODAY.isoformat()}))
    ec = (finnhub("calendar/earnings", symbol=sym, **{"from": TODAY.isoformat(),
                                                     "to": (TODAY + dt.timedelta(days=100)).isoformat()}) or {})
    dates = sorted(r["date"] for r in ec.get("earningsCalendar", []) if r.get("date"))
    earn = dt.date.fromisoformat(dates[0]) if dates else None
    wk = metric.get("52WeekLow")
    downside = (wk / price - 1) if wk and wk < price else None  # crude: distance to 52-week low
    return "ok", finish(sym, why, prof.get("name"), price, q.get("dp"), metric.get("beta"), rec_mean, None,
                        None, downside, {}, news, None, None, earn, fda, "finnhub")


def build_stock(sym, why, fda):
    errors = []
    if yf is not None:
        try:
            return build_yahoo(sym, why, fda)
        except Exception as e:  # noqa: BLE001
            source("yahoo:ticker", False, error=f"{sym}: {e}")
            errors.append(f"yahoo: {e}")
    if FINNHUB_KEY:
        try:
            return build_finnhub(sym, why, fda)
        except Exception as e:  # noqa: BLE001
            errors.append(f"finnhub: {e}")
    return "fail", " | ".join(errors) or "no data source available"


# ---------------- universe ----------------
def watchlist_file():
    p = ROOT / "watchlist.txt"
    if not p.exists():
        return {}
    text = re.sub(r"#.*", "", p.read_text())
    return {s.upper(): "My watchlist" for s in re.findall(r"\b[A-Za-z]{1,5}\b", text)}


def yahoo_movers():
    picks = {}
    if yf is None:
        return picks
    for key, why in (("day_gainers", "Top gainer"), ("day_losers", "Top loser"), ("most_actives", "Most active")):
        with Timer() as t:
            try:
                quotes = yf.screen(key, count=15).get("quotes", [])
                n = 0
                for q in quotes:
                    sym = q.get("symbol", "")
                    if re.fullmatch(r"[A-Z]{1,5}", sym) and sym not in picks:
                        picks[sym] = why
                        n += 1
                ok, err = True, None
            except Exception as e:  # noqa: BLE001
                ok, err, n = False, e, 0
        source(f"yahoo:screen:{key}", ok, n, t.ms, err)
    return picks


def finnhub_big_earnings():
    data = finnhub("calendar/earnings", **{"from": TODAY.isoformat(), "to": (TODAY + dt.timedelta(days=7)).isoformat()})
    rows = [r for r in (data or {}).get("earningsCalendar", [])
            if re.fullmatch(r"[A-Z]{1,5}", r.get("symbol") or "") and r.get("revenueEstimate")]
    rows.sort(key=lambda r: r["revenueEstimate"], reverse=True)
    return {r["symbol"]: "Big earnings this week" for r in rows[:8]}


SPONSOR_RE = re.compile(r"(?:submitted|sponsored|held) by ([A-Z][A-Za-z0-9&.,'\- ]{2,60}?)(?:,| for | to | with |\.|;|\))")


def fda_meetings():
    """Returns (calendar_events, {symbol: {date, title}}). Maps sponsors to tickers via Finnhub search."""
    events, by_sym = [], {}
    raw = finnhub("fda-advisory-committee-calendar")
    if raw is None:
        return events, by_sym
    end = TODAY + dt.timedelta(days=90)
    lookups = 0
    for r in raw:
        d = (r.get("fromDate") or "")[:10]
        if not d or not (TODAY.isoformat() <= d <= end.isoformat()):
            continue
        desc = r.get("eventDescription") or "FDA advisory committee"
        ev = {"date": d, "kind": "FDA", "title": desc[:160], "impact": "medium", "source": "Finnhub", "url": r.get("url")}
        m = SPONSOR_RE.search(desc)
        if m and lookups < 10:
            sponsor = re.sub(r"\b(Inc|Corp|Corporation|Ltd|LLC|plc|Co|Company|Pharmaceuticals?|Therapeutics|AG|SA|NV)\b\.?", "",
                             m.group(1)).strip(" ,.")
            lookups += 1
            res = (finnhub("search", q=sponsor) or {}).get("result", [])
            hit = next((x for x in res if x.get("type") == "Common Stock" and re.fullmatch(r"[A-Z]{1,5}", x.get("symbol", ""))), None)
            if hit:
                ev["symbol"] = hit["symbol"]
                ev["title"] = f"{hit['symbol']}: {ev['title']}"
                cur = by_sym.get(hit["symbol"])
                if not cur or d < cur["date"].isoformat():
                    by_sym[hit["symbol"]] = {"date": dt.date.fromisoformat(d), "title": desc[:160]}
        events.append(ev)
    source("fda:mapped", True, len(by_sym), note=f"{len(by_sym)} of {len(events)} meetings matched to a ticker")
    return events, by_sym


def macro_events():
    events = []
    for d in FOMC_DECISIONS:
        if dt.date.fromisoformat(d) >= TODAY:
            events.append({"date": d, "kind": "Fed", "title": "FOMC rate decision + press conference",
                           "impact": "high", "source": "federalreserve.gov"})
    if not FRED_KEY:
        warn("No FRED_API_KEY secret: CPI/jobs/GDP dates skipped")
        return events
    end = TODAY + dt.timedelta(days=60)
    for rid, (label, impact) in FRED_RELEASES.items():
        data = get_json("https://api.stlouisfed.org/fred/release/dates", {
            "release_id": rid, "api_key": FRED_KEY, "file_type": "json",
            "realtime_start": TODAY.isoformat(), "realtime_end": "9999-12-31",
            "include_release_dates_with_no_data": "true", "sort_order": "asc", "limit": 30}, name="fred")
        for r in (data or {}).get("release_dates", []):
            d = r.get("date")
            if d and TODAY.isoformat() <= d <= end.isoformat():
                events.append({"date": d, "kind": "Macro", "title": label, "impact": impact, "source": "FRED"})
    return events


# ---------------- previous deploy fallback ----------------
def previous_deploy(name):
    if not PAGES_URL:
        return None
    return get_json(f"{PAGES_URL}/data/{name}", {"t": int(time.time())}, name="previous-deploy", tries=2)


# ---------------- mock ----------------
def mock():
    news = [{"title": "Company beats estimates, raises guidance", "url": "https://example.com", "published": TODAY.isoformat(), "source": "Mock"}]
    e1 = add_trading_days(TODAY, 8)
    stocks = [
        finish("MOCKA", "My watchlist", "Mock Alpha Inc", 120.5, 2.1, 1.3, 1.8, "buy", 0.22, -0.07, {"low": 100, "mean": 147, "high": 180}, news, 0.06, 0.03, e1, None, "mock"),
        finish("MOCKB", "Top loser", "Mock Bio Corp", 14.2, -8.5, None, None, None, None, None, {}, [{"title": "Shares plunge after FDA delay", "url": "", "published": TODAY.isoformat(), "source": "Mock"}], -0.12, -0.08, None,
               {"date": add_trading_days(TODAY, 1), "title": "Advisory committee on mock drug"}, "mock"),
        finish("MOCKC", "Most active", "Mock Chips", 55.0, 0.2, 2.1, 2.9, "hold", 0.03, -0.09, {}, [], 0.0, 0.0, None, None, "mock"),
    ]
    cal = [{"date": FOMC_DECISIONS[0], "kind": "Fed", "title": "FOMC rate decision", "impact": "high", "source": "mock"},
           {"date": add_trading_days(TODAY, 3).isoformat(), "kind": "Macro", "title": "CPI", "impact": "high", "source": "mock"},
           {"date": add_trading_days(TODAY, 1).isoformat(), "kind": "FDA", "title": "MOCKB: Advisory committee", "impact": "medium", "source": "mock", "symbol": "MOCKB", "url": "javascript:alert(1)"}]
    return stocks, cal


# ---------------- main ----------------
def write(name, obj):
    (OUT / name).write_text(json.dumps(obj, indent=1, default=str))


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    status = "ok"
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="minutes")

    if "--mock" in sys.argv:
        stocks, cal = mock()
        DEBUG["mode"] = "mock"
    else:
        if not FINNHUB_KEY:
            warn("No FINNHUB_API_KEY secret: FDA calendar, big-earnings scan and Yahoo fallback are off")
        fda_cal, fda_map = fda_meetings()
        universe = {}
        for src in (watchlist_file(), {s: "FDA meeting soon" for s in fda_map}, finnhub_big_earnings(), yahoo_movers()):
            for sym, why in src.items():
                universe.setdefault(sym, why)
        DEBUG["universe"] = universe
        print(f"Universe: {len(universe)} symbols")

        stocks = []
        with ThreadPoolExecutor(WORKERS) as ex:
            futs = {ex.submit(build_stock, s, w, fda_map.get(s)): s for s, w in universe.items()}
            for f in as_completed(futs):
                sym = futs[f]
                try:
                    kind, val = f.result()
                except Exception:  # noqa: BLE001
                    kind, val = "fail", traceback.format_exc(limit=2)
                if kind == "ok":
                    stocks.append(val)
                elif kind == "skip":
                    DEBUG["symbols_skipped"][sym] = val
                else:
                    DEBUG["symbols_failed"][sym] = val[:300]
        order = list(universe)
        stocks.sort(key=lambda s: order.index(s["symbol"]))
        stocks = stocks[:MAX_STOCKS]

        cal = macro_events() + fda_cal
        if len(stocks) < MIN_OK_STOCKS:
            status = "error"
            warn(f"Only {len(stocks)} stocks fetched (need {MIN_OK_STOCKS}). Yahoo may be blocking GitHub; add a Finnhub key for the fallback.")
            prev = previous_deploy("stocks.json")
            if prev and prev.get("stocks"):
                have = {s["symbol"] for s in stocks}
                stocks += [dict(s, stale=True) for s in prev["stocks"] if s["symbol"] not in have]
                now = prev.get("updated", now)
                warn(f"Reused {len(prev['stocks'])} stocks from the last good deploy ({now}). Marked stale.")
        elif DEBUG["symbols_failed"] or DEBUG["warnings"]:
            status = "warn"

    cal.sort(key=lambda e: (e["date"], e["kind"]))
    DEBUG.update({"status": status, "finished": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                  "duration_s": round(time.perf_counter() - t0, 1),
                  "counts": {"stocks": len(stocks), "calendar": len(cal),
                             "failed": len(DEBUG["symbols_failed"]), "skipped": len(DEBUG["symbols_skipped"])}})
    write("stocks.json", {"updated": now, "stocks": stocks})
    write("calendar.json", {"updated": now, "events": cal})
    write("debug.json", DEBUG)
    print(f"Status {status}: {len(stocks)} stocks, {len(cal)} events, {DEBUG['duration_s']}s")

    gh_out = os.getenv("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write(f"status={status}\n")
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(f"## Stock Radar: {status}\n\n| Stocks | Events | Failed | Skipped | Time |\n|---|---|---|---|---|\n"
                    f"| {len(stocks)} | {len(cal)} | {len(DEBUG['symbols_failed'])} | {len(DEBUG['symbols_skipped'])} | {DEBUG['duration_s']}s |\n\n")
            f.write("| Source | OK | Fail | Items | Last error |\n|---|---|---|---|---|\n")
            for k, v in sorted(DEBUG["sources"].items()):
                f.write(f"| {k} | {v['ok']} | {v['fail']} | {v['items']} | {(v['last_error'] or '').replace('|', '/')} |\n")
            for w in DEBUG["warnings"]:
                f.write(f"\n- {w}")


if __name__ == "__main__":
    main()
