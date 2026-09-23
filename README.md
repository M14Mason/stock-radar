# Stock Radar

Phone-friendly stock catalyst tracker and economic calendar. GitHub Actions refreshes the data every 30 min on weekdays and GitHub Pages hosts it.

## What each stock card shows
- **Name / Beta**, price, today's move
- **Upcoming event** (earnings or FDA meeting, soonest within 45 days) and **recent news**
- **Bullish / Bearish**: 40% analyst rating + target upside, 30% headline keywords, 30% trend (vs 50-day average, 5-day move)
- **Upside** = average analyst target. **Downside** = tighter of lowest analyst target or 2x ATR (average true range). **Reward:risk** = upside / downside
- **Date of entry** = 2 trading days before the event, **Exit by** = the trading day before it (NYSE holidays skipped). Tap the date to change it; your change is saved on your phone only
- **Next earnings**

Tabs: **Stocks** (search, sort, filters), **Calendar** (Fed, CPI, jobs, GDP, PCE, PPI, retail sales, jobless claims, FDA advisory meetings, earnings), **Debug** (health of the last data run and of your device, plus a "Copy debug report" button).

## API keys (free, optional but recommended)
- `FINNHUB_API_KEY` from finnhub.io/register: FDA calendar, big-earnings scan, backup data if Yahoo blocks GitHub
- `FRED_API_KEY` from fred.stlouisfed.org/docs/api/api_key.html: economic release dates

Add them in the repo: Settings > Secrets and variables > Actions > New repository secret. Never put keys in code.

## Change tickers
Edit `watchlist.txt`. Those tickers are always included, on top of auto-scanned movers, big earnings and FDA-meeting companies.

## Debugging
1. Open the **Debug** tab in the app. Red rows show which source failed and why.
2. On GitHub: Actions tab > latest run > Summary shows a table of every source.
3. Run the offline tests: `python tests/test_logic.py`
4. Test the UI without internet: `python scripts/fetch_data.py --mock`, then `cd site && python -m http.server` and open localhost:8000.

If a data run fails, the site keeps the last good data (marked stale), and the run turns red so GitHub emails you.

## Known limits
- Yahoo data is unofficial and can break. Finnhub is the backup, but its free tier has no price targets or price history, so upside and trend show n/a in backup mode.
- FDA meetings are matched to tickers by sponsor name. Some will not match and only show in the Calendar tab.
- News sentiment is keyword-based and crude.
- GitHub pauses scheduled runs after 60 days with no commits. Push any change to restart them.
- Information only, not financial advice. Earnings and FDA decisions can gap a stock 20%+ overnight.
