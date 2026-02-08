#!/usr/bin/env python3
"""
S&P 500 Value Screener - FMP Data Fetcher
==========================================
FMP 무료 플랜 (250 calls/day) 제약을 고려한 스마트 배칭 시스템.

Phase 1: 현재 데이터 수집 (TTM metrics + profiles + quotes)
Phase 2: 과거 데이터 수집 (historical quarterly metrics + price history)

GitHub Actions에서 매일 실행하면 자동으로 진행 상황을 추적하며
데이터를 점진적으로 수집합니다.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# ──────────────────────────────────────
# Configuration
# ──────────────────────────────────────
API_KEY = os.environ.get("FMP_API_KEY", "")
BASE_URL = "https://financialmodelingprep.com/api"
DATA_DIR = "data"
STATE_FILE = os.path.join(DATA_DIR, "_state.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "sp500_data.json")
MAX_CALLS_PER_RUN = 230  # 250 limit에 여유 두기
CALL_DELAY = 0.3  # seconds between API calls
KST = timezone(timedelta(hours=9))


def api_call(endpoint, params=None):
    """Make an FMP API call and return JSON response."""
    if not API_KEY:
        print("ERROR: FMP_API_KEY environment variable not set")
        sys.exit(1)

    url = f"{BASE_URL}{endpoint}?apikey={API_KEY}"
    if params:
        for k, v in params.items():
            url += f"&{k}={v}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "SP500Screener/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            time.sleep(CALL_DELAY)
            return data
    except urllib.error.HTTPError as e:
        print(f"  HTTP Error {e.code} for {endpoint}")
        return None
    except Exception as e:
        print(f"  Error for {endpoint}: {e}")
        return None


def load_state():
    """Load progress state from file."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {
        "sp500_list": [],
        "phase1_done": [],  # tickers with current data fetched
        "phase2_done": [],  # tickers with historical data fetched
        "profiles": {},
        "current_metrics": {},
        "historical_metrics": {},
        "price_data": {},
        "sector_averages": {},
        "last_run": None,
    }


def save_state(state):
    """Save progress state to file."""
    state["last_run"] = datetime.now(KST).isoformat()
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False)


def fetch_sp500_list(state):
    """Fetch S&P 500 constituent list."""
    print("📋 Fetching S&P 500 constituent list...")
    data = api_call("/v3/sp500_constituent")
    if data:
        state["sp500_list"] = [item["symbol"] for item in data]
        # Store basic info
        for item in data:
            sym = item["symbol"]
            if sym not in state["profiles"]:
                state["profiles"][sym] = {
                    "name": item.get("name", ""),
                    "sector": item.get("sector", ""),
                    "subSector": item.get("subSector", ""),
                }
        print(f"  → {len(state['sp500_list'])} tickers loaded")
        return 1  # 1 API call used
    return 0


def fetch_current_data(state, call_budget):
    """Phase 1: Fetch current TTM metrics + profiles for remaining tickers."""
    calls_used = 0
    todo = [t for t in state["sp500_list"] if t not in state["phase1_done"]]

    if not todo:
        print("✅ Phase 1 complete - all current data fetched")
        return 0

    print(f"📊 Phase 1: Fetching current data ({len(todo)} tickers remaining)...")

    for ticker in todo:
        if calls_used + 3 > call_budget:
            break

        # 1) Key Metrics TTM
        metrics = api_call(f"/v3/key-metrics-ttm/{ticker}")
        calls_used += 1

        if metrics and len(metrics) > 0:
            m = metrics[0]
            state["current_metrics"][ticker] = {
                "peRatioTTM": m.get("peRatioTTM"),
                "pegRatioTTM": m.get("pegRatioTTM"),
                "priceToBookRatioTTM": m.get("priceToBookRatioTTM"),
                "priceToSalesRatioTTM": m.get("priceToSalesRatioTTM"),
                "enterpriseValueOverEBITDATTM": m.get("enterpriseValueOverEBITDATTM"),
                "dividendYieldTTM": m.get("dividendYieldTTM"),
                "roeTTM": m.get("roeTTM"),
                "marketCapTTM": m.get("marketCapTTM"),
                "debtToEquityTTM": m.get("debtToEquityTTM"),
            }

        # 2) Profile (for forward PE, sector, etc.)
        profile = api_call(f"/v3/profile/{ticker}")
        calls_used += 1

        if profile and len(profile) > 0:
            p = profile[0]
            state["profiles"][ticker] = {
                "name": p.get("companyName", ""),
                "sector": p.get("sector", ""),
                "industry": p.get("industry", ""),
                "price": p.get("price"),
                "marketCap": p.get("mktCap"),
                "beta": p.get("beta"),
                "range": p.get("range", ""),  # "52week low - 52week high"
            }

            # Extract 52-week range from profile
            range_str = p.get("range", "")
            if "-" in range_str:
                try:
                    parts = range_str.split("-")
                    low_52w = float(parts[0].strip())
                    high_52w = float(parts[-1].strip())
                    current_price = p.get("price", 0)
                    discount = ((current_price - high_52w) / high_52w * 100) if high_52w else 0
                    state["price_data"][ticker] = {
                        "price": current_price,
                        "low_52w": low_52w,
                        "high_52w": high_52w,
                        "discount_52w": round(discount, 2),
                    }
                except (ValueError, IndexError):
                    pass

        # 3) Ratios TTM (for additional ratios)
        ratios = api_call(f"/v3/ratios-ttm/{ticker}")
        calls_used += 1

        if ratios and len(ratios) > 0:
            r = ratios[0]
            if ticker in state["current_metrics"]:
                state["current_metrics"][ticker].update({
                    "forwardPE": r.get("priceEarningsToGrowthRatioTTM"),
                    "priceToFreeCashFlowTTM": r.get("priceToFreeCashFlowsRatioTTM"),
                    "returnOnAssetsTTM": r.get("returnOnAssetsTTM"),
                    "currentRatioTTM": r.get("currentRatioTTM"),
                })

        state["phase1_done"].append(ticker)
        print(f"  ✓ {ticker} ({len(state['phase1_done'])}/{len(state['sp500_list'])})")

    return calls_used


def fetch_historical_data(state, call_budget):
    """Phase 2: Fetch historical quarterly metrics + price history."""
    calls_used = 0
    todo = [t for t in state["sp500_list"] if t not in state["phase2_done"]]

    if not todo:
        print("✅ Phase 2 complete - all historical data fetched")
        return 0

    print(f"📈 Phase 2: Fetching historical data ({len(todo)} tickers remaining)...")

    for ticker in todo:
        if calls_used + 2 > call_budget:
            break

        # 1) Historical Key Metrics (quarterly, 5 years = ~20 quarters)
        hist = api_call(f"/v3/key-metrics/{ticker}", {"period": "quarter", "limit": "20"})
        calls_used += 1

        if hist and len(hist) > 0:
            pe_history = []
            for h in hist:
                pe = h.get("peRatio")
                date = h.get("date", "")
                if pe is not None and pe > 0:
                    pe_history.append({"date": date, "pe": pe})

            state["historical_metrics"][ticker] = {
                "pe_history": pe_history,
            }

        # 2) Historical Daily Prices (for 6-month return calculation)
        # Get 2 years of daily data
        prices = api_call(f"/v3/historical-price-full/{ticker}", {"timeseries": "504"})
        calls_used += 1

        if prices and "historical" in prices:
            price_list = []
            for p in prices["historical"][:504]:  # ~2 years of trading days
                price_list.append({
                    "date": p.get("date"),
                    "close": p.get("close"),
                })

            if ticker not in state["price_data"]:
                state["price_data"][ticker] = {}
            state["price_data"][ticker]["daily_prices"] = price_list

        state["phase2_done"].append(ticker)
        print(f"  ✓ {ticker} ({len(state['phase2_done'])}/{len(state['sp500_list'])})")

    return calls_used


def fetch_sector_averages(state):
    """Fetch sector-level P/E averages."""
    print("🗺️ Fetching sector averages...")
    today = datetime.now().strftime("%Y-%m-%d")

    for exchange in ["NYSE", "NASDAQ"]:
        data = api_call("/v4/sector_price_earning_ratio", {
            "date": today,
            "exchange": exchange,
        })
        if data:
            for item in data:
                sector = item.get("sector", "")
                pe = item.get("pe")
                if sector and pe:
                    if sector not in state["sector_averages"]:
                        state["sector_averages"][sector] = {"pe_values": []}
                    state["sector_averages"][sector]["pe_values"].append(pe)

    # Average across exchanges
    for sector in state["sector_averages"]:
        vals = state["sector_averages"][sector]["pe_values"]
        state["sector_averages"][sector]["avg_pe"] = round(sum(vals) / len(vals), 2) if vals else 0

    return 2  # 2 API calls


def calculate_scores(state):
    """Calculate value scores, percentiles, and historical performance."""
    print("🔢 Calculating scores...")

    stocks = []
    all_pe = []
    all_pb = []
    all_peg = []
    all_ps = []

    # Collect valid values for percentile calculation
    for ticker in state["sp500_list"]:
        m = state["current_metrics"].get(ticker, {})
        pe = m.get("peRatioTTM")
        pb = m.get("priceToBookRatioTTM")
        peg = m.get("pegRatioTTM")
        ps = m.get("priceToSalesRatioTTM")

        if pe and 0 < pe < 500:
            all_pe.append(pe)
        if pb and 0 < pb < 200:
            all_pb.append(pb)
        if peg and -10 < peg < 50:
            all_peg.append(peg)
        if ps and 0 < ps < 200:
            all_ps.append(ps)

    all_pe.sort()
    all_pb.sort()
    all_peg.sort()
    all_ps.sort()

    def percentile_rank(value, sorted_list):
        if not sorted_list or value is None:
            return 50
        count = sum(1 for v in sorted_list if v <= value)
        return round(count / len(sorted_list) * 100)

    # Sector aggregation
    sector_data = {}

    for ticker in state["sp500_list"]:
        m = state["current_metrics"].get(ticker, {})
        p = state["profiles"].get(ticker, {})
        pd = state["price_data"].get(ticker, {})
        hm = state["historical_metrics"].get(ticker, {})

        pe = m.get("peRatioTTM")
        pb = m.get("priceToBookRatioTTM")
        peg = m.get("pegRatioTTM")
        ps = m.get("priceToSalesRatioTTM")

        # PE percentile within S&P 500
        pe_rank = percentile_rank(pe, all_pe) if pe and 0 < pe < 500 else 50
        pb_rank = percentile_rank(pb, all_pb) if pb and 0 < pb < 200 else 50
        peg_rank = percentile_rank(peg, all_peg) if peg and -10 < peg < 50 else 50
        ps_rank = percentile_rank(ps, all_ps) if ps and 0 < ps < 200 else 50

        # Value Score (0-100, higher = more undervalued)
        # Invert percentile ranks: low PE/PB/PEG/PS = high score
        value_score = round(
            (100 - pe_rank) * 0.30 +
            (100 - pb_rank) * 0.20 +
            (100 - peg_rank) * 0.20 +
            (100 - ps_rank) * 0.15 +
            max(0, min(100, -pd.get("discount_52w", 0) * 2)) * 0.15  # 52w discount factor
        )
        value_score = max(0, min(100, value_score))

        # PE Percentile in own 5-year history (for thermometer)
        pe_percentile_hist = 50
        pe_history = hm.get("pe_history", [])
        if pe and pe_history and len(pe_history) >= 4:
            hist_pes = [h["pe"] for h in pe_history if h["pe"] and 0 < h["pe"] < 500]
            if hist_pes:
                hist_pes.sort()
                pe_percentile_hist = percentile_rank(pe, hist_pes)

        # Historical performance analysis
        # "과거에 비슷한 PE 수준일 때 6개월 후 수익률"
        hist_performance = calculate_historical_performance(ticker, pe, pe_history, pd)

        # Sector aggregation
        sector = p.get("sector", "Other")
        if sector not in sector_data:
            sector_data[sector] = {"pe_values": [], "pb_values": [], "count": 0}
        sector_data[sector]["count"] += 1
        if pe and 0 < pe < 500:
            sector_data[sector]["pe_values"].append(pe)
        if pb and 0 < pb < 200:
            sector_data[sector]["pb_values"].append(pb)

        stock = {
            "ticker": ticker,
            "name": p.get("name", ""),
            "sector": sector,
            "industry": p.get("industry", ""),
            "price": pd.get("price") or p.get("price"),
            "marketCap": p.get("marketCap") or m.get("marketCapTTM"),
            "pe": round(pe, 2) if pe else None,
            "forwardPE": round(m.get("forwardPE"), 2) if m.get("forwardPE") else None,
            "pb": round(pb, 2) if pb else None,
            "ps": round(ps, 2) if ps else None,
            "peg": round(peg, 2) if peg else None,
            "evEbitda": round(m.get("enterpriseValueOverEBITDATTM"), 2) if m.get("enterpriseValueOverEBITDATTM") else None,
            "dividendYield": round(m.get("dividendYieldTTM", 0) * 100, 2) if m.get("dividendYieldTTM") else None,
            "roe": round(m.get("roeTTM", 0) * 100, 2) if m.get("roeTTM") else None,
            "high52w": pd.get("high_52w"),
            "low52w": pd.get("low_52w"),
            "discount52w": pd.get("discount_52w"),
            "valueScore": value_score,
            "pePercentile": pe_percentile_hist,
            "peRank": pe_rank,
            "histPerformance": hist_performance,
        }
        stocks.append(stock)

    # Calculate sector averages
    sectors = {}
    for sector, sd in sector_data.items():
        pe_vals = sd["pe_values"]
        pb_vals = sd["pb_values"]
        sectors[sector] = {
            "avgPE": round(sum(pe_vals) / len(pe_vals), 1) if pe_vals else None,
            "avgPB": round(sum(pb_vals) / len(pb_vals), 1) if pb_vals else None,
            "count": sd["count"],
        }

    # Summary stats
    valid_scores = [s["valueScore"] for s in stocks if s["valueScore"] is not None]
    valid_pe = [s["pe"] for s in stocks if s["pe"] and 0 < s["pe"] < 500]

    summary = {
        "totalStocks": len(stocks),
        "avgPE": round(sum(valid_pe) / len(valid_pe), 1) if valid_pe else 0,
        "undervalued": sum(1 for s in stocks if s["valueScore"] >= 65),
        "overvalued": sum(1 for s in stocks if s["valueScore"] <= 35),
        "fairValue": sum(1 for s in stocks if 35 < s["valueScore"] < 65),
    }

    return stocks, sectors, summary


def calculate_historical_performance(ticker, current_pe, pe_history, price_data):
    """
    과거에 비슷한 PE 수준일 때 6개월 후 수익률을 계산.
    """
    result = {
        "similarCount": 0,
        "avg6mReturn": None,
        "winRate": None,
        "cases": [],
    }

    if not current_pe or not pe_history or len(pe_history) < 4:
        return result

    daily_prices = price_data.get("daily_prices", [])
    if not daily_prices or len(daily_prices) < 126:  # 6 months of trading days
        return result

    # Create price lookup
    price_map = {}
    for p in daily_prices:
        price_map[p["date"]] = p["close"]

    # Define "similar PE zone" = ±15% of current PE
    pe_low = current_pe * 0.85
    pe_high = current_pe * 1.15

    returns_6m = []
    cases = []

    for i, h in enumerate(pe_history):
        if pe_low <= h["pe"] <= pe_high:
            entry_date = h["date"]
            # Find 6-month later price
            entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
            target_dt = entry_dt + timedelta(days=180)

            # Find closest price to entry and target dates
            entry_price = find_closest_price(entry_date, daily_prices)
            target_price = find_closest_price(target_dt.strftime("%Y-%m-%d"), daily_prices)

            if entry_price and target_price and entry_price > 0:
                ret = round((target_price - entry_price) / entry_price * 100, 1)
                returns_6m.append(ret)
                cases.append({
                    "date": entry_date[:7],  # YYYY-MM
                    "pe": round(h["pe"], 1),
                    "return6m": ret,
                })

    if returns_6m:
        result["similarCount"] = len(returns_6m)
        result["avg6mReturn"] = round(sum(returns_6m) / len(returns_6m), 1)
        result["winRate"] = round(sum(1 for r in returns_6m if r > 0) / len(returns_6m) * 100)
        result["cases"] = cases[:6]  # Max 6 cases

    return result


def find_closest_price(target_date, daily_prices):
    """Find the closest price to a target date."""
    if not daily_prices:
        return None

    best = None
    best_diff = float("inf")

    target_dt = datetime.strptime(target_date[:10], "%Y-%m-%d")

    for p in daily_prices:
        try:
            p_dt = datetime.strptime(p["date"], "%Y-%m-%d")
            diff = abs((p_dt - target_dt).days)
            if diff < best_diff:
                best_diff = diff
                best = p["close"]
            if diff == 0:
                break
        except (ValueError, KeyError):
            continue

    return best if best_diff <= 10 else None  # Within 10 days


def generate_output(stocks, sectors, summary):
    """Generate final JSON output for the frontend."""
    output = {
        "lastUpdated": datetime.now(KST).strftime("%Y.%m.%d %H:%M KST"),
        "summary": summary,
        "stocks": sorted(stocks, key=lambda x: x.get("pe") or 9999),
        "sectors": sectors,
    }

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"💾 Output saved to {OUTPUT_FILE}")
    print(f"   Total stocks: {summary['totalStocks']}")
    print(f"   Avg P/E: {summary['avgPE']}")
    print(f"   Undervalued: {summary['undervalued']}")
    print(f"   Overvalued: {summary['overvalued']}")


def main():
    print("=" * 50)
    print("S&P 500 Value Screener - Data Fetcher")
    print(f"Time: {datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')}")
    print("=" * 50)

    if not API_KEY:
        print("❌ FMP_API_KEY not set. Set it as environment variable or GitHub secret.")
        sys.exit(1)

    state = load_state()
    calls_used = 0

    # Step 1: Get S&P 500 list if not loaded
    if not state["sp500_list"]:
        calls_used += fetch_sp500_list(state)
        save_state(state)

    # Step 2: Fetch current data (Phase 1)
    remaining_budget = MAX_CALLS_PER_RUN - calls_used
    phase1_todo = len([t for t in state["sp500_list"] if t not in state["phase1_done"]])

    if phase1_todo > 0:
        calls_used += fetch_current_data(state, remaining_budget)
        save_state(state)
        print(f"\n📊 Phase 1 progress: {len(state['phase1_done'])}/{len(state['sp500_list'])}")

    # Step 3: Fetch historical data (Phase 2) - only if Phase 1 is done
    remaining_budget = MAX_CALLS_PER_RUN - calls_used
    phase2_todo = len([t for t in state["sp500_list"] if t not in state["phase2_done"]])

    if phase1_todo == 0 and phase2_todo > 0 and remaining_budget > 5:
        calls_used += fetch_historical_data(state, remaining_budget)
        save_state(state)
        print(f"\n📈 Phase 2 progress: {len(state['phase2_done'])}/{len(state['sp500_list'])}")

    # Step 4: Fetch sector averages (if budget allows)
    if remaining_budget > 5 and not state["sector_averages"]:
        calls_used += fetch_sector_averages(state)
        save_state(state)

    # Step 5: Calculate scores and generate output (always, with whatever data we have)
    if len(state["phase1_done"]) > 50:  # At least 50 tickers to be useful
        stocks, sectors, summary = calculate_scores(state)
        generate_output(stocks, sectors, summary)

    print(f"\n📡 Total API calls this run: {calls_used}")
    print(f"Phase 1: {len(state['phase1_done'])}/{len(state['sp500_list'])} tickers")
    print(f"Phase 2: {len(state['phase2_done'])}/{len(state['sp500_list'])} tickers")

    # Estimate completion
    if phase1_todo > 0:
        days_left = (phase1_todo * 3) // MAX_CALLS_PER_RUN + 1
        print(f"⏰ Phase 1 완료까지 약 {days_left}일 남음")
    elif phase2_todo > 0:
        days_left = (phase2_todo * 2) // MAX_CALLS_PER_RUN + 1
        print(f"⏰ Phase 2 완료까지 약 {days_left}일 남음")
    else:
        print("🎉 모든 데이터 수집 완료!")


if __name__ == "__main__":
    main()
