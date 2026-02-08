#!/usr/bin/env python3
"""
Sample data generator for testing the dashboard without API access.
Run: python generate_sample.py
"""
import json
import random
import os

random.seed(42)

SECTORS = {
    "Technology": ["AAPL", "MSFT", "NVDA", "GOOG", "META", "AVGO", "ORCL", "CRM", "AMD", "ADBE", "INTC", "CSCO", "QCOM", "TXN", "NOW", "IBM", "PLTR", "ANET", "MU", "SNPS"],
    "Financials": ["JPM", "V", "MA", "BAC", "WFC", "GS", "MS", "BLK", "C", "SCHW", "AXP", "PGR", "CB", "ICE", "CME", "MMC", "MCO", "AON", "MET", "AIG"],
    "Healthcare": ["UNH", "LLY", "JNJ", "ABBV", "MRK", "TMO", "ABT", "PFE", "AMGN", "MDT", "BMY", "CVS", "CI", "ELV", "GILD", "ISRG", "VRTX", "SYK", "BSX", "ZTS"],
    "Consumer Discretionary": ["AMZN", "TSLA", "HD", "MCD", "NKE", "LOW", "SBUX", "TJX", "BKNG", "CMG", "GM", "F", "ROST", "MAR", "HLT", "ORLY", "AZO", "DHI", "LULU", "DG"],
    "Industrials": ["GE", "CAT", "UNP", "HON", "UPS", "RTX", "BA", "DE", "LMT", "MMM", "GD", "NOC", "WM", "ETN", "ITW", "EMR", "FDX", "CSX", "NSC", "AXON"],
    "Communication Services": ["GOOGL", "NFLX", "DIS", "CMCSA", "VZ", "T", "TMUS", "CHTR", "EA", "WBD", "MTCH", "OMC", "IPG", "FOXA", "LYV"],
    "Consumer Staples": ["PG", "KO", "PEP", "COST", "WMT", "PM", "MO", "CL", "MDLZ", "GIS", "KHC", "HSY", "K", "SJM", "CAG"],
    "Energy": ["XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO", "OXY", "PXD", "HES", "DVN", "HAL", "FANG", "BKR"],
    "Utilities": ["NEE", "DUK", "SO", "AEP", "D", "SRE", "EXC", "XEL", "WEC", "ED", "ES", "AWK", "DTE", "PPL", "FE"],
    "Real Estate": ["PLD", "AMT", "CCI", "EQIX", "PSA", "SPG", "O", "VICI", "WELL", "DLR", "AVB", "EQR", "ARE", "MAA", "UDR"],
    "Materials": ["LIN", "APD", "SHW", "FCX", "NEM", "ECL", "DOW", "NUE", "VMC", "MLM", "PPG", "CF", "ALB", "BALL", "CE"],
}

PE_RANGES = {
    "Technology": (15, 80),
    "Financials": (8, 18),
    "Healthcare": (10, 45),
    "Consumer Discretionary": (12, 90),
    "Industrials": (15, 35),
    "Communication Services": (12, 30),
    "Consumer Staples": (18, 28),
    "Energy": (7, 15),
    "Utilities": (14, 22),
    "Real Estate": (25, 55),
    "Materials": (12, 25),
}

COMPANY_NAMES = {
    "AAPL": "Apple Inc.", "MSFT": "Microsoft Corp.", "NVDA": "NVIDIA Corp.",
    "GOOG": "Alphabet Inc.", "META": "Meta Platforms", "AMZN": "Amazon.com Inc.",
    "TSLA": "Tesla Inc.", "JPM": "JPMorgan Chase", "V": "Visa Inc.",
    "UNH": "UnitedHealth Group", "LLY": "Eli Lilly", "JNJ": "Johnson & Johnson",
    "AVGO": "Broadcom Inc.", "MA": "Mastercard", "PG": "Procter & Gamble",
    "HD": "Home Depot", "ORCL": "Oracle Corp.", "ABBV": "AbbVie Inc.",
    "MRK": "Merck & Co.", "CRM": "Salesforce Inc.", "KO": "Coca-Cola",
    "PEP": "PepsiCo Inc.", "COST": "Costco Wholesale", "WMT": "Walmart Inc.",
    "BAC": "Bank of America", "AMD": "AMD Inc.", "TMO": "Thermo Fisher",
    "ADBE": "Adobe Inc.", "MCD": "McDonald's", "NKE": "Nike Inc.",
    "INTC": "Intel Corp.", "CSCO": "Cisco Systems", "PFE": "Pfizer Inc.",
    "GM": "General Motors", "F": "Ford Motor Co.", "CVS": "CVS Health",
    "C": "Citigroup Inc.", "VZ": "Verizon Comm.", "BMY": "Bristol-Myers Squibb",
    "MU": "Micron Technology", "XOM": "Exxon Mobil", "CVX": "Chevron Corp.",
    "NEE": "NextEra Energy", "GE": "GE Aerospace", "CAT": "Caterpillar Inc.",
    "BA": "Boeing Co.", "PLTR": "Palantir Tech.", "NFLX": "Netflix Inc.",
    "DIS": "Walt Disney", "WFC": "Wells Fargo", "GS": "Goldman Sachs",
    "T": "AT&T Inc.", "LOW": "Lowe's Companies", "SBUX": "Starbucks Corp.",
    "BKNG": "Booking Holdings", "LMT": "Lockheed Martin", "RTX": "RTX Corp.",
    "AXON": "Axon Enterprise", "DG": "Dollar General", "LULU": "Lululemon",
    "MRNA": "Moderna Inc.", "ENPH": "Enphase Energy",
}


def gen_pe(sector):
    lo, hi = PE_RANGES.get(sector, (10, 40))
    return round(random.uniform(lo, hi), 1)


def gen_stock(ticker, sector):
    pe = gen_pe(sector)
    price = round(random.uniform(20, 600), 2)
    high52 = round(price * random.uniform(1.0, 1.6), 2)
    low52 = round(price * random.uniform(0.5, 1.0), 2)
    discount = round((price - high52) / high52 * 100, 2)
    
    # PE percentile (history-based)
    pe_pct = random.randint(2, 98)
    # Bias: low PE stocks tend to have low percentile
    if pe < PE_RANGES.get(sector, (10, 40))[0] * 1.2:
        pe_pct = random.randint(2, 30)
    elif pe > PE_RANGES.get(sector, (10, 40))[1] * 0.8:
        pe_pct = random.randint(70, 98)

    # Value score
    pe_rank = min(100, max(0, int(pe / 80 * 100)))
    value_score = max(0, min(100, 100 - pe_rank + random.randint(-15, 15)))

    # Historical performance
    n_cases = random.randint(2, 5)
    cases = []
    returns = []
    for _ in range(n_cases):
        year = random.randint(2020, 2024)
        month = random.randint(1, 12)
        if pe_pct < 30:  # undervalued tends to go up
            ret = round(random.gauss(15, 20), 1)
        elif pe_pct > 70:  # overvalued tends to go down
            ret = round(random.gauss(-8, 18), 1)
        else:
            ret = round(random.gauss(5, 15), 1)
        returns.append(ret)
        cases.append({
            "date": f"{year}-{month:02d}",
            "pe": round(pe * random.uniform(0.85, 1.15), 1),
            "return6m": ret,
        })

    hist = {
        "similarCount": n_cases,
        "avg6mReturn": round(sum(returns) / len(returns), 1) if returns else None,
        "winRate": round(sum(1 for r in returns if r > 0) / len(returns) * 100) if returns else None,
        "cases": sorted(cases, key=lambda x: x["date"]),
    }

    mcap_base = random.uniform(5, 3000)

    return {
        "ticker": ticker,
        "name": COMPANY_NAMES.get(ticker, f"{ticker} Corp."),
        "sector": sector,
        "industry": "",
        "price": price,
        "marketCap": round(mcap_base * 1e9),
        "pe": pe,
        "forwardPE": round(pe * random.uniform(0.7, 1.1), 1),
        "pb": round(random.uniform(0.5, 50), 1),
        "ps": round(random.uniform(0.2, 20), 1),
        "peg": round(random.uniform(0.1, 5), 2),
        "evEbitda": round(random.uniform(5, 40), 1),
        "dividendYield": round(random.uniform(0, 5), 2),
        "roe": round(random.uniform(-5, 50), 1),
        "high52w": high52,
        "low52w": low52,
        "discount52w": discount,
        "valueScore": value_score,
        "pePercentile": pe_pct,
        "peRank": pe_rank,
        "histPerformance": hist,
    }


def main():
    stocks = []
    sector_data = {}

    for sector, tickers in SECTORS.items():
        pe_vals = []
        pb_vals = []
        for ticker in tickers:
            stock = gen_stock(ticker, sector)
            stocks.append(stock)
            if stock["pe"]:
                pe_vals.append(stock["pe"])
            if stock["pb"]:
                pb_vals.append(stock["pb"])

        sector_data[sector] = {
            "avgPE": round(sum(pe_vals) / len(pe_vals), 1) if pe_vals else None,
            "avgPB": round(sum(pb_vals) / len(pb_vals), 1) if pb_vals else None,
            "count": len(tickers),
        }

    # Sort by PE
    stocks.sort(key=lambda x: x.get("pe") or 9999)

    valid_pe = [s["pe"] for s in stocks if s["pe"]]
    summary = {
        "totalStocks": len(stocks),
        "avgPE": round(sum(valid_pe) / len(valid_pe), 1),
        "undervalued": sum(1 for s in stocks if s["valueScore"] >= 65),
        "overvalued": sum(1 for s in stocks if s["valueScore"] <= 35),
        "fairValue": sum(1 for s in stocks if 35 < s["valueScore"] < 65),
    }

    output = {
        "lastUpdated": "2025.02.08 07:00 KST",
        "summary": summary,
        "stocks": stocks,
        "sectors": sector_data,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/sp500_data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"✅ Sample data generated: {len(stocks)} stocks")
    print(f"   Undervalued: {summary['undervalued']}")
    print(f"   Overvalued: {summary['overvalued']}")
    print(f"   Fair Value: {summary['fairValue']}")


if __name__ == "__main__":
    main()
