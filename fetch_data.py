#!/usr/bin/env python3
"""
S&P 500 Value Screener - Data Fetcher (yfinance version)
No API key needed. Uses yfinance + hardcoded S&P 500 list.
"""
import json, os, time, sys
from datetime import datetime, timezone, timedelta

try:
    import yfinance as yf
except ImportError:
    print("Installing yfinance...")
    os.system(f"{sys.executable} -m pip install yfinance --break-system-packages -q")
    import yfinance as yf

# ─── S&P 500 Constituents (hardcoded) ───
SP500 = {
    "MMM": ("3M", "Industrials"),
    "AOS": ("A. O. 스미스", "Industrials"),
    "ABT": ("애벗 래버러토리스", "Health Care"),
    "ABBV": ("애브비", "Health Care"),
    "ACN": ("액센츄어", "Information Technology"),
    "ADBE": ("어도비", "Information Technology"),
    "AMD": ("AMD", "Information Technology"),
    "AES": ("AES Corporation", "Utilities"),
    "AFL": ("아플락", "Financials"),
    "A": ("애질런트 테크놀로지스", "Health Care"),
    "APD": ("에어프로덕츠", "Materials"),
    "ABNB": ("에어비앤비", "Consumer Discretionary"),
    "AKAM": ("아카마이 테크놀로지스", "Information Technology"),
    "ALB": ("알버말", "Materials"),
    "ARE": ("Alexandria Real Estate Equities", "Real Estate"),
    "ALGN": ("Align Technology", "Health Care"),
    "ALLE": ("Allegion", "Industrials"),
    "LNT": ("Alliant Energy", "Utilities"),
    "ALL": ("올스테이트", "Financials"),
    "GOOGL": ("알파벳 (A)", "Communication Services"),
    "GOOG": ("알파벳 (C)", "Communication Services"),
    "MO": ("알트리아", "Consumer Staples"),
    "AMZN": ("아마존", "Consumer Discretionary"),
    "AMCR": ("Amcor", "Materials"),
    "AEE": ("Ameren", "Utilities"),
    "AEP": ("아메리칸 일렉트릭 파워", "Utilities"),
    "AXP": ("아메리칸 익스프레스", "Financials"),
    "AIG": ("AIG", "Financials"),
    "AMT": ("American Tower", "Real Estate"),
    "AWK": ("American Water Works", "Utilities"),
    "AMP": ("Ameriprise Financial", "Financials"),
    "AME": ("Ametek", "Industrials"),
    "AMGN": ("Amgen", "Health Care"),
    "APH": ("Amphenol", "Information Technology"),
    "ADI": ("Analog Devices", "Information Technology"),
    "ANSS": ("Ansys", "Information Technology"),
    "AON": ("Aon", "Financials"),
    "APA": ("APA Corporation", "Energy"),
    "APO": ("Apollo Global Management", "Financials"),
    "AAPL": ("Apple", "Information Technology"),
    "AMAT": ("Applied Materials", "Information Technology"),
    "APTV": ("Aptiv", "Consumer Discretionary"),
    "ACGL": ("Arch Capital Group", "Financials"),
    "ADM": ("아처 대니얼스 미들랜드", "Consumer Staples"),
    "ANET": ("Arista Networks", "Information Technology"),
    "AJG": ("Arthur J. Gallagher", "Financials"),
    "AIZ": ("Assurant", "Financials"),
    "T": ("AT&T", "Communication Services"),
    "ATO": ("Atmos Energy", "Utilities"),
    "ADSK": ("Autodesk", "Information Technology"),
    "ADP": ("Automatic Data Processing", "Industrials"),
    "AZO": ("AutoZone", "Consumer Discretionary"),
    "AVB": ("AvalonBay Communities", "Real Estate"),
    "AVY": ("Avery Dennison", "Materials"),
    "AXON": ("Axon Enterprise", "Industrials"),
    "BKR": ("Baker Hughes", "Energy"),
    "BALL": ("Ball Corporation", "Materials"),
    "BAC": ("Bank of America", "Financials"),
    "BAX": ("Baxter International", "Health Care"),
    "BDX": ("Becton Dickinson", "Health Care"),
    "BRK-B": ("Berkshire Hathaway", "Financials"),
    "BBY": ("Best Buy", "Consumer Discretionary"),
    "TECH": ("Bio-Techne", "Health Care"),
    "BIIB": ("Biogen", "Health Care"),
    "BLK": ("BlackRock", "Financials"),
    "BX": ("Blackstone", "Financials"),
    "BK": ("뉴욕멜론은행", "Financials"),
    "BA": ("보잉", "Industrials"),
    "BKNG": ("Booking Holdings", "Consumer Discretionary"),
    "BSX": ("Boston Scientific", "Health Care"),
    "BMY": ("브리스톨 마이어스 스퀴브", "Health Care"),
    "AVGO": ("Broadcom", "Information Technology"),
    "BR": ("Broadridge Financial", "Industrials"),
    "BRO": ("Brown & Brown", "Financials"),
    "BF-B": ("Brown-Forman", "Consumer Staples"),
    "BLDR": ("Builders FirstSource", "Industrials"),
    "BG": ("Bunge Global", "Consumer Staples"),
    "BXP": ("BXP Inc", "Real Estate"),
    "CHRW": ("C.H. Robinson", "Industrials"),
    "CDNS": ("Cadence Design Systems", "Information Technology"),
    "CZR": ("Caesars Entertainment", "Consumer Discretionary"),
    "CPT": ("Camden Property Trust", "Real Estate"),
    "CPB": ("캠벨 수프", "Consumer Staples"),
    "COF": ("Capital One", "Financials"),
    "CAH": ("Cardinal Health", "Health Care"),
    "KMX": ("CarMax", "Consumer Discretionary"),
    "CCL": ("Carnival", "Consumer Discretionary"),
    "CARR": ("Carrier Global", "Industrials"),
    "CAT": ("캐터필러", "Industrials"),
    "CBOE": ("Cboe Global Markets", "Financials"),
    "CBRE": ("CBRE Group", "Real Estate"),
    "CDW": ("CDW Corporation", "Information Technology"),
    "COR": ("Cencora", "Health Care"),
    "CNC": ("Centene", "Health Care"),
    "CNP": ("CenterPoint Energy", "Utilities"),
    "CF": ("CF Industries", "Materials"),
    "CRL": ("Charles River Labs", "Health Care"),
    "SCHW": ("Charles Schwab", "Financials"),
    "CHTR": ("Charter Communications", "Communication Services"),
    "CVX": ("셰브론", "Energy"),
    "CMG": ("Chipotle", "Consumer Discretionary"),
    "CB": ("Chubb", "Financials"),
    "CHD": ("Church & Dwight", "Consumer Staples"),
    "CI": ("Cigna", "Health Care"),
    "CINF": ("Cincinnati Financial", "Financials"),
    "CTAS": ("Cintas", "Industrials"),
    "CSCO": ("Cisco", "Information Technology"),
    "C": ("Citigroup", "Financials"),
    "CFG": ("Citizens Financial", "Financials"),
    "CLX": ("Clorox", "Consumer Staples"),
    "CME": ("CME Group", "Financials"),
    "CMS": ("CMS Energy", "Utilities"),
    "KO": ("코카콜라", "Consumer Staples"),
    "CTSH": ("Cognizant", "Information Technology"),
    "CL": ("콜게이트-파몰리브", "Consumer Staples"),
    "CMCSA": ("Comcast", "Communication Services"),
    "CAG": ("Conagra Brands", "Consumer Staples"),
    "COP": ("ConocoPhillips", "Energy"),
    "ED": ("Consolidated Edison", "Utilities"),
    "STZ": ("Constellation Brands", "Consumer Staples"),
    "CEG": ("Constellation Energy", "Utilities"),
    "COO": ("Cooper Companies", "Health Care"),
    "CPRT": ("Copart", "Industrials"),
    "GLW": ("Corning", "Information Technology"),
    "CPAY": ("Corpay", "Financials"),
    "CTVA": ("Corteva", "Materials"),
    "CSGP": ("CoStar Group", "Real Estate"),
    "COST": ("Costco", "Consumer Staples"),
    "CTRA": ("Coterra", "Energy"),
    "CRWD": ("CrowdStrike", "Information Technology"),
    "CCI": ("Crown Castle", "Real Estate"),
    "CSX": ("CSX Corporation", "Industrials"),
    "CMI": ("Cummins", "Industrials"),
    "CVS": ("CVS Health", "Health Care"),
    "DHR": ("Danaher", "Health Care"),
    "DRI": ("Darden Restaurants", "Consumer Discretionary"),
    "DVA": ("DaVita", "Health Care"),
    "DAY": ("Dayforce", "Industrials"),
    "DECK": ("Deckers Brands", "Consumer Discretionary"),
    "DE": ("Deere & Company", "Industrials"),
    "DELL": ("Dell Technologies", "Information Technology"),
    "DAL": ("Delta Air Lines", "Industrials"),
    "DVN": ("Devon Energy", "Energy"),
    "DXCM": ("Dexcom", "Health Care"),
    "FANG": ("Diamondback Energy", "Energy"),
    "DLR": ("Digital Realty", "Real Estate"),
    "DFS": ("Discover Financial", "Financials"),
    "DG": ("Dollar General", "Consumer Staples"),
    "DLTR": ("Dollar Tree", "Consumer Staples"),
    "D": ("Dominion Energy", "Utilities"),
    "DPZ": ("Domino\'s", "Consumer Discretionary"),
    "DASH": ("DoorDash", "Consumer Discretionary"),
    "DOV": ("Dover Corporation", "Industrials"),
    "DOW": ("Dow Inc", "Materials"),
    "DHI": ("D.R. Horton", "Consumer Discretionary"),
    "DTE": ("DTE Energy", "Utilities"),
    "DUK": ("Duke Energy", "Utilities"),
    "DD": ("DuPont", "Materials"),
    "EMN": ("Eastman Chemical", "Materials"),
    "ETN": ("Eaton Corporation", "Industrials"),
    "EBAY": ("eBay", "Consumer Discretionary"),
    "ECL": ("Ecolab", "Materials"),
    "EIX": ("Edison International", "Utilities"),
    "EW": ("Edwards Lifesciences", "Health Care"),
    "EA": ("Electronic Arts", "Communication Services"),
    "ELV": ("Elevance Health", "Health Care"),
    "EMR": ("Emerson Electric", "Industrials"),
    "ENPH": ("Enphase Energy", "Information Technology"),
    "ETR": ("Entergy", "Utilities"),
    "EOG": ("EOG Resources", "Energy"),
    "EPAM": ("EPAM Systems", "Information Technology"),
    "EQT": ("EQT Corporation", "Energy"),
    "EFX": ("Equifax", "Industrials"),
    "EQIX": ("Equinix", "Real Estate"),
    "EQR": ("Equity Residential", "Real Estate"),
    "ERIE": ("Erie Indemnity", "Financials"),
    "ESS": ("Essex Property Trust", "Real Estate"),
    "EL": ("에스티 로더", "Consumer Staples"),
    "EG": ("Everest Group", "Financials"),
    "EVRG": ("Evergy", "Utilities"),
    "ES": ("Eversource Energy", "Utilities"),
    "EXC": ("Exelon", "Utilities"),
    "EXE": ("Expand Energy", "Energy"),
    "EXPE": ("Expedia", "Consumer Discretionary"),
    "EXPD": ("Expeditors International", "Industrials"),
    "EXR": ("Extra Space Storage", "Real Estate"),
    "XOM": ("ExxonMobil", "Energy"),
    "FFIV": ("F5 Inc", "Information Technology"),
    "FDS": ("FactSet", "Financials"),
    "FICO": ("Fair Isaac", "Information Technology"),
    "FAST": ("Fastenal", "Industrials"),
    "FRT": ("Federal Realty", "Real Estate"),
    "FDX": ("FedEx", "Industrials"),
    "FIS": ("FIS", "Financials"),
    "FITB": ("Fifth Third Bancorp", "Financials"),
    "FSLR": ("First Solar", "Information Technology"),
    "FE": ("FirstEnergy", "Utilities"),
    "FI": ("Fiserv", "Financials"),
    "F": ("Ford", "Consumer Discretionary"),
    "FTNT": ("Fortinet", "Information Technology"),
    "FTV": ("Fortive", "Industrials"),
    "FOXA": ("Fox Corp (A)", "Communication Services"),
    "FOX": ("Fox Corp (B)", "Communication Services"),
    "BEN": ("Franklin Resources", "Financials"),
    "FCX": ("Freeport-McMoRan", "Materials"),
    "GRMN": ("Garmin", "Consumer Discretionary"),
    "IT": ("Gartner", "Information Technology"),
    "GE": ("GE Aerospace", "Industrials"),
    "GEHC": ("GE HealthCare", "Health Care"),
    "GEV": ("GE Vernova", "Industrials"),
    "GEN": ("Gen Digital", "Information Technology"),
    "GNRC": ("Generac", "Industrials"),
    "GD": ("General Dynamics", "Industrials"),
    "GIS": ("General Mills", "Consumer Staples"),
    "GM": ("General Motors", "Consumer Discretionary"),
    "GPC": ("Genuine Parts", "Consumer Discretionary"),
    "GILD": ("Gilead Sciences", "Health Care"),
    "GPN": ("Global Payments", "Financials"),
    "GL": ("Globe Life", "Financials"),
    "GDDY": ("GoDaddy", "Information Technology"),
    "GS": ("골드만삭스", "Financials"),
    "HAL": ("Halliburton", "Energy"),
    "HIG": ("하트퍼드", "Financials"),
    "HAS": ("Hasbro", "Consumer Discretionary"),
    "HCA": ("HCA Healthcare", "Health Care"),
    "DOC": ("Healthpeak Properties", "Real Estate"),
    "HSIC": ("Henry Schein", "Health Care"),
    "HSY": ("Hershey", "Consumer Staples"),
    "HES": ("Hess Corporation", "Energy"),
    "HPE": ("HP Enterprise", "Information Technology"),
    "HLT": ("Hilton Worldwide", "Consumer Discretionary"),
    "HOLX": ("Hologic", "Health Care"),
    "HD": ("Home Depot", "Consumer Discretionary"),
    "HON": ("Honeywell", "Industrials"),
    "HRL": ("Hormel Foods", "Consumer Staples"),
    "HST": ("Host Hotels & Resorts", "Real Estate"),
    "HWM": ("Howmet Aerospace", "Industrials"),
    "HPQ": ("HP Inc", "Information Technology"),
    "HUBB": ("Hubbell", "Industrials"),
    "HUM": ("Humana", "Health Care"),
    "HBAN": ("Huntington Bancshares", "Financials"),
    "HII": ("Huntington Ingalls", "Industrials"),
    "IBM": ("IBM", "Information Technology"),
    "IEX": ("IDEX Corporation", "Industrials"),
    "IDXX": ("Idexx Laboratories", "Health Care"),
    "ITW": ("Illinois Tool Works", "Industrials"),
    "INCY": ("Incyte", "Health Care"),
    "IR": ("Ingersoll Rand", "Industrials"),
    "PODD": ("Insulet", "Health Care"),
    "INTC": ("Intel", "Information Technology"),
    "ICE": ("Intercontinental Exchange", "Financials"),
    "IFF": ("IFF", "Materials"),
    "IP": ("International Paper", "Materials"),
    "IPG": ("Interpublic Group", "Communication Services"),
    "INTU": ("Intuit", "Information Technology"),
    "ISRG": ("Intuitive Surgical", "Health Care"),
    "IVZ": ("Invesco", "Financials"),
    "INVH": ("Invitation Homes", "Real Estate"),
    "IQV": ("IQVIA", "Health Care"),
    "IRM": ("Iron Mountain", "Real Estate"),
    "JBHT": ("J.B. Hunt", "Industrials"),
    "JBL": ("Jabil", "Information Technology"),
    "JKHY": ("Jack Henry", "Financials"),
    "J": ("Jacobs Solutions", "Industrials"),
    "JNJ": ("Johnson & Johnson", "Health Care"),
    "JCI": ("Johnson Controls", "Industrials"),
    "JPM": ("JPMorgan Chase", "Financials"),
    "JNPR": ("Juniper Networks", "Information Technology"),
    "K": ("Kellanova", "Consumer Staples"),
    "KVUE": ("Kenvue", "Consumer Staples"),
    "KDP": ("Keurig Dr Pepper", "Consumer Staples"),
    "KEY": ("KeyCorp", "Financials"),
    "KEYS": ("Keysight Technologies", "Information Technology"),
    "KMB": ("Kimberly-Clark", "Consumer Staples"),
    "KIM": ("Kimco Realty", "Real Estate"),
    "KMI": ("Kinder Morgan", "Energy"),
    "KKR": ("KKR & Co", "Financials"),
    "KLAC": ("KLA Corporation", "Information Technology"),
    "KHC": ("Kraft Heinz", "Consumer Staples"),
    "KR": ("Kroger", "Consumer Staples"),
    "LHX": ("L3Harris", "Industrials"),
    "LH": ("Labcorp", "Health Care"),
    "LRCX": ("Lam Research", "Information Technology"),
    "LW": ("Lamb Weston", "Consumer Staples"),
    "LVS": ("Las Vegas Sands", "Consumer Discretionary"),
    "LDOS": ("Leidos", "Industrials"),
    "LEN": ("Lennar", "Consumer Discretionary"),
    "LII": ("Lennox International", "Industrials"),
    "LLY": ("Eli Lilly", "Health Care"),
    "LIN": ("Linde", "Materials"),
    "LYV": ("Live Nation", "Communication Services"),
    "LKQ": ("LKQ Corporation", "Consumer Discretionary"),
    "LMT": ("Lockheed Martin", "Industrials"),
    "L": ("Loews Corporation", "Financials"),
    "LOW": ("Lowe\'s", "Consumer Discretionary"),
    "LULU": ("Lululemon", "Consumer Discretionary"),
    "LYB": ("LyondellBasell", "Materials"),
    "MTB": ("M&T Bank", "Financials"),
    "MPC": ("Marathon Petroleum", "Energy"),
    "MKTX": ("MarketAxess", "Financials"),
    "MAR": ("Marriott", "Consumer Discretionary"),
    "MMC": ("Marsh McLennan", "Financials"),
    "MLM": ("Martin Marietta", "Materials"),
    "MAS": ("Masco", "Industrials"),
    "MA": ("Mastercard", "Financials"),
    "MTCH": ("Match Group", "Communication Services"),
    "MKC": ("McCormick", "Consumer Staples"),
    "MCD": ("McDonald\'s", "Consumer Discretionary"),
    "MCK": ("McKesson", "Health Care"),
    "MDT": ("Medtronic", "Health Care"),
    "MRK": ("Merck", "Health Care"),
    "META": ("Meta Platforms", "Communication Services"),
    "MET": ("MetLife", "Financials"),
    "MTD": ("Mettler Toledo", "Health Care"),
    "MGM": ("MGM Resorts", "Consumer Discretionary"),
    "MCHP": ("Microchip Technology", "Information Technology"),
    "MU": ("Micron Technology", "Information Technology"),
    "MSFT": ("Microsoft", "Information Technology"),
    "MAA": ("Mid-America Apartment", "Real Estate"),
    "MRNA": ("Moderna", "Health Care"),
    "MHK": ("Mohawk Industries", "Consumer Discretionary"),
    "MOH": ("Molina Healthcare", "Health Care"),
    "TAP": ("Molson Coors", "Consumer Staples"),
    "MDLZ": ("Mondelez", "Consumer Staples"),
    "MPWR": ("Monolithic Power", "Information Technology"),
    "MNST": ("Monster Beverage", "Consumer Staples"),
    "MCO": ("Moody\'s", "Financials"),
    "MS": ("Morgan Stanley", "Financials"),
    "MOS": ("Mosaic", "Materials"),
    "MSI": ("Motorola Solutions", "Information Technology"),
    "MSCI": ("MSCI Inc", "Financials"),
    "NDAQ": ("Nasdaq Inc", "Financials"),
    "NTAP": ("NetApp", "Information Technology"),
    "NFLX": ("Netflix", "Communication Services"),
    "NEM": ("Newmont", "Materials"),
    "NWSA": ("News Corp (A)", "Communication Services"),
    "NWS": ("News Corp (B)", "Communication Services"),
    "NEE": ("NextEra Energy", "Utilities"),
    "NKE": ("Nike", "Consumer Discretionary"),
    "NI": ("NiSource", "Utilities"),
    "NDSN": ("Nordson", "Industrials"),
    "NSC": ("Norfolk Southern", "Industrials"),
    "NTRS": ("Northern Trust", "Financials"),
    "NOC": ("Northrop Grumman", "Industrials"),
    "NCLH": ("Norwegian Cruise Line", "Consumer Discretionary"),
    "NRG": ("NRG Energy", "Utilities"),
    "NUE": ("Nucor", "Materials"),
    "NVDA": ("NVIDIA", "Information Technology"),
    "NVR": ("NVR Inc", "Consumer Discretionary"),
    "NXPI": ("NXP Semiconductors", "Information Technology"),
    "ORLY": ("O\'Reilly Automotive", "Consumer Discretionary"),
    "OXY": ("Occidental Petroleum", "Energy"),
    "ODFL": ("Old Dominion", "Industrials"),
    "OMC": ("Omnicom", "Communication Services"),
    "ON": ("ON Semiconductor", "Information Technology"),
    "OKE": ("Oneok", "Energy"),
    "ORCL": ("Oracle", "Information Technology"),
    "OTIS": ("Otis Worldwide", "Industrials"),
    "PCAR": ("Paccar", "Industrials"),
    "PKG": ("Packaging Corp", "Materials"),
    "PLTR": ("Palantir", "Information Technology"),
    "PANW": ("Palo Alto Networks", "Information Technology"),
    "PARA": ("Paramount Global", "Communication Services"),
    "PH": ("Parker Hannifin", "Industrials"),
    "PAYX": ("Paychex", "Industrials"),
    "PAYC": ("Paycom", "Industrials"),
    "PYPL": ("PayPal", "Financials"),
    "PNR": ("Pentair", "Industrials"),
    "PEP": ("PepsiCo", "Consumer Staples"),
    "PFE": ("Pfizer", "Health Care"),
    "PCG": ("PG&E", "Utilities"),
    "PM": ("Philip Morris", "Consumer Staples"),
    "PSX": ("Phillips 66", "Energy"),
    "PNW": ("Pinnacle West", "Utilities"),
    "PNC": ("PNC Financial", "Financials"),
    "POOL": ("Pool Corporation", "Consumer Discretionary"),
    "PPG": ("PPG Industries", "Materials"),
    "PPL": ("PPL Corporation", "Utilities"),
    "PFG": ("Principal Financial", "Financials"),
    "PG": ("Procter & Gamble", "Consumer Staples"),
    "PGR": ("Progressive", "Financials"),
    "PLD": ("Prologis", "Real Estate"),
    "PRU": ("Prudential Financial", "Financials"),
    "PEG": ("PSEG", "Utilities"),
    "PTC": ("PTC Inc", "Information Technology"),
    "PSA": ("Public Storage", "Real Estate"),
    "PHM": ("PulteGroup", "Consumer Discretionary"),
    "PWR": ("Quanta Services", "Industrials"),
    "QCOM": ("Qualcomm", "Information Technology"),
    "DGX": ("Quest Diagnostics", "Health Care"),
    "RL": ("Ralph Lauren", "Consumer Discretionary"),
    "RJF": ("Raymond James", "Financials"),
    "RTX": ("RTX Corporation", "Industrials"),
    "O": ("Realty Income", "Real Estate"),
    "REG": ("Regency Centers", "Real Estate"),
    "REGN": ("Regeneron", "Health Care"),
    "RF": ("Regions Financial", "Financials"),
    "RSG": ("Republic Services", "Industrials"),
    "RMD": ("ResMed", "Health Care"),
    "RVTY": ("Revvity", "Health Care"),
    "ROK": ("Rockwell Automation", "Industrials"),
    "ROL": ("Rollins", "Industrials"),
    "ROP": ("Roper Technologies", "Information Technology"),
    "ROST": ("Ross Stores", "Consumer Discretionary"),
    "RCL": ("Royal Caribbean", "Consumer Discretionary"),
    "SPGI": ("S&P Global", "Financials"),
    "CRM": ("Salesforce", "Information Technology"),
    "SBAC": ("SBA Communications", "Real Estate"),
    "SLB": ("Schlumberger", "Energy"),
    "STX": ("Seagate Technology", "Information Technology"),
    "SRE": ("Sempra", "Utilities"),
    "NOW": ("ServiceNow", "Information Technology"),
    "SHW": ("Sherwin-Williams", "Materials"),
    "SPG": ("Simon Property Group", "Real Estate"),
    "SWKS": ("Skyworks Solutions", "Information Technology"),
    "SJM": ("J.M. Smucker", "Consumer Staples"),
    "SW": ("Smurfit Westrock", "Materials"),
    "SNA": ("Snap-on", "Industrials"),
    "SOLV": ("Solventum", "Health Care"),
    "SO": ("Southern Company", "Utilities"),
    "LUV": ("Southwest Airlines", "Industrials"),
    "SWK": ("Stanley Black & Decker", "Industrials"),
    "SBUX": ("Starbucks", "Consumer Discretionary"),
    "STT": ("스테이트 스트리트", "Financials"),
    "STLD": ("Steel Dynamics", "Materials"),
    "STE": ("Steris", "Health Care"),
    "SYK": ("Stryker", "Health Care"),
    "SMCI": ("Supermicro", "Information Technology"),
    "SYF": ("Synchrony Financial", "Financials"),
    "SNPS": ("Synopsys", "Information Technology"),
    "SYY": ("Sysco", "Consumer Staples"),
    "TMUS": ("T-Mobile US", "Communication Services"),
    "TROW": ("T. Rowe Price", "Financials"),
    "TTWO": ("Take-Two Interactive", "Communication Services"),
    "TPR": ("Tapestry", "Consumer Discretionary"),
    "TRGP": ("Targa Resources", "Energy"),
    "TGT": ("Target", "Consumer Staples"),
    "TEL": ("TE Connectivity", "Information Technology"),
    "TDY": ("Teledyne Technologies", "Information Technology"),
    "TER": ("Teradyne", "Information Technology"),
    "TSLA": ("Tesla", "Consumer Discretionary"),
    "TXN": ("Texas Instruments", "Information Technology"),
    "TPL": ("Texas Pacific Land", "Energy"),
    "TXT": ("Textron", "Industrials"),
    "TMO": ("Thermo Fisher", "Health Care"),
    "TJX": ("TJX Companies", "Consumer Discretionary"),
    "TKO": ("TKO Group", "Communication Services"),
    "TSCO": ("Tractor Supply", "Consumer Discretionary"),
    "TT": ("Trane Technologies", "Industrials"),
    "TDG": ("TransDigm", "Industrials"),
    "TRV": ("Travelers", "Financials"),
    "TRMB": ("Trimble", "Information Technology"),
    "TFC": ("Truist Financial", "Financials"),
    "TYL": ("Tyler Technologies", "Information Technology"),
    "TSN": ("Tyson Foods", "Consumer Staples"),
    "USB": ("U.S. Bancorp", "Financials"),
    "UBER": ("Uber", "Industrials"),
    "UDR": ("UDR Inc", "Real Estate"),
    "ULTA": ("Ulta Beauty", "Consumer Discretionary"),
    "UNP": ("Union Pacific", "Industrials"),
    "UAL": ("United Airlines", "Industrials"),
    "UPS": ("UPS", "Industrials"),
    "URI": ("United Rentals", "Industrials"),
    "UNH": ("UnitedHealth Group", "Health Care"),
    "UHS": ("Universal Health Services", "Health Care"),
    "VLO": ("Valero Energy", "Energy"),
    "VTR": ("Ventas", "Real Estate"),
    "VLTO": ("Veralto", "Industrials"),
    "VRSN": ("Verisign", "Information Technology"),
    "VRSK": ("Verisk Analytics", "Industrials"),
    "VZ": ("Verizon", "Communication Services"),
    "VRTX": ("Vertex Pharmaceuticals", "Health Care"),
    "VTRS": ("Viatris", "Health Care"),
    "VICI": ("Vici Properties", "Real Estate"),
    "V": ("Visa", "Financials"),
    "VST": ("Vistra", "Utilities"),
    "VMC": ("Vulcan Materials", "Materials"),
    "WRB": ("W.R. Berkley", "Financials"),
    "GWW": ("W.W. Grainger", "Industrials"),
    "WAB": ("Wabtec", "Industrials"),
    "WBA": ("Walgreens", "Consumer Staples"),
    "WMT": ("Walmart", "Consumer Staples"),
    "DIS": ("Walt Disney", "Communication Services"),
    "WBD": ("워너 브라더스 디스커버리", "Communication Services"),
    "WM": ("Waste Management", "Industrials"),
    "WAT": ("Waters Corporation", "Health Care"),
    "WEC": ("WEC Energy", "Utilities"),
    "WFC": ("Wells Fargo", "Financials"),
    "WELL": ("Welltower", "Real Estate"),
    "WST": ("West Pharmaceutical", "Health Care"),
    "WDC": ("Western Digital", "Information Technology"),
    "WY": ("Weyerhaeuser", "Real Estate"),
    "WSM": ("Williams-Sonoma", "Consumer Discretionary"),
    "WMB": ("Williams Companies", "Energy"),
    "WTW": ("윌리스 타워스 왓슨", "Financials"),
    "WDAY": ("워크데이", "Information Technology"),
    "WYNN": ("윈 리조트", "Consumer Discretionary"),
    "XEL": ("엑셀 에너지", "Utilities"),
    "XYL": ("자일럼", "Industrials"),
    "YUM": ("얌! 브랜즈", "Consumer Discretionary"),
    "ZBRA": ("지브라 테크놀로지스", "Information Technology"),
    "ZBH": ("짐머바이오메트", "Health Care"),
    "ZTS": ("조에티스", "Health Care"),
}

KST = timezone(timedelta(hours=9))


def safe_get(info, key, default=None):
    """Safely get a value from yfinance info dict."""
    try:
        v = info.get(key, default)
        if v is None or v == 'Infinity' or v == float('inf') or v == float('-inf'):
            return default
        return v
    except:
        return default


def fetch_stock_data(ticker, name, sector):
    """Fetch all data for a single stock using yfinance."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info or {}

        price = safe_get(info, 'currentPrice') or safe_get(info, 'regularMarketPrice')
        if not price:
            return None

        pe = safe_get(info, 'trailingPE')
        fwd_pe = safe_get(info, 'forwardPE')
        pb = safe_get(info, 'priceToBook')
        ps = safe_get(info, 'priceToSalesTrailing12Months')
        peg = safe_get(info, 'pegRatio')
        ev_ebitda = safe_get(info, 'enterpriseToEbitda')
        div_yield = safe_get(info, 'dividendYield')
        roe = safe_get(info, 'returnOnEquity')
        mkt_cap = safe_get(info, 'marketCap')
        high52 = safe_get(info, 'fiftyTwoWeekHigh')
        low52 = safe_get(info, 'fiftyTwoWeekLow')

        # 52w discount
        discount52w = None
        if high52 and price:
            discount52w = round((price - high52) / high52 * 100, 2)

        # P/E History: get quarterly EPS + historical prices
        pe_history = []
        try:
            # Get 5 years of quarterly prices (last trading day of each quarter)
            hist = stock.history(period="5y", interval="3mo")
            earnings = safe_get(info, 'trailingEps')

            if not hist.empty and pe and pe > 0 and pe < 500:
                # Use current P/E as anchor and estimate historical P/E from price ratios
                current_price = price
                for date, row in hist.iterrows():
                    hist_price = row.get('Close')
                    if hist_price and hist_price > 0:
                        # Estimate historical P/E using price ratio
                        est_pe = pe * (hist_price / current_price)
                        if 0 < est_pe < 500:
                            pe_history.append({
                                "date": date.strftime("%Y-%m"),
                                "pe": round(est_pe, 1)
                            })
        except Exception:
            pass

        # Ensure current P/E is the last entry
        if pe and 0 < pe < 500:
            now_date = datetime.now().strftime("%Y-%m")
            if not pe_history or pe_history[-1]["date"] != now_date:
                pe_history.append({"date": now_date, "pe": round(pe, 1)})

        # Calculate percentile from pe_history
        pe_percentile = None
        if pe and pe_history and len(pe_history) >= 3:
            all_pes = [p["pe"] for p in pe_history if p["pe"] > 0]
            if all_pes:
                below = sum(1 for p in all_pes if p < pe)
                pe_percentile = round(below / len(all_pes) * 100)

        # Historical performance: find similar P/E periods
        hist_perf = None
        if pe and pe_history and len(pe_history) >= 5:
            cases = []
            for i, ph in enumerate(pe_history[:-1]):
                if abs(ph["pe"] - pe) / pe < 0.15:  # within 15%
                    # Estimate 6-month return using subsequent price data
                    if i + 2 < len(pe_history):
                        ret = round((pe_history[i+2]["pe"] / ph["pe"] - 1) * 100 * (pe / pe_history[i+2]["pe"]), 1)
                        # Simplify: use price change proxy
                        try:
                            p_start = ph["pe"]
                            p_end = pe_history[min(i+2, len(pe_history)-1)]["pe"]
                            ret = round((p_end / p_start - 1) * 100, 1)
                        except:
                            ret = 0
                        cases.append({"date": ph["date"], "pe": ph["pe"], "return6m": ret})

            if cases:
                returns = [c["return6m"] for c in cases]
                hist_perf = {
                    "similarCount": len(cases),
                    "avg6mReturn": round(sum(returns) / len(returns), 1),
                    "winRate": round(sum(1 for r in returns if r > 0) / len(returns) * 100),
                    "cases": cases[:6]
                }

        # Value score calculation
        value_score = None
        if pe and pe > 0:
            scores = []
            if pe: scores.append(max(0, min(100, 100 - (pe / 50 * 100))))
            if fwd_pe: scores.append(max(0, min(100, 100 - (fwd_pe / 40 * 100))))
            if pb: scores.append(max(0, min(100, 100 - (pb / 20 * 100))))
            if ps: scores.append(max(0, min(100, 100 - (ps / 15 * 100))))
            if peg and peg > 0: scores.append(max(0, min(100, 100 - (peg / 3 * 100))))
            if discount52w is not None: scores.append(max(0, min(100, 50 + abs(discount52w) * (1 if discount52w < 0 else -1))))
            if scores:
                value_score = round(sum(scores) / len(scores))

        return {
            "ticker": ticker,
            "name": name,
            "sector": sector,
            "price": price,
            "marketCap": mkt_cap,
            "pe": round(pe, 2) if pe else None,
            "forwardPE": round(fwd_pe, 2) if fwd_pe else None,
            "pb": round(pb, 2) if pb else None,
            "ps": round(ps, 2) if ps else None,
            "peg": round(peg, 2) if peg else None,
            "evEbitda": round(ev_ebitda, 2) if ev_ebitda else None,
            "dividendYield": round(div_yield * 100, 2) if div_yield else None,
            "roe": round(roe * 100, 2) if roe else None,
            "high52w": high52,
            "low52w": low52,
            "discount52w": discount52w,
            "valueScore": value_score,
            "pePercentile": pe_percentile,
            "peRank": round(pe / 50 * 100) if pe else None,
            "peHistory": pe_history,
            "histPerformance": hist_perf,
        }
    except Exception as e:
        print(f"  ⚠️ Error fetching {ticker}: {e}")
        return None


def main():
    print("=" * 60)
    print("  S&P 500 Value Screener - Data Fetcher (yfinance)")
    print(f"  Time: {datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')}")
    print("=" * 60)

    tickers = list(SP500.keys())
    total = len(tickers)
    print(f"\n📊 Fetching data for {total} S&P 500 stocks...\n")

    stocks = []
    errors = 0

    for i, ticker in enumerate(tickers):
        name, sector = SP500[ticker]
        pct = (i + 1) / total * 100
        print(f"  [{i+1}/{total}] ({pct:.0f}%) {ticker} - {name}...", end=" ", flush=True)

        result = fetch_stock_data(ticker, name, sector)

        if result:
            stocks.append(result)
            pe_str = f"P/E={result['pe']}" if result['pe'] else "P/E=N/A"
            print(f"✅ {pe_str}")
        else:
            errors += 1
            print("❌ Failed")

        # Small delay to avoid rate limiting
        if (i + 1) % 10 == 0:
            time.sleep(1)

    print(f"\n✅ Fetched {len(stocks)} stocks ({errors} errors)")

    # Calculate sector averages
    sector_data = {}
    for s in stocks:
        sec = s["sector"]
        if sec not in sector_data:
            sector_data[sec] = {"pe_vals": [], "pb_vals": [], "count": 0}
        sector_data[sec]["count"] += 1
        if s["pe"] and 0 < s["pe"] < 500:
            sector_data[sec]["pe_vals"].append(s["pe"])
        if s["pb"] and 0 < s["pb"] < 200:
            sector_data[sec]["pb_vals"].append(s["pb"])

    sectors = {}
    for sec, data in sector_data.items():
        sectors[sec] = {
            "avgPE": round(sum(data["pe_vals"]) / len(data["pe_vals"]), 1) if data["pe_vals"] else None,
            "avgPB": round(sum(data["pb_vals"]) / len(data["pb_vals"]), 1) if data["pb_vals"] else None,
            "count": data["count"]
        }

    # Summary
    valid_pe = [s["pe"] for s in stocks if s["pe"] and 0 < s["pe"] < 500]
    vs = [s["valueScore"] for s in stocks if s["valueScore"] is not None]

    summary = {
        "totalStocks": len(stocks),
        "avgPE": round(sum(valid_pe) / len(valid_pe), 1) if valid_pe else 0,
        "undervalued": sum(1 for s in stocks if s.get("valueScore") and s["valueScore"] >= 65),
        "overvalued": sum(1 for s in stocks if s.get("valueScore") and s["valueScore"] <= 35),
        "fairValue": sum(1 for s in stocks if s.get("valueScore") and 35 < s["valueScore"] < 65),
    }

    # Sort by P/E
    stocks.sort(key=lambda x: x.get("pe") or 9999)

    output = {
        "lastUpdated": datetime.now(KST).strftime("%Y.%m.%d %H:%M KST"),
        "summary": summary,
        "stocks": stocks,
        "sectors": sectors,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/sp500_data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False)

    print(f"\n🎉 Data saved to data/sp500_data.json")
    print(f"  📊 Total stocks: {summary['totalStocks']}")
    print(f"  📈 Avg P/E: {summary['avgPE']}")
    print(f"  🟢 Undervalued: {summary['undervalued']}")
    print(f"  🔴 Overvalued: {summary['overvalued']}")


if __name__ == "__main__":
    main()
