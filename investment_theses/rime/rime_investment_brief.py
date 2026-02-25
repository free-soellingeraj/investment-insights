#!/usr/bin/env python3
"""Generate a 1-page PDF investment brief for RIME (Algorhythm Holdings)."""

from fpdf import FPDF
from fpdf.enums import XPos, YPos

class InvestmentBrief(FPDF):
    def header(self):
        pass

    def footer(self):
        self.set_y(-10)
        self.set_font("Helvetica", "I", 6)
        self.set_text_color(120, 120, 120)
        self.cell(0, 4, "Disclaimer: For informational purposes only. Not financial advice. Data as of Feb 19, 2026.", align="C")

pdf = InvestmentBrief("P", "mm", "Letter")
pdf.set_auto_page_break(auto=False)
pdf.add_page()
W = pdf.w - 20  # usable width (10mm margins each side)
LEFT = 10

# ── Title Bar ──
pdf.set_fill_color(20, 40, 80)
pdf.rect(0, 0, pdf.w, 18, "F")
pdf.set_xy(LEFT, 3)
pdf.set_font("Helvetica", "B", 16)
pdf.set_text_color(255, 255, 255)
pdf.cell(0, 6, "RIME | Algorhythm Holdings, Inc.", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
pdf.set_x(LEFT)
pdf.set_font("Helvetica", "", 8)
pdf.set_text_color(180, 200, 255)
pdf.cell(0, 5, "NASDAQ: RIME  |  AI Freight Logistics  |  Investment Research Brief  |  Feb 19, 2026", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

y = 21

# ── Helper functions ──
def section_header(title, y_pos):
    pdf.set_xy(LEFT, y_pos)
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(20, 40, 80)
    pdf.set_fill_color(230, 237, 250)
    pdf.cell(W, 4.5, f"  {title}", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    return y_pos + 5.5

def kv_row(label, value, x, y_pos, col_w=45, val_w=45):
    pdf.set_xy(x, y_pos)
    pdf.set_font("Helvetica", "B", 6.5)
    pdf.set_text_color(60, 60, 60)
    pdf.cell(col_w, 3.5, label)
    pdf.set_font("Helvetica", "", 6.5)
    pdf.set_text_color(30, 30, 30)
    pdf.cell(val_w, 3.5, str(value))
    return y_pos + 3.8

def body_text(text, y_pos, font_size=6.5, indent=0):
    pdf.set_xy(LEFT + indent, y_pos)
    pdf.set_font("Helvetica", "", font_size)
    pdf.set_text_color(30, 30, 30)
    pdf.multi_cell(W - indent, 3.3, text)
    return pdf.get_y()

def bullet(text, y_pos, indent=3):
    pdf.set_xy(LEFT + indent, y_pos)
    pdf.set_font("Helvetica", "", 6.5)
    pdf.set_text_color(30, 30, 30)
    pdf.cell(3, 3.3, "-")
    pdf.multi_cell(W - indent - 3, 3.3, text)
    return pdf.get_y()

def red_text(label, value, x, y_pos, col_w=45, val_w=45):
    pdf.set_xy(x, y_pos)
    pdf.set_font("Helvetica", "B", 6.5)
    pdf.set_text_color(60, 60, 60)
    pdf.cell(col_w, 3.5, label)
    pdf.set_font("Helvetica", "B", 6.5)
    pdf.set_text_color(180, 30, 30)
    pdf.cell(val_w, 3.5, str(value))
    return y_pos + 3.8

# ── Thesis Box ──
y = section_header("THESIS", y)
y = body_text(
    "Algorhythm Holdings (formerly The Singing Machine Co.) acquired SemiCab, an AI freight logistics platform, in Jul 2024. "
    "After rebranding, executing a 1:200 reverse split, and divesting its karaoke business, the company released a white paper on "
    "Feb 12, 2026 claiming 4x broker productivity and 70% empty-mile reduction. The stock surged 222%+ in one day, while major "
    "trucking stocks (CHRW, RXO, JBHT) lost $17B+ in combined market cap - drawing comparisons to the DeepSeek disruption.",
    y, 6.5
)
y += 1

# ── Two-column: Financials left, Key Metrics right ──
y = section_header("KEY FINANCIALS (TTM / Latest Filing)", y)
col1_x = LEFT
col2_x = LEFT + W / 2 + 2
row_y = y

row_y = kv_row("Market Cap:", "$16.4M", col1_x, row_y)
kv_row("Enterprise Value:", "$21.4M", col2_x, row_y - 3.8)
row_y = kv_row("Revenue (TTM):", "$26.4M", col1_x, row_y)
kv_row("Gross Margin:", "15.6%", col2_x, row_y - 3.8)
row_y = kv_row("Net Income (TTM):", "-$28.7M", col1_x, row_y)
kv_row("Operating Margin:", "-43.0%", col2_x, row_y - 3.8)
row_y = kv_row("Cash:", "$2.84M", col1_x, row_y)
kv_row("Total Debt:", "$6.81M", col2_x, row_y - 3.8)
row_y = kv_row("Shares Outstanding:", "5.76M", col1_x, row_y)
kv_row("Float:", "5.48M", col2_x, row_y - 3.8)
row_y = kv_row("SemiCab ARR (Dec 2025):", "$9.7M (+300% YoY)", col1_x, row_y)
kv_row("Short Interest:", "7.02%", col2_x, row_y - 3.8)
row_y = red_text("Altman Z-Score:", "-9.25 (Bankruptcy Risk)", col1_x, row_y)
red_text("Going Concern:", "Yes - Substantial Doubt", col2_x, row_y - 3.8)
y = row_y + 1

# ── Timeline ──
y = section_header("KEY TIMELINE", y)
events = [
    ("Jul 2024", "Singing Machine acquires SemiCab ($2.25M total funding)"),
    ("Sep 2024", "Rebrands to Algorhythm Holdings; ticker MICS -> RIME"),
    ("Dec 2024", "Closes $9.5M public offering; pauses capital raises"),
    ("Feb 2025", "Executes 1:200 reverse split (Nasdaq compliance)"),
    ("Aug 2025", "Sells Singing Machine karaoke business for $500K"),
    ("Dec 2025", "$6M contract expansion (10x over pilot); F500 SaaS partnership"),
    ("Jan 2026", "ARR hits $9.7M; HUL $1.6M contract expansion"),
    ("Feb 12, 2026", "White paper release -> 222% stock surge; $17B+ trucking selloff"),
]
for date, desc in events:
    pdf.set_xy(LEFT + 2, y)
    pdf.set_font("Helvetica", "B", 6)
    pdf.set_text_color(20, 40, 80)
    pdf.cell(22, 3.3, date)
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(30, 30, 30)
    pdf.cell(W - 24, 3.3, desc)
    y += 3.5
y += 1

# ── Red Flags ──
y = section_header("RED FLAGS & RISKS", y)
y = bullet("Altman Z-Score -9.25; going concern; cash burn -$14.5M/yr FCF; current ratio 0.53", y)
y = bullet("Streeterville Capital $20M financing: SEC-charged toxic lender; 10M+ shares registered for resale at 90% of lowest VWAP", y)
y = bullet("1:200 reverse split (>1:100 splits maintain price <20% of the time historically); insider ownership just 0.01%", y)
y = bullet("White paper was inaccessible for verification when released; only 1 analyst covers the stock", y)
y += 1

# ── Most Similar Stocks ──
y = section_header("BEST COMPARABLE STOCKS", y)
# Table header
pdf.set_xy(LEFT, y)
pdf.set_font("Helvetica", "B", 6)
pdf.set_text_color(255, 255, 255)
pdf.set_fill_color(20, 40, 80)
cols = [("Ticker", 14), ("Company", 30), ("Mkt Cap", 16), ("Similarity to RIME", 56), ("Key Risk", 50)]
for label, w in cols:
    pdf.cell(w, 4, label, fill=True)
y += 4.2

rows = [
    ("OBAI", "Our Bond (f/k/a TG-17)", "$36.5M", "AI disrupting $350B security; F500 clients; rebrand; fresh listing", "Customer concentration; $925K cash"),
    ("DVLT", "Datavault AI (f/k/a WiSA)", "~$44M", "Audio->AI pivot; 1:150 R/S; going concern; $200M rev guide", "Net loss -$86M; Nasdaq compliance"),
    ("FRGT", "Freight Technologies", "$25.2M", "Same industry; AI freight (Zayren); going concern; low float", "CRITICAL: $292K cash; -24% gross margin"),
    ("BNAI", "Brand Engagement Net.", "~$112M", "LLM platform (ELM); healthcare/insurance; 1:10 R/S; going concern", "Rev $99K; -$33.7M loss; current ratio 0.15"),
]
for ticker, name, mcap, sim, risk in rows:
    pdf.set_xy(LEFT, y)
    pdf.set_font("Helvetica", "B", 5.5)
    pdf.set_text_color(20, 40, 80)
    pdf.cell(14, 3.5, ticker)
    pdf.set_font("Helvetica", "", 5.5)
    pdf.set_text_color(30, 30, 30)
    pdf.cell(30, 3.5, name)
    pdf.cell(16, 3.5, mcap)
    pdf.cell(56, 3.5, sim)
    pdf.set_text_color(150, 30, 30)
    pdf.cell(50, 3.5, risk)
    y += 3.7

y += 0.5
y = body_text(
    "Focus: companies using LLMs/AI software to unlock large traditional markets (not hardware, robotics, or data center infra). "
    "OBAI remains the strongest overall match (AI disrupting $350B security). DVLT checks nearly every RIME box: legacy rebrand, "
    "1:150 reverse split, going concern, aggressive $200M guidance. BNAI is the newest find: a proprietary LLM platform (ELM) "
    "targeting healthcare, insurance, and financial services with near-zero revenue and going concern. "
    "FRGT is the closest industry match but $292K cash is critical survival risk.",
    y, 6
)
y += 1.5

# ── Screening Criteria ──
y = section_header("PATTERN SCREENING CRITERIA (Find the Next RIME)", y)
criteria = (
    "LLM/AI SOFTWARE unlocking traditional markets (not hardware/robotics/data centers) | "
    "Mkt Cap <$150M | Reverse split in last 18mo | Business pivot/rename in last 24mo | Going concern warning | "
    "Rev growth >100% YoY | Named F500 customers | AI/LLM platform claims | Accelerating PR cadence | "
    "Float <10M shares | Target industry: large, fragmented, labor-intensive | Check for toxic lender financing"
)
y = body_text(criteria, y, 6)
y += 1.5

# ── Sources / Links ──
y = section_header("SOURCES & LINKS", y)
pdf.set_font("Helvetica", "", 5.5)
pdf.set_text_color(30, 80, 160)
links = [
    ("RIME Financials - StockAnalysis", "https://stockanalysis.com/stocks/rime/statistics/"),
    ("SEC EDGAR - RIME Filings", "https://www.nasdaq.com/market-activity/stocks/rime/sec-filings"),
    ("CNBC: Trucking Stocks Tumble on AI Tool", "https://www.cnbc.com/2026/02/12/trucking-and-logistics-stocks-tumble-on-release-of-ai-freight-scaling-tool.html"),
    ("Sherwood: Karaoke Co. Obliterates Trucking", "https://sherwood.news/markets/a-former-karaoke-machine-company-has-obliterated-billions-of-dollars-in/"),
    ("GlobeNewsWire: SemiCab Acquisition", "https://www.globenewswire.com/news-release/2024/07/05/2908996/0/en/Singing-Machine-Completes-Acquisition-of-Leading-AI-Logistics-Company.html"),
    ("Algorhythm 2025 Year-End Recap", "https://www.globenewswire.com/news-release/2025/12/22/3209309/0/en/Algorhythm-Holdings-Recaps-Transformational-2025-Marked-by-Key-Customer-Wins-Major-Contract-Expansions-and-Strong-Revenue-Growth.html"),
]
link_x = LEFT + 2
for i, (title, url) in enumerate(links):
    col = i % 2
    if col == 0:
        pdf.set_xy(link_x, y)
    else:
        pdf.set_xy(link_x + W / 2, y)
    pdf.set_font("Helvetica", "", 5.5)
    pdf.set_text_color(30, 80, 160)
    pdf.cell(W / 2 - 2, 3, f"{title}", link=url)
    if col == 1:
        y += 3.3

if len(links) % 2 == 1:
    y += 3.3

###############################################################################
# PAGES 2+: Detailed Comparable Company Profiles
###############################################################################

def comp_page(ticker, name, subtitle, overview, pivot_text, timeline_events,
              traction_bullets, financials_left, financials_right, risks, sources):
    """Render a full comparable-company profile page."""
    pdf.add_page()
    # Title bar
    pdf.set_fill_color(20, 40, 80)
    pdf.rect(0, 0, pdf.w, 14, "F")
    pdf.set_xy(LEFT, 2)
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 6, f"{ticker} | {name}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_x(LEFT)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(180, 200, 255)
    pdf.cell(0, 4, subtitle, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    y = 17

    # Business Overview
    y = section_header("BUSINESS OVERVIEW", y)
    y = body_text(overview, y, 6.5)
    y += 1

    # The Pivot
    y = section_header("THE PIVOT", y)
    y = body_text(pivot_text, y, 6.5)
    y += 1

    # Key Timeline (with RIME comparison)
    y = section_header("KEY TIMELINE (vs. RIME)", y)
    # Table header
    pdf.set_xy(LEFT, y)
    pdf.set_font("Helvetica", "B", 5.5)
    pdf.set_text_color(255, 255, 255)
    pdf.set_fill_color(20, 40, 80)
    pdf.cell(20, 3.5, "Date", fill=True)
    pdf.cell(W / 2 - 10, 3.5, f"{ticker} Event", fill=True)
    pdf.cell(W / 2 - 10, 3.5, "RIME Comparison", fill=True)
    y += 4
    for date, event, rime_comp in timeline_events:
        pdf.set_xy(LEFT, y)
        pdf.set_font("Helvetica", "B", 5.5)
        pdf.set_text_color(20, 40, 80)
        pdf.cell(20, 3.2, date)
        pdf.set_font("Helvetica", "", 5.5)
        pdf.set_text_color(30, 30, 30)
        pdf.cell(W / 2 - 10, 3.2, event)
        pdf.set_text_color(100, 100, 100)
        pdf.set_font("Helvetica", "I", 5.5)
        pdf.cell(W / 2 - 10, 3.2, rime_comp)
        y += 3.4
    y += 1

    # Concrete Traction
    y = section_header("CONCRETE TRACTION & MILESTONES", y)
    for b in traction_bullets:
        y = bullet(b, y, 3)
    y += 1

    # Financials - two column
    y = section_header("KEY FINANCIALS", y)
    col1_x = LEFT
    col2_x = LEFT + W / 2 + 2
    row_y = y
    for i, (lbl, val) in enumerate(financials_left):
        row_y_tmp = kv_row(lbl, val, col1_x, row_y + i * 3.8)
    row_y2 = y
    for i, (lbl, val) in enumerate(financials_right):
        row_y2_tmp = kv_row(lbl, val, col2_x, row_y2 + i * 3.8)
    y = max(row_y + len(financials_left) * 3.8,
            row_y2 + len(financials_right) * 3.8) + 1

    # Risks
    y = section_header("KEY RISKS", y)
    for r in risks:
        y = bullet(r, y, 3)
    y += 1

    # Sources
    y = section_header("SOURCES", y)
    for title, url in sources:
        pdf.set_xy(LEFT + 2, y)
        pdf.set_font("Helvetica", "", 5.5)
        pdf.set_text_color(30, 80, 160)
        pdf.cell(W - 4, 3.3, title, link=url)
        y += 3.5


# ═══════════════════════════════════════════════════════════════════════════
# 1. OBAI - Our Bond (Best Overall Match)
# ═══════════════════════════════════════════════════════════════════════════
comp_page(
    ticker="OBAI",
    name="Our Bond, Inc. (f/k/a TG-17, Inc.)",
    subtitle="NASDAQ: OBAI  |  AI-Powered Personal Security  |  Comparable #1 (Best Overall Match)",
    overview=(
        "Bond is an AI-enabled preventative personal security platform that combines an AI mobile app "
        "with 24/7 command centers staffed by trained Personal Security Agents. The platform offers 14 "
        "services including video monitoring, route tracking, on-demand standby, scheduled security checks, "
        "and escalation to first responders. Bond serves corporate clients across 28 countries and recently "
        "listed on Nasdaq via direct listing on Feb 4, 2026. The company changed its name from TG-17 to "
        "Our Bond on Feb 18, 2026. Employs 52 people."
    ),
    pivot_text=(
        "Unlike RIME's legacy-company-acquires-AI-startup model, Bond is a fresh Nasdaq listing positioning "
        "AI as the disruptor of the $350B+ global security services industry. The parallel to RIME is the "
        "narrative: a small company claiming AI can replace human-intensive workflows in a massive, fragmented "
        "industry. Bond claims 100% customer retention and serves Fortune 500 clients. Like RIME's claims of "
        "4x productivity, Bond positions its AI platform as dramatically more scalable than traditional "
        "manned security operations. The company just rebranded (TG-17 -> Our Bond) similar to RIME's rebrand."
    ),
    timeline_events=[
        ("2019", "Bond platform founded (personal security)", "Singing Machine (karaoke) public"),
        ("2024", "35% revenue growth; expands to 28 countries", "RIME acquires SemiCab; rebrands"),
        ("2025", "Completes 1-yr paid pilot with mega-employer", "RIME: 1:200 R/S; sells karaoke biz"),
        ("Sep 2025", "$9.8M TTM revenue; unprofitable", "RIME: ARR $2.5M -> $9.7M trajectory"),
        ("Feb 4, 2026", "Direct listing on Nasdaq under OBAI", "RIME already trading on Nasdaq"),
        ("Feb 13, 2026", "Announces mega-employer rollout discussions", "RIME: white paper +222% (Feb 12)"),
        ("Feb 18, 2026", "Renames TG-17 -> Our Bond, Inc.", "RIME: rebranded Sep 2024"),
    ],
    traction_bullets=[
        "Completed 1-year paid pilot with one of world's largest employers (~1M US employees)",
        "Discussions underway for broader workforce rollout potentially generating $10M+ annual revenue",
        "100% customer retention rate across existing enterprise deployments",
        "Operations in 28 countries with corporate clients",
        "Revenue: $9.8M TTM with 35% sales growth in 2024",
        "Direct listing on Nasdaq Feb 4, 2026 (very recent - limited trading history)",
        "Active discussions with additional Fortune 500 companies following pilot success",
        "WARNING: One customer = 51.9% of 9-month 2025 revenue (extreme concentration)",
    ],
    financials_left=[
        ("Market Cap:", "$36.5M"),
        ("Enterprise Value:", "$48.4M"),
        ("Revenue (TTM):", "$9.8M"),
        ("Net Income:", "-$10.9M"),
        ("Cash:", "$925K"),
        ("Total Debt:", "$8.1M"),
        ("Altman Z-Score:", "-75.09"),
    ],
    financials_right=[
        ("Shares Outstanding:", "13.9M"),
        ("Float:", "13.7M"),
        ("Insider Ownership:", "1.52%"),
        ("Institutional:", "18.06%"),
        ("Gross Margin:", "6.1%"),
        ("Op. Margin:", "-97.7%"),
        ("Current Ratio:", "0.44"),
    ],
    risks=[
        "Altman Z-Score -75.09 - extreme financial distress; negative book value (-$10.4M)",
        "Extreme customer concentration: one client = 52-64% of revenue",
        "Gross margin only 6.1% - very little room for profitability even at scale",
        "Only $925K cash with -$7.6M operating cash flow; needs capital urgently",
        "Newly listed (Feb 4, 2026) - no established trading history or analyst coverage",
        "Working capital -$3.5M; breakeven target is late 2026 at earliest",
    ],
    sources=[
        ("Bond Largest Deployment (GlobeNewsWire)", "https://www.globenewswire.com/news-release/2026/02/13/3238087/0/en/Bond-Advances-Toward-Its-Largest-Enterprise-Wide-Deployment-to-Date-Following-Successful-Paid-Pilot-with-One-of-the-World-s-Largest-Employers.html"),
        ("Bond Nasdaq Debut (Nasdaq)", "https://www.nasdaq.com/press-release/bond-debuts-nasdaq-ushering-new-era-ai-powered-preventative-personal-security-2026-02"),
        ("OBAI Analysis (Seeking Alpha)", "https://seekingalpha.com/article/4867448-our-bond-ai-driven-personal-security-solution"),
        ("OBAI Financials - StockAnalysis", "https://stockanalysis.com/stocks/obai/"),
    ],
)

# ═══════════════════════════════════════════════════════════════════════════
# 2. FRGT - Freight Technologies
# ═══════════════════════════════════════════════════════════════════════════
comp_page(
    ticker="FRGT",
    name="Freight Technologies, Inc.",
    subtitle="NASDAQ: FRGT  |  AI Cross-Border Freight  |  Comparable #2 (Closest Industry Match - Cash Position Concerning)",
    overview=(
        "Freight Technologies operates Fr8App, a digital freight-matching platform focused on the "
        "US-Mexico cross-border corridor and domestic US/Mexico lanes. The company connects shippers "
        "with carriers through technology, aiming to disrupt the $20B+ US-Mexico freight market. "
        "Fr8Tech employs 82 people and has built integrations via its Fleet Rocket TMS."
    ),
    pivot_text=(
        "In November 2025, Fr8Tech launched Zayren, an AI/ML freight-rate prediction and carrier-matching "
        "platform - pivoting from a traditional digital brokerage to an AI-first model. In January 2026, "
        "it released Zayren Pro with agentic AI capabilities, unlimited query capacity, and a proprietary "
        "carrier portal. The platform targets the same 'AI replaces human freight brokers' narrative as RIME's "
        "SemiCab. Fr8Tech offers carriers a 90-day free trial to join the Zayren network, mirroring a "
        "land-and-expand SaaS model. Like RIME, the stock surged 74% on the AI platform announcement."
    ),
    timeline_events=[
        ("2020", "Fr8App digital freight brokerage founded", "Singing Machine (karaoke) already public"),
        ("2022", "Fr8Tech IPOs on Nasdaq via SPAC", "RIME still operating as MICS karaoke"),
        ("2024", "Cross-border freight platform operational", "RIME acquires SemiCab; rebrands (Sep)"),
        ("Nov 2025", "Zayren AI platform launched; stock +74%", "RIME: $6M contract expansion (Dec)"),
        ("Dec 2025", "Q3 results: guides $12-14M rev for 2025", "RIME: ARR hits $9.7M"),
        ("Jan 2026", "Zayren Pro (agentic AI) launched", "RIME: HUL $1.6M contract expansion"),
        ("Feb 2026", "Stock down -95% from 52-wk high", "RIME: white paper -> +222% surge"),
    ],
    traction_bullets=[
        "Zayren AI platform launched Nov 19, 2025; stock surged 74.2% on announcement day",
        "Zayren Pro (premium tier) launched Jan 22, 2026 with agentic AI agents and carrier self-onboarding portal",
        "Fleet Rocket TMS users get exclusive early access to Zayren; carriers offered 90-day free trial",
        "2025 revenue guidance: $12M-$14M; operating loss guidance: $5.5M-$6.5M",
        "Company closed additional enterprise-level subscriptions during Q3 2025",
        "Cross-border tailwind: elevated US tariffs on Mexico trade boosting higher-margin brokerage mix",
        "Voice-enabled AI logistics agents planned for 2026 deployment",
    ],
    financials_left=[
        ("Market Cap:", "$25.2M"),
        ("Enterprise Value:", "$28.5M"),
        ("Revenue (LTM):", "$13.3M"),
        ("Net Income:", "-$5.3M"),
        ("Cash:", "$292K"),
        ("Total Debt:", "$3.3M"),
        ("Altman Z-Score:", "N/A"),
    ],
    financials_right=[
        ("Shares Outstanding:", "21.6M"),
        ("Float:", "1.45M (very low)"),
        ("Insider Ownership:", "0.00%"),
        ("Institutional:", "0.03%"),
        ("Short Interest:", "8.00% of float"),
        ("Gross Margin:", "-24.4%"),
        ("52-Wk Change:", "-95.4%"),
    ],
    risks=[
        "Going concern: only $292K cash; negative working capital; burning $5.5M/yr",
        "Negative gross margin (-24.4%) means losing money on every dollar of revenue before overhead",
        "Shares outstanding grew 1,454% YoY - extreme dilution",
        "Zero insider ownership (0.00%) and near-zero institutional ownership (0.03%)",
        "52-week stock decline of -95.4% despite AI narrative; Zayren is pre-revenue",
    ],
    sources=[
        ("Zayren Launch Announcement (GlobeNewsWire)", "https://www.globenewswire.com/news-release/2025/11/19/3190999/0/en/Freight-Technologies-Announces-the-Commercial-Launch-of-Zayren-AI-Powered-Freight-Rate-Prediction-Carrier-Matching-Platform.html"),
        ("Zayren Pro Launch (GlobeNewsWire)", "https://www.globenewswire.com/news-release/2026/01/22/3223833/0/en/Freight-Technologies-Launches-Zayren-Pro-with-Next-Generation-AI-Agents.html"),
        ("Q3 2025 Results (GlobeNewsWire)", "https://www.globenewswire.com/news-release/2025/12/11/3204025/0/en/Freight-Technologies-Announces-Third-Quarter-2025-Results.html"),
        ("FRGT Financials - StockAnalysis", "https://stockanalysis.com/stocks/frgt/statistics/"),
    ],
)


# ═══════════════════════════════════════════════════════════════════════════
# 3. DVLT - Datavault AI (formerly WiSA Technologies)
# ═══════════════════════════════════════════════════════════════════════════
comp_page(
    ticker="DVLT",
    name="Datavault AI Inc. (f/k/a WiSA Technologies)",
    subtitle="NASDAQ: DVLT  |  AI Data Sciences & Tokenization  |  Comparable #3 (Strongest New Find)",
    overview=(
        "Datavault AI (formerly WiSA Technologies, a wireless audio company) is a data sciences technology "
        "company that owns and operates data management platforms with supercomputing capabilities. The company "
        "offers data technology and software solutions for data ownership, privacy, and security through HPC "
        "infrastructure and proprietary software. It provides tech-licensing fees and tokenization/monetization "
        "services. Rebranded from WiSA to Datavault AI on Feb 14, 2025."
    ),
    pivot_text=(
        "WiSA Technologies made wireless audio technology for home theater systems. The company executed a "
        "1:150 reverse stock split in Apr 2024 to maintain Nasdaq compliance, then completely pivoted to AI "
        "data sciences, tokenization, and blockchain data monetization. This is one of the most dramatic pivots "
        "in the dataset - from consumer audio hardware to AI/blockchain data services. Like RIME, the company "
        "claims extraordinarily aggressive revenue growth: FY25 guidance of $38-40M (1,300% YoY) and FY26 "
        "guidance of $200M (400%+ YoY). The going concern warning, Nasdaq compliance history, and massive "
        "reverse split mirror RIME's pattern almost exactly."
    ),
    timeline_events=[
        ("Pre-2024", "WiSA Technologies: wireless audio standard", "Singing Machine: karaoke hardware"),
        ("Apr 2024", "1:150 reverse split for Nasdaq compliance", "RIME: 1:200 R/S in Feb 2025"),
        ("Late 2024", "Begins AI/data pivot; acquires HPC assets", "RIME acquires SemiCab (Jul 2024)"),
        ("Feb 2025", "Renames to Datavault AI; ticker DVLT", "RIME: rebranded Sep 2024"),
        ("Oct 2025", "S-3 shelf: 5M shares for resale", "RIME: Streeterville 10M+ share resale"),
        ("Feb 2026", "Raises FY25 guide to $38-40M (+1,300% YoY)", "RIME: white paper -> +222% surge"),
        ("FY26 Target", "Guides $200M revenue (400%+ YoY growth)", "RIME: ARR $9.7M, no such guide"),
    ],
    traction_bullets=[
        "FY25 preliminary revenue raised to $38-40M from $30M prior estimate (+30% raise, +1,300% YoY growth)",
        "FY26 revenue reaffirmed at $200M target (~400%+ YoY growth from FY25)",
        "Revenue driven by tech-licensing fees and tokenization/monetization services",
        "One analyst: Strong Buy rating with $4.00 price target (vs. current $0.75)",
        "Company claims data technology platform with supercomputing capabilities",
        "WARNING: Revenue claims are extraordinary and difficult to verify from public filings",
    ],
    financials_left=[
        ("Market Cap:", "~$44M"),
        ("Stock Price:", "$0.75"),
        ("Revenue (TTM):", "$6.17M"),
        ("Net Income:", "-$86.01M"),
        ("Cash:", "N/A (see risk)"),
        ("Total Debt:", "High (see interest exp)"),
        ("Interest Expense:", "$18.2M TTM"),
    ],
    financials_right=[
        ("Shares Outstanding:", "58.7M"),
        ("EPS (Diluted):", "-$1.60"),
        ("Gross Margin:", "6.8%"),
        ("Op. Expenses:", "$43.4M"),
        ("FCF/Share:", "-$0.47"),
        ("FCF:", "-$27.5M"),
        ("Rev Growth:", "+181% TTM"),
    ],
    risks=[
        "Going concern warning flagged in prospectus filings",
        "History of Nasdaq compliance issues; reverse split 1:150 in Apr 2024",
        "Net loss of -$86M on just $6.17M TTM revenue; operating expenses 7x revenue",
        "FY26 revenue guidance of $200M appears extremely aggressive vs. $6.17M TTM run rate",
        "Interest expense of $18.2M suggests heavy debt burden; negative FCF of -$27.5M",
        "Gross margin only 6.8% - very thin even if revenue materializes",
        "S-3 shelf registering 5M shares for resale by selling stockholders",
    ],
    sources=[
        ("DVLT Financials - StockAnalysis", "https://stockanalysis.com/stocks/dvlt/"),
        ("WiSA -> Datavault AI Rebrand (Nasdaq)", "https://www.nasdaq.com/press-release/wisa-technologies-now-datavault-ai-inc-2025-02-13"),
        ("Reverse Split Announcement (IR)", "https://ir.datavaultsite.com/news-events/press-releases/detail/271/wisa-technologies-announces-reverse-stock-split"),
        ("FY25 Revenue Update (StockTitan)", "https://www.stocktitan.net/news/DVLT/datavault-ai-updates-revenue-estimates-by-approximately-30-at-38m-to-0z8gpttbdllf.html"),
    ],
)

# ═══════════════════════════════════════════════════════════════════════════
# 4. BNAI - Brand Engagement Network (Conversational AI / LLM)
# ═══════════════════════════════════════════════════════════════════════════
comp_page(
    ticker="BNAI",
    name="Brand Engagement Network Inc.",
    subtitle="NASDAQ: BNAI  |  Conversational AI / LLM Platform  |  Comparable #4 (LLM Play for Regulated Industries)",
    overview=(
        "Brand Engagement Network (BEN) develops conversational AI agents built for regulated and customer-centric "
        "industries. Its proprietary Engagement Language Model (ELM) with retrieval-augmented generation enables "
        "enterprises to deploy multimodal, compliance-first AI across chat, voice, avatar, and digital channels. "
        "The platform targets life sciences, healthcare, insurance, financial services, hospitality, retail, and "
        "automotive verticals. BEN executed a 1:10 reverse stock split in Dec 2025 for Nasdaq compliance."
    ),
    pivot_text=(
        "BNAI represents the purest LLM-disrupting-traditional-industries play in this comparable set. Like RIME "
        "claims its AI platform can replace human freight brokers at 4x productivity, BEN claims its ELM platform "
        "can replace human engagement across healthcare, insurance, and financial services - all massive, regulated, "
        "labor-intensive industries. The company has a proprietary language model (ELM), not just a wrapper on GPT. "
        "It has near-zero revenue ($99K in FY2024) but has signed a top-10 global pharmaceutical client and is "
        "launching a sovereign healthcare AI platform in Mexico (Skye Salud). The going concern warning, Nasdaq "
        "compliance via reverse split, and pre-revenue status with bold enterprise claims all mirror the RIME pattern."
    ),
    timeline_events=[
        ("2022", "BEN founded; begins building AI platform", "Singing Machine: karaoke hardware"),
        ("2024", "FY revenue: $99K; loss -$33.7M", "RIME acquires SemiCab; rebrands (Sep)"),
        ("Q4 2025", "Signs top-10 pharma client ($250K dev rev)", "RIME: $6M contract expansion"),
        ("Dec 2025", "1:10 reverse split for Nasdaq compliance", "RIME: 1:200 R/S (Feb 2025)"),
        ("Q1 2026", "Skye Salud healthcare AI pilots in Mexico", "RIME: HUL $1.6M expansion"),
        ("Jan 2026", "Warrant exercises raise $1.46M; debt converted", "RIME: ARR $9.7M"),
        ("Feb 2026", "Market cap ~$112M; stock highly volatile", "RIME: white paper -> +222% surge"),
    ],
    traction_bullets=[
        "Proprietary Engagement Language Model (ELM) with RAG - not a GPT wrapper but a purpose-built LLM",
        "Multimodal deployment: chat, voice, avatar, and digital channels for compliance-first verticals",
        "Signed vendor agreement with top-10 global pharmaceutical company ($250K development revenue)",
        "Skye Salud JV: sovereign AI healthcare platform launching in Mexico (Q1 2026 pilots)",
        "Targets $350B+ combined TAM across healthcare, insurance, financial services, pharma",
        "Debt-to-equity conversions and warrant exercises improving balance sheet ($1.46M raised Jan 2026)",
        "WARNING: FY2024 revenue was only $99K - essentially pre-revenue despite bold enterprise claims",
        "WARNING: Going concern warning; current ratio 0.15; massive cash burn",
    ],
    financials_left=[
        ("Market Cap:", "~$112M"),
        ("Stock Price:", "~$10 (post R/S)"),
        ("Revenue (FY2024):", "$99K (+183% YoY)"),
        ("Net Loss (FY2024):", "-$33.7M"),
        ("P/E Ratio:", "-3.60"),
        ("Current Ratio:", "0.15"),
    ],
    financials_right=[
        ("52-Wk High:", "$86.28"),
        ("52-Wk Low:", "$1.18"),
        ("Reverse Split:", "1:10 (Dec 2025)"),
        ("Nasdaq Status:", "Compliance via R/S"),
        ("Going Concern:", "Yes"),
        ("Key Model:", "ELM (proprietary LLM)"),
    ],
    risks=[
        "Going concern warning with current ratio of 0.15 - extreme financial distress",
        "FY2024 revenue of $99K is virtually zero; net loss 340x revenue",
        "Market cap of ~$112M on $99K revenue implies extreme speculation premium",
        "1:10 reverse split (Dec 2025) for Nasdaq compliance - familiar pattern with poor historical outcomes",
        "Pharma client contract ($250K) is tiny relative to company valuation",
        "Skye Salud Mexico platform is in pilot stage - unproven market, regulatory uncertainty",
        "52-week range of $1.18 to $86.28 indicates extreme volatility",
    ],
    sources=[
        ("BNAI Financials - StockAnalysis", "https://stockanalysis.com/stocks/bnai/"),
        ("Reverse Split Announcement (PRNewswire)", "https://www.prnewswire.com/news-releases/brand-engagement-network-announces-a-1-for-10-reverse-stock-split-302630928.html"),
        ("Warrant Exercises / Debt Conversion (StockTitan)", "https://www.stocktitan.net/news/BNAI/brand-engagement-network-nasdaq-bnai-reports-1-46-million-in-cash-3qvmatewb6tn.html"),
        ("BNAI News & SEC Filings (StockTitan)", "https://www.stocktitan.net/news/BNAI/"),
    ],
)

# ── Output ──
outpath = "/Users/free-soellingeraj/code/.para-llm-directory/envs/investment-insights-rime-related-alerts/investment-insights/RIME_Investment_Brief.pdf"
pdf.output(outpath)
print(f"PDF saved to: {outpath}")
