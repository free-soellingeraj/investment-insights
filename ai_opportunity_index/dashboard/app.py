"""Streamlit dashboard for the AI Opportunity Index."""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure the project root is on sys.path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ai_opportunity_index.config import PROCESSED_DIR, PROJECT_ROOT, QUADRANT_LABELS, REALIZATION_WEIGHTS
from ai_opportunity_index.storage.db import get_company_detail, get_full_index, get_subscriber_by_token, init_db


def _fmt_large_number(value):
    """Format a large number for display (e.g. 2.95T, 245B, 12.3M)."""
    if value is None or pd.isna(value):
        return "N/A"
    abs_val = abs(value)
    if abs_val >= 1e12:
        return f"${value / 1e12:.2f}T"
    if abs_val >= 1e9:
        return f"${value / 1e9:.1f}B"
    if abs_val >= 1e6:
        return f"${value / 1e6:.1f}M"
    return f"${value:,.0f}"


def _fmt_employees(value):
    if value is None or pd.isna(value):
        return "N/A"
    if value >= 1000:
        return f"{value / 1000:.0f}K"
    return str(int(value))


def _fmt_score(value, default="N/A"):
    if value is None:
        return default
    return f"{value:.2f}"


def render_detail_page(ticker: str):
    """Render the company detail page with full score explanations."""
    from ai_opportunity_index.scoring.explainer import explain_company

    explanation = explain_company(ticker)
    if not explanation:
        st.warning(f"Company **{ticker}** not found in the index.")
        if st.button("Back to Index"):
            st.query_params.clear()
            st.rerun()
        return

    company = explanation["company"]
    opp_exp = explanation["opportunity_explanation"]
    real_exp = explanation["realization_explanations"]
    flags = explanation["flags"]

    # --- Back button ---
    if st.button("< Back to Index"):
        st.query_params.clear()
        st.rerun()

    # --- Company Header ---
    st.markdown(f"# {company['ticker']} — {company.get('company_name', 'Unknown')}")
    header_parts = []
    if company.get("sector"):
        header_parts.append(company["sector"])
    if company.get("industry"):
        header_parts.append(company["industry"])
    if company.get("exchange"):
        header_parts.append(company["exchange"])
    if header_parts:
        st.markdown(" | ".join(header_parts))

    col1, col2, col3 = st.columns(3)
    col1.metric("Market Cap", _fmt_large_number(company.get("market_cap")))
    col2.metric("Revenue", _fmt_large_number(company.get("revenue")))
    col3.metric("Employees", _fmt_employees(company.get("employees")))

    st.divider()

    # --- Index Position ---
    st.subheader("Index Position")
    idx = company.get("index", {})
    opp_score = idx.get("opportunity")
    real_score = idx.get("realization")
    quadrant_label = idx.get("label", "N/A")

    pos_col1, pos_col2 = st.columns([1, 2])

    with pos_col1:
        st.metric("Quadrant", quadrant_label)
        st.metric("Opportunity Score", _fmt_score(opp_score))
        st.metric("Realization Score", _fmt_score(real_score))

    with pos_col2:
        if opp_score is not None and real_score is not None:
            # Mini scatter plot with this company highlighted
            df_all = get_full_index()
            if not df_all.empty:
                fig = go.Figure()
                # All other companies
                fig.add_trace(go.Scatter(
                    x=df_all["opportunity"],
                    y=df_all["realization"],
                    mode="markers",
                    marker=dict(size=4, color="lightgray", opacity=0.4),
                    name="All companies",
                    hoverinfo="skip",
                ))
                # This company
                fig.add_trace(go.Scatter(
                    x=[opp_score],
                    y=[real_score],
                    mode="markers+text",
                    marker=dict(size=14, color="red", symbol="star"),
                    text=[ticker],
                    textposition="top center",
                    name=ticker,
                ))
                # Quadrant lines
                opp_median = df_all["opportunity"].median()
                real_median = df_all["realization"].median()
                fig.add_hline(y=real_median, line_dash="dash", line_color="gray", opacity=0.3)
                fig.add_vline(x=opp_median, line_dash="dash", line_color="gray", opacity=0.3)
                fig.update_layout(
                    xaxis=dict(range=[0, 1], title="AI Opportunity"),
                    yaxis=dict(range=[0, 1], title="AI Realization"),
                    height=300,
                    margin=dict(l=40, r=20, t=20, b=40),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # --- AI Opportunity Score ---
    composite_opp = opp_exp.get("composite", {})
    st.subheader(f"AI Opportunity Score ({_fmt_score(composite_opp.get('composite_score'))})")

    # Revenue Opportunity
    rev = opp_exp.get("revenue", {})
    rev_score = rev.get("normalized_score", "N/A")
    with st.expander(f"Revenue Opportunity: {_fmt_score(rev_score)}", expanded=True):
        method = rev.get("method", "unknown")
        if rev.get("sector"):
            st.markdown(f"**Sector:** {rev['sector']}" + (f" ({rev.get('industry', '')})" if rev.get("industry") else ""))

        if rev.get("is_b2b"):
            st.markdown("**Business model:** B2B (sells to other businesses)")
        elif rev.get("is_b2b") is False:
            st.markdown("**Business model:** B2C / Non-B2B")

        if rev.get("customer_industries"):
            st.markdown("**Customer industries and their workforce AI applicability:**")
            for ci in rev["customer_industries"]:
                st.markdown(f"- **{ci['naics_name']}** (NAICS {ci['naics']}) — avg applicability: {ci['avg_ai_applicability']:.4f}")
                for sg in ci.get("soc_groups", []):
                    st.markdown(f"  - {sg['code']} {sg['name']}: {sg['ai_applicability']:.4f}")

            if rev.get("avg_customer_ai_applicability"):
                st.markdown(f"**Average customer AI applicability:** {rev['avg_customer_ai_applicability']:.4f}")

            if rev.get("b2b_boost_applied"):
                st.markdown(f"**B2B boost applied:** {rev['b2b_multiplier']}x")

        elif rev.get("own_workforce_soc_groups"):
            st.markdown("**Own workforce AI applicability (used for revenue estimate):**")
            for sg in rev["own_workforce_soc_groups"]:
                st.markdown(f"- {sg['code']} {sg['name']}: {sg['ai_applicability']:.4f}")

        elif rev.get("workforce_soc_groups"):
            st.markdown("**Sector workforce groups:**")
            for sg in rev["workforce_soc_groups"]:
                st.markdown(f"- {sg['code']} {sg['name']}: {sg['ai_applicability']:.4f}")

        if rev.get("note"):
            st.info(rev["note"])

        st.markdown(f"**Normalized score:** {_fmt_score(rev_score)}")

    # Cost Opportunity
    cost = opp_exp.get("cost", {})
    cost_score = cost.get("normalized_score", "N/A")
    with st.expander(f"Cost Opportunity: {_fmt_score(cost_score)}", expanded=True):
        if cost.get("workforce_soc_groups"):
            st.markdown("**Workforce occupations:**")
            for sg in cost["workforce_soc_groups"]:
                st.markdown(f"- {sg['code']} {sg['name']}: {sg['ai_applicability']:.4f}")

        if cost.get("avg_ai_applicability"):
            st.markdown(f"**Average AI applicability:** {cost['avg_ai_applicability']:.4f}")

        if cost.get("employees") and cost.get("employee_scaling_factor"):
            st.markdown(
                f"**Employee scaling:** {cost['employees']:,} employees "
                f"-> {cost['employee_scaling_factor']:.2f}x factor"
            )

        if cost.get("note"):
            st.info(cost["note"])

        st.markdown(f"**Normalized score:** {_fmt_score(cost_score)}")

    # Composite
    st.markdown(
        f"**Composite:** {composite_opp.get('revenue_weight', 0.5):.0%} x "
        f"{_fmt_score(composite_opp.get('revenue_score'))} + "
        f"{composite_opp.get('cost_weight', 0.5):.0%} x "
        f"{_fmt_score(composite_opp.get('cost_score'))} = "
        f"**{_fmt_score(composite_opp.get('composite_score'))}**"
    )

    # Sources
    if opp_exp.get("sources"):
        st.markdown("**Sources:**")
        for src in opp_exp["sources"]:
            st.markdown(f"- {src['name']}: {src['detail']}")

    st.divider()

    # --- AI Realization Score ---
    real_composite = real_exp.get("composite", {})
    st.subheader(f"AI Realization Score ({_fmt_score(real_composite.get('composite'))})")

    # Filing NLP
    filing = real_exp.get("filing_nlp", {})
    filing_label = _fmt_score(filing.get("score")) if filing.get("available") else "N/A"
    filing_weight = REALIZATION_WEIGHTS.get("filing_nlp", 0.35)
    with st.expander(f"Filing NLP: {filing_label} (weight: {filing_weight:.0%})", expanded=True):
        if filing.get("available"):
            st.markdown(f"**Based on:** {filing.get('filing_file', 'unknown filing')}")

            keywords = filing.get("keywords_found", [])
            if keywords:
                st.markdown(f"**AI keywords found ({len(keywords)}):**")
                for kw in keywords[:15]:
                    st.markdown(
                        f'- "{kw["keyword"]}" x{kw["count"]} '
                        f'(weight: {kw["weight"]}, contribution: {kw["contribution"]})'
                    )
                if len(keywords) > 15:
                    st.markdown(f"  ... and {len(keywords) - 15} more")

            st.markdown(
                f"**Total weighted count:** {filing.get('total_weighted_count', 0)} "
                f"/ {filing.get('normalization_divisor', 100)} = **{filing_label}**"
            )

            if filing.get("source"):
                st.markdown(f"**Source:** {filing['source']['name']} — {filing['source']['detail']}")
        else:
            st.info(filing.get("reason", "Filing NLP not available"))

    # Product Analysis
    product = real_exp.get("product_analysis", {})
    product_label = _fmt_score(product.get("score")) if product.get("available") else "N/A"
    product_weight = REALIZATION_WEIGHTS.get("product_analysis", 0.25)
    with st.expander(f"Product Analysis: {product_label} (weight: {product_weight:.0%})"):
        if product.get("available"):
            st.markdown(f"**AI products found:** {product.get('ai_products_found', 0)}")
            st.markdown(f"**Partnerships:** {product.get('partnerships', 0)}")
            st.markdown(f"**Shipped products:** {product.get('shipped_products', 0)}")

            evidence_list = product.get("evidence", [])
            if evidence_list:
                st.markdown("**Evidence:**")
                for ev in evidence_list[:10]:
                    st.markdown(f"- [{ev.get('type', 'signal')}] {ev.get('title', 'N/A')}")

            if product.get("source"):
                st.markdown(f"**Source:** {product['source']['name']} — {product['source']['detail']}")
        else:
            st.info(product.get("reason", "Product analysis not yet scored"))

    # Realization Composite
    if real_composite.get("scores"):
        parts = []
        for key, score in real_composite["scores"].items():
            weight = real_composite["weights"].get(key, 0)
            parts.append(f"{_fmt_score(score)} x {weight:.0%}")
        total_w = real_composite.get("total_weight", 1.0)
        st.markdown(
            f"**Composite:** ({' + '.join(parts)}) / {total_w:.0%} = "
            f"**{_fmt_score(real_composite.get('composite'))}**"
        )

    st.divider()

    # --- Flags ---
    st.subheader("Flags")
    if flags:
        for flag in flags:
            st.warning(flag)
    else:
        st.success("No discrepancy flags detected.")


def main():
    st.set_page_config(
        page_title="AI Opportunity Index",
        page_icon="🤖",
        layout="wide",
    )

    init_db()

    # --- Auth Gate ---
    token = st.query_params.get("token")
    if not token:
        st.warning("You need a valid subscription to access the dashboard.")
        st.markdown("[Subscribe to AI Opportunity Index](/#subscribe)")
        st.stop()

    subscriber = get_subscriber_by_token(token)
    if not subscriber or subscriber.status != "active":
        st.error("Your access token is invalid or your subscription has expired.")
        st.markdown("[Subscribe to AI Opportunity Index](/#subscribe)")
        st.stop()

    # --- Sidebar: Account ---
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"Signed in as **{subscriber.email}**")
    st.sidebar.markdown(f"[Manage Subscription](/api/customer-portal?token={token})")

    # --- Page Routing ---
    params = st.query_params
    ticker_param = params.get("ticker")

    if ticker_param:
        render_detail_page(ticker_param.upper())
        return

    # --- Main Index View ---
    st.title("AI Opportunity Index")
    st.markdown("Mapping every publicly traded company on AI opportunity vs. realization")

    tab_index, tab_memos = st.tabs(["Index", "Investment Memos"])

    # ══ Investment Memos Tab ══════════════════════════════════════════
    with tab_memos:
        st.subheader("Investment Memos")
        st.markdown("Download research briefs and investment memos produced by the AI Opportunity Index team.")

        rime_pdf_path = PROJECT_ROOT / "RIME_Investment_Brief.pdf"
        if rime_pdf_path.exists():
            st.markdown("---")
            st.markdown("### RIME Investment Brief")
            st.markdown(
                "An in-depth analysis of RIME (Agilent Technologies' AI-driven "
                "quality management platform) — covering market opportunity, competitive "
                "positioning, and investment thesis."
            )
            with open(rime_pdf_path, "rb") as f:
                st.download_button(
                    label="Download RIME Investment Brief (PDF)",
                    data=f,
                    file_name="RIME_Investment_Brief.pdf",
                    mime="application/pdf",
                )
        else:
            st.info("No investment memos available yet.")

    # ══ Index Tab ═════════════════════════════════════════════════════
    with tab_index:
        # Load data
        df = get_full_index()

        if df.empty:
            st.warning(
                "No index data found. Run the scoring pipeline first:\n\n"
                "```bash\n"
                "python scripts/build_universe.py\n"
                "python scripts/fetch_data.py\n"
                "python scripts/score_companies.py\n"
                "```"
            )
            return

        # ── Sidebar Filters ────────────────────────────────────────────────
        st.sidebar.header("Filters")

        sectors = ["All"] + sorted(df["sector"].dropna().unique().tolist())
        selected_sector = st.sidebar.selectbox("Sector", sectors)
        if selected_sector != "All":
            df = df[df["sector"] == selected_sector]

        exchanges = ["All"] + sorted(df["exchange"].dropna().unique().tolist())
        selected_exchange = st.sidebar.selectbox("Exchange", exchanges)
        if selected_exchange != "All":
            df = df[df["exchange"] == selected_exchange]

        # Market cap filter
        if "market_cap" in df.columns and df["market_cap"].notna().any():
            cap_min, cap_max = float(df["market_cap"].min()), float(df["market_cap"].max())
            if cap_min < cap_max:
                cap_range = st.sidebar.slider(
                    "Market Cap Range ($B)",
                    min_value=cap_min / 1e9,
                    max_value=cap_max / 1e9,
                    value=(cap_min / 1e9, cap_max / 1e9),
                    format="%.1f",
                )
                df = df[
                    (df["market_cap"] >= cap_range[0] * 1e9)
                    & (df["market_cap"] <= cap_range[1] * 1e9)
                ]

        # Score thresholds
        opp_min = st.sidebar.slider("Min Opportunity Score", 0.0, 1.0, 0.0, 0.05)
        real_min = st.sidebar.slider("Min Realization Score", 0.0, 1.0, 0.0, 0.05)
        df = df[(df["opportunity"] >= opp_min) & (df["realization"] >= real_min)]

        # ── Summary Metrics ────────────────────────────────────────────────
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Companies", len(df))
        col2.metric("Avg Opportunity", f"{df['opportunity'].mean():.2f}")
        col3.metric("Avg Realization", f"{df['realization'].mean():.2f}")

        quadrant_counts = df["quadrant_label"].value_counts()
        top_quadrant = quadrant_counts.index[0] if len(quadrant_counts) > 0 else "N/A"
        col4.metric("Largest Quadrant", top_quadrant)

        # ── Main Scatter Plot ──────────────────────────────────────────────
        st.subheader("AI Opportunity vs. Realization")

        # Prepare hover data
        hover_cols = ["ticker", "company_name", "sector", "quadrant_label"]
        available_hover = [c for c in hover_cols if c in df.columns]

        size_col = None
        if "market_cap" in df.columns and df["market_cap"].notna().any():
            df["market_cap_display"] = df["market_cap"].fillna(0) / 1e9
            size_col = "market_cap_display"

        color_col = "sector" if "sector" in df.columns and df["sector"].notna().any() else "quadrant_label"

        fig = px.scatter(
            df,
            x="opportunity",
            y="realization",
            color=color_col,
            size=size_col if size_col else None,
            hover_data=available_hover,
            labels={
                "opportunity": "AI Opportunity Score",
                "realization": "AI Realization Score",
            },
            height=600,
        )

        # Add quadrant lines and labels
        opp_median = df["opportunity"].median()
        real_median = df["realization"].median()

        fig.add_hline(y=real_median, line_dash="dash", line_color="gray", opacity=0.5)
        fig.add_vline(x=opp_median, line_dash="dash", line_color="gray", opacity=0.5)

        # Quadrant labels
        fig.add_annotation(x=0.25, y=0.85, text="Over-investing?", showarrow=False, font=dict(size=12, color="gray"))
        fig.add_annotation(x=0.75, y=0.85, text="AI Leaders", showarrow=False, font=dict(size=12, color="gray"))
        fig.add_annotation(x=0.25, y=0.15, text="AI-Resistant", showarrow=False, font=dict(size=12, color="gray"))
        fig.add_annotation(x=0.75, y=0.15, text="Untapped Potential", showarrow=False, font=dict(size=12, color="gray"))

        fig.update_layout(
            xaxis=dict(range=[0, 1]),
            yaxis=dict(range=[0, 1]),
        )

        st.plotly_chart(fig, use_container_width=True, key="main_scatter")

        # ── Quadrant Distribution ──────────────────────────────────────────
        st.subheader("Quadrant Distribution")
        col_a, col_b = st.columns(2)

        with col_a:
            quad_fig = px.pie(
                df,
                names="quadrant_label",
                title="Companies by Quadrant",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(quad_fig, use_container_width=True)

        with col_b:
            if "sector" in df.columns:
                sector_fig = px.histogram(
                    df,
                    x="sector",
                    color="quadrant_label",
                    title="Quadrant by Sector",
                    barmode="stack",
                )
                sector_fig.update_xaxes(tickangle=45)
                st.plotly_chart(sector_fig, use_container_width=True)

        # ── Company Table with Clickable Links ─────────────────────────────
        st.subheader("Company Rankings")

        display_df = df.copy()
        display_cols = [
            c for c in ["ticker", "company_name", "sector", "opportunity", "realization", "quadrant_label", "market_cap"]
            if c in display_df.columns
        ]
        display_df = display_df[display_cols].sort_values("opportunity", ascending=False)

        # Add a clickable link column
        display_df.insert(0, "Detail", display_df["ticker"].apply(
            lambda t: f"?ticker={t}"
        ))

        st.dataframe(
            display_df,
            use_container_width=True,
            height=400,
            column_config={
                "Detail": st.column_config.LinkColumn(
                    "Detail",
                    display_text="View",
                ),
            },
        )

        # ── Index Performance ─────────────────────────────────────────────
        st.subheader("Index Performance")

        import json

        variant_names = {
            "top_30_score_weighted": "Top 30 Score-Weighted",
            "top_50_equal_weighted": "Top 50 Equal-Weighted",
            "top_50_score_weighted": "Top 50 Score-Weighted",
        }

        # Load index history CSVs
        history_dfs = {}
        for variant_key, variant_label in variant_names.items():
            csv_path = PROCESSED_DIR / f"index_history_{variant_key}.csv"
            if csv_path.exists():
                history_dfs[variant_label] = pd.read_csv(csv_path, parse_dates=["date"])

        if history_dfs:
            # Growth of $10K chart
            fig_perf = go.Figure()
            spy_added = False
            colors = {"Top 30 Score-Weighted": "#6366f1", "Top 50 Equal-Weighted": "#22c55e", "Top 50 Score-Weighted": "#f59e0b"}

            for label, hdf in history_dfs.items():
                fig_perf.add_trace(go.Scatter(
                    x=hdf["date"], y=hdf["portfolio_value"],
                    name=label, mode="lines",
                    line=dict(color=colors.get(label, "#888"), width=2),
                ))
                if not spy_added:
                    fig_perf.add_trace(go.Scatter(
                        x=hdf["date"], y=hdf["spy_value"],
                        name="S&P 500 (SPY)", mode="lines",
                        line=dict(color="#ef4444", width=2, dash="dash"),
                    ))
                    spy_added = True

            fig_perf.update_layout(
                title="Growth of $10,000",
                yaxis_title="Portfolio Value ($)",
                yaxis_tickprefix="$", yaxis_tickformat=",.0f",
                hovermode="x unified", height=500,
            )
            st.plotly_chart(fig_perf, use_container_width=True, key="index_perf_chart")

            # Metrics cards
            metrics_path = PROCESSED_DIR / "index_metrics.json"
            if metrics_path.exists():
                with open(metrics_path) as f:
                    all_metrics = json.load(f)

                for variant_key, variant_label in variant_names.items():
                    m = all_metrics.get(variant_key)
                    if not m:
                        continue
                    with st.expander(f"{variant_label} — Metrics", expanded=False):
                        mc1, mc2, mc3, mc4 = st.columns(4)
                        mc1.metric("Ann. Return", f"{m['annualized_return'] * 100:.1f}%")
                        mc2.metric("Sharpe Ratio", f"{m['sharpe_ratio']:.2f}")
                        mc3.metric("Max Drawdown", f"{m['max_drawdown'] * 100:.1f}%")
                        mc4.metric("Alpha vs SPY", f"{m['alpha_vs_spy'] * 100:.1f}%")

                        mc5, mc6, mc7, mc8 = st.columns(4)
                        mc5.metric("Beta", f"{m['beta']:.2f}")
                        mc6.metric("Info Ratio", f"{m['information_ratio']:.2f}")
                        mc7.metric("Total Return", f"{m['total_return'] * 100:.1f}%")
                        mc8.metric("Period", f"{m['period_years']:.1f} yrs")

            with st.expander("Index Methodology"):
                st.markdown("""
**AI Opportunity Index** ranks publicly traded companies on a composite of two dimensions:
- **AI Opportunity Score**: How much could AI impact this company's revenue and cost structure?
- **AI Realization Score**: How much AI is this company actually implementing?

The composite score is the average of the two dimensions. Three portfolio variants are offered:
1. **Top 30 Score-Weighted**: Top 30 companies, weighted proportional to composite score
2. **Top 50 Equal-Weighted**: Top 50 companies, equally weighted
3. **Top 50 Score-Weighted**: Top 50 companies, weighted proportional to composite score

*Backtested performance is hypothetical and does not reflect actual trading.*
                """)
        else:
            st.info("Index performance data not yet computed. Run `python scripts/compute_index_history.py`.")

        # ── Quick Ticker Lookup ─────────────────────────────────────────────
        st.subheader("Company Detail")
        ticker_input = st.text_input("Enter ticker symbol for detail view:")
        if ticker_input:
            st.query_params["ticker"] = ticker_input.upper()
            st.rerun()


if __name__ == "__main__":
    main()
