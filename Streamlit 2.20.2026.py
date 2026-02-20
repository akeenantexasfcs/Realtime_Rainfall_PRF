"""
PRF Live Rainfall Tracker
Streamlit in Snowflake Application
Texas Farm Credit
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

FC_GREEN = "#5E9732"
FC_SLATE = "#5B707F"
FC_RUST = "#9D5F58"
FC_CREAM = "#F5F1E8"
FC_AMBER = "#C4952B"

st.markdown(f"""
<style>
    .main .block-container,
    .appview-container .main .block-container,
    section[data-testid="stMainBlockContainer"] {{
        max-width: 100% !important;
        width: 100% !important;
        padding: 1rem 2rem !important;
    }}
    div[data-testid="stMetric"] {{
        background: {FC_CREAM};
        border: 2px solid {FC_GREEN};
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }}
    div[data-testid="stMetric"] label {{ 
        color: {FC_SLATE} !important; 
        font-size: 1rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.5px !important;
    }}
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {{ 
        color: #2d3a2e !important; 
        font-size: 2rem !important;
        font-weight: 800 !important;
    }}
    .signal-indemnity {{ 
        background: {FC_GREEN}; color: white; padding: 8px 20px; 
        border-radius: 8px; font-weight: 700; font-size: 1rem;
        text-align: center; margin-top: 4px;
    }}
    .signal-ok {{ 
        background: {FC_SLATE}; color: white; padding: 8px 20px; 
        border-radius: 8px; font-weight: 700; font-size: 1rem;
        text-align: center; margin-top: 4px;
    }}
    section[data-testid="stSidebar"] {{ min-width: 320px; max-width: 380px; }}
    div[data-testid="stDataFrame"],
    div[data-testid="stDataFrame"] > div {{
        width: 100% !important;
    }}
</style>
""", unsafe_allow_html=True)

st.markdown("# 🌾 PRF Live Rainfall Tracker")
st.markdown("**Jan–Feb 2026 · Interval 625** · Real-time CPC rainfall vs 3-year implied normals")
st.divider()

@st.cache_data(ttl=3600)
def load_texas_grids():
    return session.sql("""
        WITH grid_counties AS (
            SELECT TRY_TO_NUMBER(m.SUB_COUNTY_CODE) AS GRID_ID,
                   LISTAGG(DISTINCT c.COUNTY_NAME, ' / ') WITHIN GROUP (ORDER BY c.COUNTY_NAME) AS COUNTY_NAME
            FROM MAP_YTD m
            LEFT JOIN COUNTY_YTD c 
                ON c.STATE_CODE = '48' AND c.COUNTY_CODE = m.COUNTY_CODE
                AND c.REINSURANCE_YEAR = 2025 AND c.DELETED_DATE IS NULL
            WHERE m.INSURANCE_PLAN_CODE = '13' AND m.STATE_CODE = '48' AND m.DELETED_DATE IS NULL
            GROUP BY 1
        )
        SELECT n.GRID_ID, n.NORMAL_IN, n.CV_PCT, n.CONFIDENCE_TIER, n.YEARS_USED,
               gc.COUNTY_NAME, g.CENTER_LAT, g.CENTER_LON
        FROM PRF_GRID_NORMALS n
        JOIN grid_counties gc ON n.GRID_ID = gc.GRID_ID
        LEFT JOIN PRF_GRID_REFERENCE g ON g.GRIDCODE = n.GRID_ID
        ORDER BY n.GRID_ID
    """).to_pandas()

@st.cache_data(ttl=600)
def load_rainfall_2026():
    return session.sql("""
        SELECT g.GRIDCODE AS GRID_ID,
               ROUND(SUM(r.PRECIP_IN), 4) AS RAIN_SO_FAR,
               COUNT(DISTINCT r.OBSERVATION_DATE) AS DAYS_COLLECTED,
               MAX(r.OBSERVATION_DATE) AS LAST_DAY,
               MIN(r.FILE_TYPE) AS FILE_TYPE
        FROM PRF_RAINFALL_REALTIME r
        JOIN PRF_GRID_REFERENCE g
            ON ROUND(r.LATITUDE, 3) = ROUND(g.CENTER_LAT, 3)
            AND ROUND(r.LONGITUDE, 3) = ROUND(g.CENTER_LON, 3)
        WHERE r.OBSERVATION_DATE BETWEEN '2026-01-01' AND '2026-02-28'
        GROUP BY 1
    """).to_pandas()

def build_tracker(grids_df, rain_df, coverage_level):
    merged = grids_df.merge(rain_df, on="GRID_ID", how="inner")
    total_days = 59
    merged["PARTIAL_INDEX"] = (merged["RAIN_SO_FAR"] / merged["NORMAL_IN"] * 100).round(1)
    merged["DAILY_RATE"] = merged["RAIN_SO_FAR"] / merged["DAYS_COLLECTED"]
    merged["PROJECTED_RAIN"] = (merged["DAILY_RATE"] * total_days).round(4)
    merged["PROJECTED_INDEX"] = (merged["PROJECTED_RAIN"] / merged["NORMAL_IN"] * 100).round(1)
    trigger = coverage_level
    merged["SIGNAL"] = merged["PROJECTED_INDEX"].apply(
        lambda idx: "LIKELY INDEMNITY" if idx < trigger else "OK"
    )
    return merged


def create_gauge(grid_id, projected_index, partial_index, signal, 
                 rain_so_far, normal_in, days, coverage_level, county_name=None):
    
    bar_color = FC_GREEN if signal == "LIKELY INDEMNITY" else FC_SLATE
    
    trigger = coverage_level
    max_range = max(150, projected_index + 20)
    pct_through = round(days / 59 * 100)
    
    county_str = ""
    if county_name and pd.notna(county_name):
        county_str = f"  ·  {county_name}"
    
    fig = go.Figure()
    
    fig.add_trace(go.Indicator(
        mode="gauge+number",
        value=projected_index,
        number={
            "valueformat": ".1f",
            "font": {"size": 64, "color": "#2d3a2e", "family": "Arial Black"},
        },
        title={
            "text": (
                f"<b style='font-size:20px'>Grid {grid_id}</b>"
                f"<span style='font-size:13px;color:{FC_SLATE}'>{county_str}</span>"
                f"<br>"
                f"<span style='font-size:14px;color:{FC_SLATE}'>"
                f"Rain: <b>{rain_so_far:.2f}\"</b> of {normal_in:.1f}\" normal"
                f"  ·  {days}/59 days ({pct_through}%)"
                f"  ·  Coverage: {coverage_level}%"
                f"</span>"
            ),
            "font": {"size": 14, "color": "#2d3a2e"},
        },
        gauge={
            "axis": {
                "range": [0, max_range],
                "tickwidth": 2,
                "tickcolor": FC_SLATE,
                "tickfont": {"color": FC_SLATE, "size": 14},
                "dtick": 25,
            },
            "bar": {"color": bar_color, "thickness": 0.75},
            "bgcolor": "#e8e4dd",
            "borderwidth": 0,
            "steps": [
                {"range": [0, trigger], "color": "rgba(94, 151, 50, 0.28)"},
                {"range": [trigger, max_range], "color": "rgba(91, 112, 127, 0.08)"},
            ],
            "threshold": {
                "line": {"color": "#2d3a2e", "width": 5},
                "thickness": 0.9,
                "value": partial_index,
            },
        },
        domain={"x": [0.15, 0.85], "y": [0, 0.85]},
    ))
    
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=100, b=10),
        paper_bgcolor=FC_CREAM,
        plot_bgcolor=FC_CREAM,
        font={"color": "#2d3a2e"},
    )
    
    return fig


# ─── SIDEBAR ───
with st.sidebar:
    st.markdown(f"### ⚙️ Controls")
    
    grids_df = load_texas_grids()
    
    coverage_level = st.selectbox(
        "Coverage Level",
        options=[90, 85, 80, 75, 70],
        index=0,
        help="Indemnity triggers below this index level"
    )
    
    st.divider()
    
    grids_df["LABEL"] = grids_df.apply(
        lambda r: f"{r['GRID_ID']} — {r['COUNTY_NAME']}" 
        if pd.notna(r.get("COUNTY_NAME")) else str(r["GRID_ID"]), axis=1
    )
    label_to_id = dict(zip(grids_df["LABEL"], grids_df["GRID_ID"]))
    
    all_counties = set()
    for names in grids_df["COUNTY_NAME"].dropna():
        for c in names.split(" / "):
            all_counties.add(c.strip())
    
    selected_counties = st.multiselect("Filter by County", sorted(all_counties), default=[])
    
    if selected_counties:
        mask = grids_df["COUNTY_NAME"].apply(
            lambda x: any(c in str(x) for c in selected_counties) if pd.notna(x) else False
        )
        filtered_labels = grids_df[mask]["LABEL"].tolist()
    else:
        filtered_labels = grids_df["LABEL"].tolist()
    
    st.markdown("**Select Grids**")
    grid_entry = st.text_input(
        "Enter Grid IDs (comma separated)",
        placeholder="7929, 8230, 8231",
        help="Type grid IDs directly"
    )
    selected_labels = st.multiselect("Or pick from list", filtered_labels, default=[])
    
    st.divider()
    st.markdown("**Quick Select**")
    col1, col2 = st.columns(2)
    with col1:
        top_n = st.selectbox("Driest N", [10, 25, 50, "All"], index=0)
    with col2:
        show_all_likely = st.checkbox("Likely only", value=False)
    
    st.divider()
    generate = st.button("🚀 Generate", type="primary", use_container_width=True)
    
    st.divider()
    st.markdown(f"""
    <div style='font-size:11px;color:{FC_SLATE}'>
        <b>Sources:</b> CPC Gauge (RT) · 3yr normals · Linear projection
    </div>
    """, unsafe_allow_html=True)


# ─── MAIN ───
if generate:
    rain_df = load_rainfall_2026()
    tracker = build_tracker(grids_df, rain_df, coverage_level)
    
    display_df = None
    if grid_entry and grid_entry.strip():
        try:
            typed_ids = [int(x.strip()) for x in grid_entry.split(",") if x.strip()]
            display_df = tracker[tracker["GRID_ID"].isin(typed_ids)].copy()
        except ValueError:
            st.error("Invalid grid IDs. Use comma-separated numbers like: 7929, 8230")
            st.stop()
    elif selected_labels:
        selected_ids = [label_to_id[lbl] for lbl in selected_labels]
        display_df = tracker[tracker["GRID_ID"].isin(selected_ids)].copy()
    elif show_all_likely:
        display_df = tracker[tracker["SIGNAL"] == "LIKELY INDEMNITY"].copy()
    else:
        n = len(tracker) if top_n == "All" else int(top_n)
        display_df = tracker.nsmallest(n, "PROJECTED_INDEX").copy()
    
    if display_df is None or display_df.empty:
        st.warning("No grids found.")
        st.stop()
    
    display_df = display_df.sort_values("PROJECTED_INDEX", ascending=True)
    
    days_in = int(display_df["DAYS_COLLECTED"].iloc[0])
    likely_ct = len(display_df[display_df["SIGNAL"] == "LIKELY INDEMNITY"])
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Grids", len(display_df))
    c2.metric("Days", f"{days_in} / 59")
    c3.metric("Coverage", f"{coverage_level}%")
    c4.metric("Likely Indemnity", likely_ct)
    
    st.divider()
    st.markdown("### 📊 Projected Final Index")
    
    # ── Legend ──
    st.markdown(f"""
    <div style="
        display: flex; align-items: center; gap: 32px; 
        padding: 12px 20px; background: {FC_CREAM}; 
        border: 1px solid #d5d0c6; border-radius: 8px; margin-bottom: 16px;
        flex-wrap: wrap;
    ">
        <div style="display:flex; align-items:center; gap:8px;">
            <div style="width:40px; height:14px; background:{FC_GREEN}; border-radius:3px;"></div>
            <span style="font-size:13px; color:#2d3a2e;"><b>Bar</b> — Projected Final Index</span>
        </div>
        <div style="display:flex; align-items:center; gap:8px;">
            <div style="width:4px; height:22px; background:#2d3a2e; border-radius:1px;"></div>
            <span style="font-size:13px; color:#2d3a2e;"><b>Line</b> — Current Estimated Index Value</span>
        </div>
        <div style="display:flex; align-items:center; gap:8px;">
            <div style="width:40px; height:14px; background:rgba(94,151,50,0.28); border:1px solid #ccc; border-radius:3px;"></div>
            <span style="font-size:13px; color:#2d3a2e;">Indemnity zone (below {coverage_level})</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    for _, row in display_df.iterrows():
        fig = create_gauge(
            grid_id=row["GRID_ID"],
            projected_index=row["PROJECTED_INDEX"],
            partial_index=row["PARTIAL_INDEX"],
            signal=row["SIGNAL"],
            rain_so_far=row["RAIN_SO_FAR"],
            normal_in=row["NORMAL_IN"],
            days=row["DAYS_COLLECTED"],
            coverage_level=coverage_level,
            county_name=row.get("COUNTY_NAME"),
        )
        st.plotly_chart(fig, use_container_width=True)
        
        sig = row["SIGNAL"]
        proj = row["PROJECTED_INDEX"]
        part = row["PARTIAL_INDEX"]
        if sig == "LIKELY INDEMNITY":
            st.markdown(
                f'<div class="signal-indemnity">'
                f'✅ LIKELY INDEMNITY — Current: {part:.1f}  ·  Projected: {proj:.1f}  ·  Trigger: {coverage_level}'
                f'</div>', unsafe_allow_html=True)
        else:
            st.markdown(
                f'<div class="signal-ok">'
                f'OK — Current: {part:.1f}  ·  Projected: {proj:.1f}'
                f'</div>', unsafe_allow_html=True)
        
        st.markdown("")
    
    st.divider()
    
    st.markdown("### 📋 Detail")
    
    table_df = display_df[[
        "GRID_ID", "COUNTY_NAME", "NORMAL_IN", "DAYS_COLLECTED",
        "RAIN_SO_FAR", "PARTIAL_INDEX", "PROJECTED_RAIN", 
        "PROJECTED_INDEX", "SIGNAL", "CV_PCT"
    ]].copy()
    table_df.columns = [
        "Grid", "Counties", "Normal (in)", "Days", 
        "Rain (in)", "Current Idx", "Proj Rain (in)",
        "Proj Index", "Signal", "CV%"
    ]
    
    st.dataframe(
        table_df, use_container_width=True, hide_index=True,
        height=min(600, 50 + len(table_df) * 40),
        column_config={
            "Grid": st.column_config.NumberColumn(format="%d"),
            "Normal (in)": st.column_config.NumberColumn(format="%.1f"),
            "Rain (in)": st.column_config.NumberColumn(format="%.1f"),
            "Proj Rain (in)": st.column_config.NumberColumn(format="%.1f"),
            "Current Idx": st.column_config.NumberColumn(format="%.1f"),
            "Proj Index": st.column_config.NumberColumn(format="%.1f"),
            "CV%": st.column_config.NumberColumn(format="%.1f"),
        }
    )

else:
    st.markdown(f"""
    <div style='text-align:center; padding:80px 20px; color:{FC_SLATE};'>
        <h2 style='color:{FC_GREEN};'>Select grids and click Generate</h2>
        <p style='font-size:16px;'>
            Type grid IDs directly or use the county filter.<br>
            Set your coverage level, then Generate.
        </p>
        <p style='font-size:14px; margin-top:20px;'>
            🌾 636 insured grids · 3-year implied normals · 0.4% avg CV · RT data through today
        </p>
    </div>
    """, unsafe_allow_html=True)