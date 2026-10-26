"""HVACPulse local dashboard — reads Delta tables directly (no Spark needed).

Run: streamlit run dashboard/app.py
Data root: HVACPULSE_DATA_ROOT (default .local/)
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("HVACPULSE_DATA_ROOT", str(REPO_ROOT / ".local")))


def load(name: str) -> pd.DataFrame:
    path = DATA_ROOT / "delta" / name
    if not (path / "_delta_log").exists():
        return pd.DataFrame()
    return DeltaTable(str(path)).to_pandas()


st.set_page_config(page_title="HVACPulse", layout="wide")
st.title("HVACPulse — telemetry to control")
st.caption(f"Data root: `{DATA_ROOT}`")

tab_plant, tab_energy, tab_quality, tab_freshness, tab_recs = st.tabs(
    ["Plant learning & control", "Air-handler energy", "Air-handler quality", "Freshness & compliance", "Advisory recommendations"]
)

with tab_plant:
    from dashboard.plant_view import render_plant

    render_plant(load)

with tab_energy:
    gold = load("gold_hvac_metrics_current")
    if gold.empty:
        st.info("No Gold metrics yet — run the pipeline first.")
    else:
        gold = gold.sort_values("window_start")
        site = st.selectbox("Site", sorted(gold["site_id"].unique()))
        view = gold[gold["site_id"] == site]
        # Reported energy: FINAL and RESTATED only (matches mv_hvac_energy.reported_energy_kwh)
        reported_view = view[view["window_status"].isin(["FINAL", "RESTATED"])]
        c1, c2, c3 = st.columns(3)
        c1.metric("Devices", view["device_id"].nunique())
        c2.metric("Reported Energy (kWh)", round(reported_view["energy_kwh"].sum(), 2))
        c3.metric("Restated windows", int((view["window_status"] == "RESTATED").sum()))
        st.subheader("Energy per window")
        st.line_chart(
            view.pivot_table(
                index="window_start", columns="device_id", values="energy_kwh", aggfunc="sum"
            )
        )
        st.subheader("Zone temperature vs setpoint")
        st.line_chart(
            view.pivot_table(
                index="window_start", columns="device_id", values="avg_zone_temp_c", aggfunc="mean"
            )
        )
        st.dataframe(view, use_container_width=True)

with tab_quality:
    scorecard = load("gold_data_quality_scorecard")
    quarantine = load("quarantine_events")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Quality scorecard")
        if scorecard.empty:
            st.info("Run `python -m streaming.jobs.quality_metrics`.")
        else:
            st.dataframe(
                scorecard.sort_values("window_start", ascending=False),
                use_container_width=True,
            )
    with c2:
        st.subheader("Quarantine reasons")
        if quarantine.empty:
            st.info("Quarantine is empty.")
        else:
            counts = quarantine["error_code"].value_counts()
            st.bar_chart(counts)
            st.dataframe(quarantine.head(200), use_container_width=True)

with tab_freshness:
    freshness = load("gold_sensor_freshness")
    compliance = load("gold_compliance")
    health = load("gold_equipment_health")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Sensor freshness")
        if freshness.empty:
            st.info("Run `python -m streaming.jobs.quality_metrics`.")
        else:
            st.dataframe(freshness, use_container_width=True)
        st.subheader("Equipment health")
        if not health.empty:
            st.dataframe(health, use_container_width=True)
    with c2:
        st.subheader("Environmental compliance")
        if compliance.empty:
            st.info("Run `python -m streaming.jobs.quality_metrics`.")
        else:
            st.dataframe(
                compliance.sort_values("violation_minutes", ascending=False),
                use_container_width=True,
            )

with tab_recs:
    recs = load("gold_recommendations")
    events = load("recommendation_events")
    late = load("late_corrections")
    restated = load("metric_restatement_events")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Active recommendations (advisory)")
        if recs.empty:
            st.info("Run `python -m streaming.jobs.recommendations_job`.")
        else:
            st.dataframe(recs, use_container_width=True)
        st.subheader("Late corrections")
        if not late.empty:
            st.dataframe(late, use_container_width=True)
    with c2:
        st.subheader("Revocation events")
        if not events.empty:
            st.dataframe(events, use_container_width=True)
        st.subheader("Metric restatements")
        if not restated.empty:
            st.dataframe(restated, use_container_width=True)
