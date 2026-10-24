"""Read-only view of the shared plant feature, model and control Delta tables."""

import json

import pandas as pd
import streamlit as st


def render_plant(load):
    st.warning("SYNTHETIC MOCK PLANT — no physical writes or verified energy savings.")
    st.caption(
        "Collect → Kafka/Bronze → validated Silver → streaming features → train → estimate load → gated control"
    )
    features = load("gold_plant_features")
    quarantine = load("quarantine_plant_snapshots")
    if not quarantine.empty and "row_id" in features.columns:
        features = features[~features["row_id"].isin(quarantine["row_id"].dropna())]
    if features.empty:
        st.info("No plant features yet. Run the plant pipeline described in docs/runbooks/plant-pipeline.md.")
        return
    scope = st.selectbox("Plant (tenant/site/plant)", sorted(features["scope"].unique()))
    features = features[features["scope"] == scope].copy()
    features["captured_at"] = pd.to_datetime(features["captured_at"], utc=True)
    features = features.sort_values("captured_at")
    days = sorted(features["captured_at"].dt.strftime("%Y-%m-%d").unique())
    day = st.selectbox("Feature day (UTC)", days, index=len(days) - 1)
    view = features[features["captured_at"].dt.strftime("%Y-%m-%d") == day]
    st.caption(
        f"Source: gold_plant_features · {len(view):,} instantaneous snapshots on {day} · latest capture {features['captured_at'].max().isoformat()}"
    )
    c1, c2, c3 = st.columns(3)
    c1.metric("Feature snapshots (selected day)", len(view))
    c2.metric("Mean cooling load (kW)", f"{view['cooling_kw'].mean():.1f}")
    c3.metric("Mean metered power (kW)", f"{view['power_kw'].mean():.1f}")
    st.caption(
        "Cooling = Σ 1.163 × water flow (m³/h) × return/supply Δ°C. Power = sum of plant meters. These are not kWh or savings."
    )
    for column, title in [("cooling_kw", "Measured cooling demand"), ("power_kw", "Plant electrical power")]:
        st.subheader(title)
        st.vega_lite_chart(
            view[["captured_at", column]],
            {
                "mark": {"type": "line", "point": True},
                "encoding": {
                    "x": {
                        "field": "captured_at",
                        "type": "temporal",
                        "scale": {"type": "utc"},
                        "title": "Capture time (UTC)",
                    },
                    "y": {"field": column, "type": "quantitative", "title": "kW"},
                },
            },
            use_container_width=True,
        )
    models = load("plant_model_registry")
    st.subheader("Model readiness")
    if models.empty or models[models["scope"] == scope].empty:
        st.info(
            "No trained model for this plant. Training requires clean history spanning at least two days."
        )
    else:
        models = models[models["scope"] == scope].sort_values(["trained_at", "model_id"], ascending=False)
        latest = models.iloc[0]
        metrics = json.loads(latest["metrics_json"])
        lineage = json.loads(latest["lineage_json"])
        revoked = not quarantine.empty and quarantine["row_id"].isin(lineage["feature_ids"]).any()
        if revoked:
            st.warning(
                "Model revoked for current use: a training snapshot was later quarantined. Retrain on clean data."
            )
        else:
            st.write(
                f"Training result: {'passed mock evaluation gates' if latest['approved'] else 'blocked by evaluation gate'}"
            )
        st.caption(
            "Runtime freshness, drift, interlocks and override gates are checked again for every decision."
        )
        st.caption(
            "Model evaluation is over its full holdout, not the selected feature day. 60/20/20 chronological split; CVRMSE ≤15% and beats a constant-load baseline."
        )
        st.dataframe(pd.DataFrame([metrics["test"]]), use_container_width=True)
        with st.expander("Model and training lineage"):
            st.dataframe(models.drop(columns=["artifact_json", "lineage_json"]), use_container_width=True)
            st.json(lineage)
    st.subheader("Control decisions (selected day)")
    decisions = load("gold_plant_control_decisions")
    if not decisions.empty:
        decisions = decisions[(decisions["scope"] == scope) & decisions["captured_at"].str.startswith(day)]
    if decisions.empty:
        st.info(
            "No decisions for this plant/day. Shadow mode is the default; simulate mode acknowledges mock commands only."
        )
    else:
        decisions = decisions.sort_values("decided_at", ascending=False)
        st.write(f"Latest on selected day: {decisions.iloc[0]['state']} — {decisions.iloc[0]['reason']}")
        st.dataframe(
            decisions[
                [
                    "state",
                    "reason",
                    "mode",
                    "captured_at",
                    "decided_at",
                    "model_id",
                    "feature_id",
                    "decision_id",
                ]
            ],
            use_container_width=True,
        )
        with st.expander("Last decision and command audit"):
            st.json(json.loads(decisions.sort_values("decided_at").iloc[-1]["result_json"]))
    if not quarantine.empty:
        quarantine = quarantine[(quarantine["scope"] == scope) | quarantine["scope"].isna()]
        st.subheader("Rejected snapshots (all loaded dates; includes unscoped malformed records)")
        st.dataframe(
            quarantine[["captured_at", "available_at", "issues", "completeness"]].tail(200),
            use_container_width=True,
        )
    with st.expander("Feature records and lineage"):
        st.dataframe(view.tail(200), use_container_width=True)
