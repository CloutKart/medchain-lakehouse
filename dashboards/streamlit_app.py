"""MedChain Analytics dashboard.

Four tabs matching the spec's consumer groups, plus data quality:

    Clinical      patient journey, readmissions, the MPI's clinical value
    Operational   bed occupancy, ward pressure, doctor utilisation
    Financial     claim settlement, denial reasons, the reimbursement gap
    Quality       the scorecard, including recovery metrics against ground truth

Reads the Gold Delta tables directly through delta-rs and DuckDB rather than through
Spark or a SQL warehouse. That matters for two reasons: the dashboard starts in
under a second instead of waiting on a JVM, and demoing it on Azure costs nothing
because no cluster has to be running. Set MEDCHAIN_BACKEND=databricks to route the
same queries through a SQL warehouse when one is up.
"""

from __future__ import annotations

import os

# The package is importable when the app runs from the repo root; fall back to the
# src layout so `streamlit run dashboards/streamlit_app.py` works either way.
import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from medchain.config import load_config  # noqa: E402

st.set_page_config(
    page_title="MedChain Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

PALETTE = ["#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#3B7A57", "#6C5B7B", "#355070"]

GOLD_TABLES = [
    "dim_date",
    "dim_patient",
    "dim_doctor",
    "dim_hospital",
    "dim_insurer",
    "dim_procedure",
    "fact_patient_visit",
    "fact_claim_lifecycle",
    "fact_billing_reconciliation",
    "fact_bed_occupancy",
    "dq_scorecard",
]


@st.cache_resource(show_spinner="Connecting to the Gold layer…")
def connect() -> duckdb.DuckDBPyConnection:
    """Register every Gold Delta table as a DuckDB view.

    DuckDB reads Delta through delta-rs, so the dashboard needs neither Spark nor a
    running warehouse — it reads the same files the pipeline wrote.
    """
    cfg = load_config(os.environ.get("MEDCHAIN_ENV", "local"))
    con = duckdb.connect(":memory:")
    con.execute("INSTALL delta; LOAD delta;")

    if cfg.is_azure:
        con.execute("INSTALL azure; LOAD azure;")
        con.execute("CREATE SECRET (TYPE azure, PROVIDER credential_chain);")

    registered = []
    for table in GOLD_TABLES:
        path = cfg.table_path("gold", table)
        try:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM delta_scan('{path}')")
            registered.append(table)
        except Exception:  # noqa: BLE001 - a missing table is reported in the UI
            pass
    return con, registered


@st.cache_data(ttl=300, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    con, _ = connect()
    return con.execute(sql).fetchdf()


def metric_row(items: list[tuple[str, str, str | None]]) -> None:
    cols = st.columns(len(items))
    for col, (label, value, delta) in zip(cols, items):
        col.metric(label, value, delta)


def styled(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=40, b=10),
        colorway=PALETTE,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.2)")
    return fig


# --------------------------------------------------------------------- shell

con, registered = connect()

st.title("🏥 MedChain Analytics")
st.caption("Unified lakehouse across 8 hospitals · 5 cities · Bronze → Silver → Gold on Delta Lake")

if not registered:
    st.error(
        "No Gold tables found. Build them first:\n\n```\nmake gen SCALE=1.0\nmake run-local\n```"
    )
    st.stop()

missing = set(GOLD_TABLES) - set(registered)
if missing:
    st.warning(f"Missing tables (some panels will be empty): {', '.join(sorted(missing))}")

with st.sidebar:
    st.header("Filters")
    hospitals = q(
        "SELECT DISTINCT hospital_name, city FROM dim_hospital ORDER BY city, hospital_name"
    )
    cities = st.multiselect("City", sorted(hospitals["city"].unique()), default=None)
    city_filter = "AND h.city IN (" + ",".join(f"'{c}'" for c in cities) + ")" if cities else ""
    st.divider()
    st.caption(f"Environment: `{os.environ.get('MEDCHAIN_ENV', 'local')}`")
    st.caption(f"{len(registered)} Gold tables loaded")

clinical, operational, financial, quality = st.tabs(
    ["🩺 Clinical", "🛏️ Operational", "💰 Financial", "✅ Data Quality"]
)

# ------------------------------------------------------------------ clinical

with clinical:
    st.subheader("Patient journey and readmissions")

    summary = q(f"""
        SELECT
          COUNT(*) AS visits,
          COUNT(DISTINCT v.mpi_id) AS patients,
          SUM(CASE WHEN v.is_inpatient THEN 1 ELSE 0 END) AS inpatient,
          AVG(CASE WHEN v.is_inpatient THEN v.readmit_30d_network::INT END) AS rate_network,
          AVG(CASE WHEN v.is_inpatient THEN v.readmit_30d_same_hospital::INT END) AS rate_hospital
        FROM fact_patient_visit v
        JOIN dim_hospital h ON v.hospital_sk = h.hospital_sk
        WHERE 1=1 {city_filter}
    """).iloc[0]

    understatement = (summary["rate_network"] - summary["rate_hospital"]) * 100
    metric_row(
        [
            ("Visits", f"{int(summary['visits']):,}", None),
            ("Distinct patients (MPI)", f"{int(summary['patients']):,}", None),
            ("30-day readmission (network)", f"{summary['rate_network'] * 100:.2f}%", None),
            (
                "Single-hospital view",
                f"{summary['rate_hospital'] * 100:.2f}%",
                f"-{understatement:.2f} pp understated",
            ),
        ]
    )

    st.info(
        f"**What the Master Patient Index bought:** measured hospital by hospital, the "
        f"readmission rate reads {summary['rate_hospital'] * 100:.2f}%. Measured across the "
        f"network through resolved patient identities, it is {summary['rate_network'] * 100:.2f}% "
        f"— an understatement of {understatement:.2f} percentage points. Those are patients who "
        f"were readmitted somewhere else in the network under a different patient_id."
    )

    left, right = st.columns(2)

    with left:
        by_hospital = q(f"""
            SELECT h.hospital_name,
                   AVG(v.readmit_30d_same_hospital::INT) * 100 AS "Single hospital",
                   AVG(v.readmit_30d_network::INT) * 100       AS "Network wide"
            FROM fact_patient_visit v
            JOIN dim_hospital h ON v.hospital_sk = h.hospital_sk
            WHERE v.is_inpatient {city_filter}
            GROUP BY h.hospital_name ORDER BY "Network wide" DESC
        """)
        fig = px.bar(
            by_hospital.melt(id_vars="hospital_name", var_name="Measured", value_name="Rate %"),
            x="Rate %",
            y="hospital_name",
            color="Measured",
            barmode="group",
            title="30-day readmission rate: what each hospital sees vs reality",
        )
        fig.update_layout(yaxis_title=None)
        st.plotly_chart(styled(fig, 420), use_container_width=True)

    with right:
        monthly = q(f"""
            SELECT d.month_start AS month,
                   COUNT(*) AS visits,
                   AVG(v.readmit_30d_network::INT) * 100 AS readmission_pct
            FROM fact_patient_visit v
            JOIN dim_date d ON v.admission_date_sk = d.date_sk
            JOIN dim_hospital h ON v.hospital_sk = h.hospital_sk
            WHERE v.is_inpatient {city_filter}
            GROUP BY d.month_start ORDER BY d.month_start
        """)
        fig = go.Figure()
        fig.add_bar(
            x=monthly["month"],
            y=monthly["visits"],
            name="Inpatient visits",
            marker_color=PALETTE[0],
            opacity=0.55,
        )
        fig.add_scatter(
            x=monthly["month"],
            y=monthly["readmission_pct"],
            name="Readmission %",
            yaxis="y2",
            line=dict(color=PALETTE[3], width=3),
        )
        fig.update_layout(
            title="Admission volume and readmission rate over time",
            yaxis=dict(title="Visits"),
            yaxis2=dict(title="Readmission %", overlaying="y", side="right", showgrid=False),
        )
        st.plotly_chart(styled(fig, 420), use_container_width=True)

    st.subheader("Cross-hospital care continuity")
    multi = q("""
        SELECT registered_hospital_count AS hospitals_registered_at,
               COUNT(*) AS patients
        FROM dim_patient GROUP BY 1 ORDER BY 1
    """)
    fig = px.bar(
        multi,
        x="hospitals_registered_at",
        y="patients",
        title="Patients by number of hospitals they are registered at",
        labels={"hospitals_registered_at": "Hospitals", "patients": "Patients"},
    )
    fig.update_yaxes(type="log")
    st.plotly_chart(styled(fig, 320), use_container_width=True)
    st.caption(
        "Every patient above 1 was invisible as a single person before identity "
        "resolution — they existed as two or more unrelated records."
    )

# --------------------------------------------------------------- operational

with operational:
    st.subheader("Bed occupancy and ward pressure")

    occ = q(f"""
        SELECT AVG(b.occupancy_rate) AS avg_occ,
               SUM(CASE WHEN b.is_high_occupancy THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS pct_high,
               AVG(b.avg_length_of_stay) AS alos,
               SUM(b.admissions) AS admissions
        FROM fact_bed_occupancy b
        JOIN dim_hospital h ON b.hospital_sk = h.hospital_sk
        WHERE 1=1 {city_filter}
    """).iloc[0]

    metric_row(
        [
            ("Average occupancy", f"{occ['avg_occ'] * 100:.1f}%", None),
            ("Ward-days above 85%", f"{occ['pct_high'] * 100:.1f}%", None),
            ("Average length of stay", f"{occ['alos']:.1f} days", None),
            ("Total admissions", f"{int(occ['admissions']):,}", None),
        ]
    )

    heat = q(f"""
        SELECT h.hospital_name, b.ward_type,
               AVG(b.occupancy_rate) * 100 AS occupancy_pct
        FROM fact_bed_occupancy b
        JOIN dim_hospital h ON b.hospital_sk = h.hospital_sk
        WHERE 1=1 {city_filter}
        GROUP BY h.hospital_name, b.ward_type
    """)
    if not heat.empty:
        pivot = heat.pivot(index="hospital_name", columns="ward_type", values="occupancy_pct")
        fig = px.imshow(
            pivot,
            text_auto=".0f",
            aspect="auto",
            color_continuous_scale="RdYlGn_r",
            title="Mean occupancy % by hospital and ward type",
            labels=dict(color="Occupancy %"),
        )
        st.plotly_chart(styled(fig, 400), use_container_width=True)

    left, right = st.columns(2)
    with left:
        util = q("""
            SELECT department_at_visit AS department, COUNT(*) AS consultations,
                   COUNT(DISTINCT doctor_id) AS doctors,
                   COUNT(*)::DOUBLE / NULLIF(COUNT(DISTINCT doctor_id), 0) AS per_doctor
            FROM fact_patient_visit
            WHERE department_at_visit IS NOT NULL
            GROUP BY 1 ORDER BY per_doctor DESC LIMIT 15
        """)
        fig = px.bar(
            util,
            x="per_doctor",
            y="department",
            orientation="h",
            title="Consultations per doctor, by department at time of visit",
        )
        fig.update_layout(yaxis_title=None, xaxis_title="Consultations per doctor")
        st.plotly_chart(styled(fig, 440), use_container_width=True)

    with right:
        st.markdown("**Attribution: point-in-time vs current department**")
        attribution = q("""
            WITH pit AS (
              SELECT department_at_visit AS department, COUNT(*) AS correct
              FROM fact_patient_visit WHERE department_at_visit IS NOT NULL GROUP BY 1
            ), naive AS (
              SELECT department_current AS department, COUNT(*) AS naive
              FROM fact_patient_visit WHERE department_current IS NOT NULL GROUP BY 1
            )
            SELECT COALESCE(p.department, n.department) AS department,
                   COALESCE(p.correct, 0) AS correct,
                   COALESCE(n.naive, 0) AS naive,
                   COALESCE(n.naive, 0) - COALESCE(p.correct, 0) AS misattributed
            FROM pit p FULL OUTER JOIN naive n ON p.department = n.department
            ORDER BY ABS(COALESCE(n.naive,0) - COALESCE(p.correct,0)) DESC LIMIT 12
        """)
        fig = px.bar(
            attribution,
            x="misattributed",
            y="department",
            orientation="h",
            color="misattributed",
            color_continuous_scale="RdBu",
            color_continuous_midpoint=0,
            title="Consultations misattributed without SCD2 history",
        )
        fig.update_layout(yaxis_title=None, coloraxis_showscale=False)
        st.plotly_chart(styled(fig, 440), use_container_width=True)
        st.caption(
            "Without effective-dated doctor history, this many consultations would be "
            "credited to the department each doctor sits in *today*."
        )

# ----------------------------------------------------------------- financial

with financial:
    st.subheader("Claims, settlement and the reimbursement gap")

    fin = q(f"""
        SELECT SUM(r.billed_amount) AS billed,
               SUM(r.net_reimbursement) AS reimbursed,
               SUM(r.reimbursement_gap) AS gap,
               SUM(r.room_rent_excess) AS room_excess,
               AVG(r.is_reconciled::INT) AS reconciled_rate
        FROM fact_billing_reconciliation r
        JOIN dim_hospital h ON r.hospital_sk = h.hospital_sk
        WHERE 1=1 {city_filter}
    """).iloc[0]

    metric_row(
        [
            ("Billed", f"₹{fin['billed'] / 1e7:,.1f} Cr", None),
            ("Reimbursed", f"₹{fin['reimbursed'] / 1e7:,.1f} Cr", None),
            (
                "Gap",
                f"₹{fin['gap'] / 1e7:,.1f} Cr",
                f"{fin['gap'] / fin['billed'] * 100:.1f}% of billed",
            ),
            ("Recoverable (room excess)", f"₹{fin['room_excess'] / 1e7:,.1f} Cr", None),
        ]
    )

    waterfall = q(f"""
        SELECT SUM(r.billed_amount) AS billed,
               SUM(r.excluded_amount) AS excluded,
               SUM(r.room_rent_excess) AS room_excess,
               SUM(r.copay_amount) AS copay,
               SUM(r.other_deduction) AS other,
               SUM(r.net_reimbursement) AS net
        FROM fact_billing_reconciliation r
        JOIN dim_hospital h ON r.hospital_sk = h.hospital_sk
        WHERE 1=1 {city_filter}
    """).iloc[0]

    fig = go.Figure(
        go.Waterfall(
            orientation="v",
            measure=["absolute", "relative", "relative", "relative", "relative", "total"],
            x=[
                "Billed",
                "Exclusions",
                "Room rent excess",
                "Co-pay",
                "Other deductions",
                "Net reimbursed",
            ],
            y=[
                waterfall["billed"] / 1e7,
                -waterfall["excluded"] / 1e7,
                -waterfall["room_excess"] / 1e7,
                -waterfall["copay"] / 1e7,
                -waterfall["other"] / 1e7,
                waterfall["net"] / 1e7,
            ],
            text=[
                f"₹{abs(v) / 1e7:,.1f} Cr"
                for v in [
                    waterfall["billed"],
                    waterfall["excluded"],
                    waterfall["room_excess"],
                    waterfall["copay"],
                    waterfall["other"],
                    waterfall["net"],
                ]
            ],
            textposition="outside",
            decreasing=dict(marker_color=PALETTE[3]),
            increasing=dict(marker_color=PALETTE[4]),
            totals=dict(marker_color=PALETTE[0]),
        )
    )
    fig.update_layout(
        title="Where the money goes: billed → net reimbursed (₹ crore)", yaxis_title="₹ crore"
    )
    st.plotly_chart(styled(fig, 440), use_container_width=True)
    st.caption(
        "Room-rent excess is the only bucket the hospital controls — it disappears if "
        "admission room category matches policy entitlement. Co-pay and contractual "
        "deductions are not recoverable."
    )

    left, right = st.columns(2)
    with left:
        funnel = q("""
            SELECT status_code, COUNT(DISTINCT claim_id) AS claims
            FROM fact_claim_lifecycle GROUP BY 1
        """)
        order = [
            "Submitted",
            "Under Review",
            "Partially Approved",
            "Approved",
            "Rejected",
            "Settled",
        ]
        funnel["rank"] = funnel["status_code"].map({s: i for i, s in enumerate(order)})
        funnel = funnel.sort_values("rank")
        fig = px.funnel(
            funnel,
            x="claims",
            y="status_code",
            title="Claim lifecycle funnel (reconstructed from snapshots)",
        )
        fig.update_layout(yaxis_title=None)
        st.plotly_chart(styled(fig, 420), use_container_width=True)

    with right:
        denials = q("""
            SELECT rejection_reason, COUNT(DISTINCT claim_id) AS claims,
                   SUM(claim_amount) / 100000 AS value_lakh
            FROM fact_claim_lifecycle
            WHERE status_code = 'Rejected' AND rejection_reason IS NOT NULL
            GROUP BY 1 ORDER BY value_lakh DESC LIMIT 10
        """)
        fig = px.bar(
            denials,
            x="value_lakh",
            y="rejection_reason",
            orientation="h",
            title="Denial reasons by value (₹ lakh)",
        )
        fig.update_layout(yaxis_title=None, xaxis_title="₹ lakh")
        st.plotly_chart(styled(fig, 420), use_container_width=True)

# ------------------------------------------------------------------- quality

with quality:
    st.subheader("Data quality scorecard")

    if "dq_scorecard" not in registered:
        st.warning("No scorecard yet. Run `make run-quality`.")
    else:
        latest = q("""
            SELECT * FROM dq_scorecard
            WHERE run_ts = (SELECT MAX(run_ts) FROM dq_scorecard)
        """)
        blocking_failed = latest[(latest.severity == "blocking") & (~latest.passed)]
        warnings_ = latest[(latest.severity != "blocking") & (~latest.passed)]

        metric_row(
            [
                ("Checks run", f"{len(latest)}", None),
                ("Passed", f"{int(latest.passed.sum())}", None),
                ("Blocking failures", f"{len(blocking_failed)}", None),
                ("Warnings", f"{len(warnings_)}", None),
            ]
        )

        if len(blocking_failed):
            st.error(f"{len(blocking_failed)} blocking check(s) failed")
            st.dataframe(
                blocking_failed[["check_name", "actual_value", "threshold", "detail"]],
                use_container_width=True,
                hide_index=True,
            )

        st.markdown(
            "**Recovery metrics** — measured against ground truth, not internal consistency"
        )
        recovery = latest[latest.check_type == "recovery"].copy()
        if not recovery.empty:
            recovery["value"] = recovery["actual_value"].round(4)
            st.dataframe(
                recovery[["check_name", "value", "threshold", "passed", "detail"]].sort_values(
                    "check_name"
                ),
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("**Structural checks**")
        structural = latest[latest.check_type != "recovery"].copy()
        structural["value"] = structural["actual_value"].round(4)
        st.dataframe(
            structural[
                [
                    "table_name",
                    "check_name",
                    "check_type",
                    "severity",
                    "value",
                    "threshold",
                    "passed",
                    "detail",
                ]
            ].sort_values(["passed", "table_name"]),
            use_container_width=True,
            hide_index=True,
        )
