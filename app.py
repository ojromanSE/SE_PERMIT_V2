import io
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="TX/LA Drilling Permit Tracker", layout="wide")

REQUIRED_COLUMNS = [
    "permit_id",
    "state",
    "operator",
    "county_parish",
    "well_name",
    "permit_type",
    "status",
    "application_date",
    "approval_date",
    "expiration_date",
]


def load_sample_data() -> pd.DataFrame:
    today = date.today()
    rows = [
        {
            "permit_id": "TX-2026-00123",
            "state": "TX",
            "operator": "Lone Star Energy",
            "county_parish": "Reeves",
            "well_name": "LSU 14H",
            "permit_type": "Horizontal",
            "status": "Approved",
            "application_date": today - timedelta(days=34),
            "approval_date": today - timedelta(days=13),
            "expiration_date": today + timedelta(days=352),
        },
        {
            "permit_id": "TX-2026-00188",
            "state": "TX",
            "operator": "Permian Delta Operating",
            "county_parish": "Midland",
            "well_name": "Delta Unit 5",
            "permit_type": "Vertical",
            "status": "Pending",
            "application_date": today - timedelta(days=21),
            "approval_date": pd.NaT,
            "expiration_date": today + timedelta(days=344),
        },
        {
            "permit_id": "LA-2026-0041",
            "state": "LA",
            "operator": "Bayou Hydrocarbons",
            "county_parish": "Caddo",
            "well_name": "BH Cotton Valley 3",
            "permit_type": "Directional",
            "status": "Approved",
            "application_date": today - timedelta(days=50),
            "approval_date": today - timedelta(days=25),
            "expiration_date": today + timedelta(days=155),
        },
        {
            "permit_id": "LA-2026-0057",
            "state": "LA",
            "operator": "Gulf Coast Resources",
            "county_parish": "Lafourche",
            "well_name": "GC Levee 1",
            "permit_type": "Re-entry",
            "status": "Expired",
            "application_date": today - timedelta(days=420),
            "approval_date": today - timedelta(days=390),
            "expiration_date": today - timedelta(days=15),
        },
    ]
    return pd.DataFrame(rows)


def validate_dataframe(df: pd.DataFrame) -> tuple[bool, list[str]]:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    return (len(missing) == 0, missing)


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["application_date", "approval_date", "expiration_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


st.title("Texas + Louisiana Drilling Permit Tracker")
st.caption(
    "Track, filter, and prioritize drilling permits for operators across TX and LA. "
    "Upload your permit export or start with sample data."
)

with st.sidebar:
    st.header("Data source")
    uploaded = st.file_uploader("Upload permit CSV", type=["csv"])
    use_sample = st.toggle("Use sample data", value=uploaded is None)

if uploaded is not None and not use_sample:
    data = pd.read_csv(io.BytesIO(uploaded.getvalue()))
else:
    data = load_sample_data()

valid, missing_cols = validate_dataframe(data)
if not valid:
    st.error(
        "The uploaded file is missing required columns: "
        + ", ".join(missing_cols)
        + ".\n\n"
        + "Expected columns: "
        + ", ".join(REQUIRED_COLUMNS)
    )
    st.stop()

data = normalize_dates(data)

today = pd.Timestamp.today().normalize()
data["days_to_expiry"] = (data["expiration_date"] - today).dt.days

st.subheader("Portfolio at a glance")
metric_1, metric_2, metric_3, metric_4 = st.columns(4)
metric_1.metric("Total permits", len(data))
metric_2.metric("Pending", int((data["status"] == "Pending").sum()))
metric_3.metric("Approved", int((data["status"] == "Approved").sum()))
metric_4.metric("Expiring <= 60 days", int((data["days_to_expiry"] <= 60).sum()))

with st.sidebar:
    st.header("Filters")
    states = st.multiselect("State", sorted(data["state"].dropna().unique()), default=["TX", "LA"])
    statuses = st.multiselect("Status", sorted(data["status"].dropna().unique()), default=sorted(data["status"].dropna().unique()))
    operators = st.multiselect(
        "Operator",
        sorted(data["operator"].dropna().unique()),
        default=sorted(data["operator"].dropna().unique()),
    )
    days_limit = st.slider("Expiry horizon (days)", min_value=0, max_value=365, value=60)

filtered = data[
    data["state"].isin(states)
    & data["status"].isin(statuses)
    & data["operator"].isin(operators)
].copy()

left, right = st.columns([1, 1])

with left:
    st.subheader("Permit status by state")
    if len(filtered) == 0:
        st.info("No records match current filters.")
    else:
        status_fig = px.histogram(
            filtered,
            x="state",
            color="status",
            barmode="group",
            title="Status Distribution",
        )
        st.plotly_chart(status_fig, use_container_width=True)

with right:
    st.subheader("Permits nearing expiration")
    expiring = filtered[filtered["days_to_expiry"] <= days_limit].sort_values("days_to_expiry")
    st.dataframe(
        expiring[
            [
                "permit_id",
                "state",
                "operator",
                "county_parish",
                "status",
                "expiration_date",
                "days_to_expiry",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Detailed permit list")
st.dataframe(
    filtered.sort_values(["state", "operator", "expiration_date"]),
    use_container_width=True,
    hide_index=True,
)

st.download_button(
    "Download filtered permits as CSV",
    data=filtered.to_csv(index=False).encode("utf-8"),
    file_name="filtered_permits.csv",
    mime="text/csv",
)

with st.expander("Suggested next steps for production"):
    st.markdown(
        """
1. Add automated ingestion from **Texas Railroad Commission** and **Louisiana SONRIS** exports/APIs.
2. Implement user authentication (Okta/Azure AD) and row-level access control by business unit.
3. Add daily alerting via email/Teams for permits expiring within policy windows.
4. Store permit history in a managed database (PostgreSQL/Snowflake/BigQuery).
"""
    )
