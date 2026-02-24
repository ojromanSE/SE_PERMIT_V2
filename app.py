import io
from datetime import date, timedelta
from typing import Any

import pandas as pd
import plotly.express as px
import requests
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

# Public export endpoints can be overridden in Streamlit secrets.
DEFAULT_TX_RRC_EXPORT = "https://rrcsearch3.neubus.com/esd3-rrc/api/permit/drilling/export.csv"
DEFAULT_LA_SONRIS_EXPORT = "https://sonlite.dnr.state.la.us/sundown/cart_prod/cart_drillpermit.csv"
SOURCE_DEFAULTS = {
    "tx_url": "",
    "la_url": "",
}

COLUMN_ALIASES = {
    "permit_id": ["permit_id", "permit_number", "permit_no", "api_number", "api"],
    "state": ["state", "jurisdiction"],
    "operator": ["operator", "operator_name", "company", "organization"],
    "county_parish": ["county_parish", "county", "parish", "location"],
    "well_name": ["well_name", "well", "wellbore_name", "lease_well_name"],
    "permit_type": ["permit_type", "well_type", "drill_type", "permit_category"],
    "status": ["status", "permit_status", "approval_status"],
    "application_date": ["application_date", "application_dt", "filed_date", "submitted_date"],
    "approval_date": ["approval_date", "approved_date", "approval_dt", "issue_date"],
    "expiration_date": ["expiration_date", "expiry_date", "expiration_dt", "expires_on"],
}


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


def _find_alias_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lookup = {c.lower().strip(): c for c in df.columns}
    for candidate in candidates:
        key = candidate.lower().strip()
        if key in lookup:
            return lookup[key]
    return None


def harmonize_schema(df: pd.DataFrame, fallback_state: str) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for target_col, aliases in COLUMN_ALIASES.items():
        source_col = _find_alias_column(df, aliases)
        out[target_col] = df[source_col] if source_col is not None else pd.NA

    out["state"] = out["state"].fillna(fallback_state)
    out["status"] = out["status"].fillna("Pending")
    out["permit_type"] = out["permit_type"].fillna("Unknown")
    out["operator"] = out["operator"].fillna("Unknown Operator")
    out["county_parish"] = out["county_parish"].fillna("Unknown")
    out["well_name"] = out["well_name"].fillna("Unknown Well")

    if out["permit_id"].isna().all():
        out["permit_id"] = [f"{fallback_state}-AUTO-{i+1:06d}" for i in range(len(out))]

    return out


def _response_to_dataframe(content: bytes, content_type: str) -> pd.DataFrame:
    if "json" in content_type:
        payload: Any = requests.models.complexjson.loads(content.decode("utf-8"))
        if isinstance(payload, dict):
            for key in ["data", "results", "features", "items"]:
                if key in payload:
                    payload = payload[key]
                    break
        if isinstance(payload, list):
            return pd.json_normalize(payload)
        return pd.DataFrame(payload)
    return pd.read_csv(io.BytesIO(content))


@st.cache_data(ttl=60 * 60)
def fetch_remote_permits(url: str, state: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type", "").lower()
    raw = _response_to_dataframe(resp.content, content_type)
    return harmonize_schema(raw, fallback_state=state)


def validate_dataframe(df: pd.DataFrame) -> tuple[bool, list[str]]:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    return (len(missing) == 0, missing)


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["application_date", "approval_date", "expiration_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def load_live_data(tx_url: str, la_url: str) -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    issues: list[str] = []

    for state, url in [("TX", tx_url), ("LA", la_url)]:
        if not url:
            issues.append(f"{state} source URL is empty.")
            continue
        try:
            frames.append(fetch_remote_permits(url, state))
        except Exception as exc:  # noqa: BLE001
            issues.append(f"{state} fetch failed: {exc}")

    if not frames:
        return pd.DataFrame(columns=REQUIRED_COLUMNS), issues

    data = pd.concat(frames, ignore_index=True)
    return data, issues


st.title("Texas + Louisiana Drilling Permit Tracker")
st.caption("Automatic RRC/SONRIS pulls + upload fallback for drilling permit surveillance.")

secrets_defaults = st.secrets.get("permit_sources", {}) if hasattr(st, "secrets") else {}
tx_default = secrets_defaults.get("tx_rrc_export_url", DEFAULT_TX_RRC_EXPORT)
la_default = secrets_defaults.get("la_sonris_export_url", DEFAULT_LA_SONRIS_EXPORT)
if tx_url:
        try:
            frames.append(fetch_remote_permits(tx_url, "TX"))
        except Exception as exc:  # noqa: BLE001
            issues.append(f"Texas fetch failed: {exc}")
        else:
            issues.append("Texas source URL is empty.")

if la_url:
        try:
            frames.append(fetch_remote_permits(la_url, "LA"))
        except Exception as exc:  # noqa: BLE001
            issues.append(f"Louisiana fetch failed: {exc}")
        else:
            issues.append("Louisiana source URL is empty.")

if not frames:
        return (pd.DataFrame(columns=REQUIRED_COLUMNS), issues)

        data = pd.concat(frames, ignore_index=True)
        return (data, issues)


st.title("Texas + Louisiana Drilling Permit Tracker")
st.caption(
    "Track, filter, and prioritize drilling permits for operators across TX and LA. "
    "Use automatic source pulls, upload your export, or start with sample data."
)

with st.sidebar:
    st.header("Data source")
    mode = st.radio("Select mode", ["Live source pull", "Upload CSV", "Sample data"], index=0)

    tx_url = st.text_input("Texas RRC export URL", value=tx_default)
    la_url = st.text_input("Louisiana SONRIS export URL", value=la_default)
    st.caption("Store these in Streamlit secrets under [permit_sources] for unattended refresh.")
    refresh = st.button("Refresh live data")
    secrets_defaults = st.secrets.get("permit_sources", {}) if hasattr(st, "secrets") else {}
    tx_default = secrets_defaults.get("tx_url", SOURCE_DEFAULTS["tx_url"])
    la_default = secrets_defaults.get("la_url", SOURCE_DEFAULTS["la_url"])

    tx_url = st.text_input("Texas source URL (CSV/JSON)", value=tx_default)
    la_url = st.text_input("Louisiana source URL (CSV/JSON)", value=la_default)
    st.caption("Tip: Put permanent source URLs in `.streamlit/secrets.toml` under `[permit_sources]`.")
    refresh = st.button("Refresh live data")

    uploaded = st.file_uploader("Upload permit CSV", type=["csv"])

if refresh:
    fetch_remote_permits.clear()

issues: list[str] = []
if mode == "Live source pull":
    data, issues = load_live_data(tx_url=tx_url.strip(), la_url=la_url.strip())
    if data.empty:
        st.warning("Live pull returned no rows. Falling back to sample data.")
        data = load_sample_data()
elif mode == "Upload CSV" and uploaded is not None:
    data = pd.read_csv(io.BytesIO(uploaded.getvalue()))
else:
    data = load_sample_data()

for issue in issues:
    st.warning(issue)

data = normalize_dates(data)
valid, missing_cols = validate_dataframe(data)
if not valid:
    st.error(
        "Loaded data is missing required columns: "
        + ", ".join(missing_cols)
        + ".\n\nExpected columns: "
        + ", ".join(REQUIRED_COLUMNS)
    )
    st.stop()

data = normalize_dates(data)

today = pd.Timestamp.today().normalize()
data["days_to_expiry"] = (data["expiration_date"] - today).dt.days

st.subheader("Portfolio at a glance")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total permits", len(data))
col2.metric("Pending", int((data["status"].astype(str).str.lower() == "pending").sum()))
col3.metric("Approved", int((data["status"].astype(str).str.lower() == "approved").sum()))
col4.metric("Expiring <= 60 days", int((data["days_to_expiry"] <= 60).sum()))
metric_1, metric_2, metric_3, metric_4 = st.columns(4)
metric_1.metric("Total permits", len(data))
metric_2.metric("Pending", int((data["status"].astype(str).str.lower() == "pending").sum()))
metric_3.metric("Approved", int((data["status"].astype(str).str.lower() == "approved").sum()))
metric_4.metric("Expiring <= 60 days", int((data["days_to_expiry"] <= 60).sum()))

with st.sidebar:
    st.header("Filters")
    states = st.multiselect("State", sorted(data["state"].dropna().astype(str).unique()), default=sorted(data["state"].dropna().astype(str).unique()))
    statuses = st.multiselect("Status", sorted(data["status"].dropna().astype(str).unique()), default=sorted(data["status"].dropna().astype(str).unique()))
    operators = st.multiselect("Operator", sorted(data["operator"].dropna().astype(str).unique()), default=sorted(data["operator"].dropna().astype(str).unique()))
    operators = st.multiselect(
        "Operator",
        sorted(data["operator"].dropna().astype(str).unique()),
        default=sorted(data["operator"].dropna().astype(str).unique()),
    )
    days_limit = st.slider("Expiry horizon (days)", min_value=0, max_value=365, value=60)

filtered = data[
    data["state"].astype(str).isin(states)
    & data["status"].astype(str).isin(statuses)
    & data["operator"].astype(str).isin(operators)
].copy()

left, right = st.columns(2)
left, right = st.columns([1, 1])

with left:
    st.subheader("Permit status by state")
    if filtered.empty:
        st.info("No records match current filters.")
    else:
        st.plotly_chart(px.histogram(filtered, x="state", color="status", barmode="group"), use_container_width=True)
        status_fig = px.histogram(filtered, x="state", color="status", barmode="group", title="Status Distribution")
        st.plotly_chart(status_fig, use_container_width=True)

with right:
    st.subheader("Permits nearing expiration")
    expiring = filtered[filtered["days_to_expiry"] <= days_limit].sort_values("days_to_expiry")
    st.dataframe(expiring[["permit_id", "state", "operator", "county_parish", "status", "expiration_date", "days_to_expiry"]], use_container_width=True, hide_index=True)

st.subheader("Detailed permit list")
st.dataframe(filtered.sort_values(["state", "operator", "expiration_date"]), use_container_width=True, hide_index=True)
st.download_button("Download filtered permits as CSV", data=filtered.to_csv(index=False).encode("utf-8"), file_name="filtered_permits.csv", mime="text/csv")

with st.expander("Operational notes"):
    st.markdown(
        """
- Direct source URLs are loaded from `.streamlit/secrets.toml` keys:
  - `permit_sources.tx_rrc_export_url`
  - `permit_sources.la_sonris_export_url`
- Use `ingest_job.py` in a scheduler (cron/GitHub Actions/Cloud Run Job) to persist normalized rows into PostgreSQL or Snowflake.
    st.dataframe(
        expiring[["permit_id", "state", "operator", "county_parish", "status", "expiration_date", "days_to_expiry"]],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Detailed permit list")
st.dataframe(filtered.sort_values(["state", "operator", "expiration_date"]), use_container_width=True, hide_index=True)

st.download_button(
    "Download filtered permits as CSV",
    data=filtered.to_csv(index=False).encode("utf-8"),
    file_name="filtered_permits.csv",
    mime="text/csv",
)

with st.expander("Suggested next steps for production"):
    st.markdown(
        """
1. Connect **direct Texas RRC and Louisiana SONRIS export URLs** in Streamlit secrets for unattended refreshes.
2. Add scheduled ingestion jobs and store normalized records in PostgreSQL/Snowflake.
3. Implement SSO (Okta/Azure AD) and business-unit level access controls.
4. Add daily Teams/email alerts for permits nearing expiration.
"""
    )
