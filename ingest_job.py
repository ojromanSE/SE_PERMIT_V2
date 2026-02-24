import io
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests
from sqlalchemy import create_engine

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


def _find_alias_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lookup = {c.lower().strip(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower().strip() in lookup:
            return lookup[candidate.lower().strip()]
    return None


def harmonize_schema(df: pd.DataFrame, fallback_state: str) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for target_col, aliases in COLUMN_ALIASES.items():
        source = _find_alias_column(df, aliases)
        out[target_col] = df[source] if source is not None else pd.NA
    out["state"] = out["state"].fillna(fallback_state)
    for col, value in {
        "status": "Pending",
        "permit_type": "Unknown",
        "operator": "Unknown Operator",
        "county_parish": "Unknown",
        "well_name": "Unknown Well",
    }.items():
        out[col] = out[col].fillna(value)
    if out["permit_id"].isna().all():
        out["permit_id"] = [f"{fallback_state}-AUTO-{i+1:06d}" for i in range(len(out))]
    for col in ["application_date", "approval_date", "expiration_date"]:
        out[col] = pd.to_datetime(out[col], errors="coerce")
    out["ingested_at_utc"] = datetime.now(timezone.utc)
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


def fetch_remote(url: str, state: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return harmonize_schema(_response_to_dataframe(resp.content, resp.headers.get("Content-Type", "").lower()), state)


def main() -> None:
    tx_url = os.getenv("TX_RRC_EXPORT_URL", "")
    la_url = os.getenv("LA_SONRIS_EXPORT_URL", "")
    db_url = os.getenv("POSTGRES_URL", "")
    table_name = os.getenv("PERMITS_TABLE", "drilling_permits")

    if not tx_url or not la_url:
        raise ValueError("Set TX_RRC_EXPORT_URL and LA_SONRIS_EXPORT_URL")
    if not db_url:
        raise ValueError("Set POSTGRES_URL for persistence target")

    data = pd.concat([fetch_remote(tx_url, "TX"), fetch_remote(la_url, "LA")], ignore_index=True)
    missing = [c for c in REQUIRED_COLUMNS if c not in data.columns]
    if missing:
        raise ValueError(f"Missing required columns after harmonization: {missing}")

    engine = create_engine(db_url)
    data.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"Persisted {len(data)} rows to {table_name}")


if __name__ == "__main__":
    main()
