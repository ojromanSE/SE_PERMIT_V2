# TX/LA Drilling Permit Tracker (Streamlit)

A Streamlit app for oil & gas operators to monitor drilling permits in **Texas** and **Louisiana**.

## Features

- Pull live data from direct Texas RRC and Louisiana SONRIS export URLs
- Normalize varying source schemas into one permit model
- Filter by state, status, operator, and expiration horizon
- View status distribution chart and expiring permits table
- Download filtered permit portfolio as CSV
- Run scheduled ingestion into PostgreSQL/Snowflake-compatible targets

## Streamlit secrets for unattended refresh

Configure `.streamlit/secrets.toml`:

```toml
[permit_sources]
tx_rrc_export_url = "https://rrcsearch3.neubus.com/esd3-rrc/api/permit/drilling/export.csv"
la_sonris_export_url = "https://sonlite.dnr.state.la.us/sundown/cart_prod/cart_drillpermit.csv"
```

The app loads those values by default and you can override in the sidebar.

## Scheduled ingestion job (recommended)

Use `ingest_job.py` from cron, GitHub Actions, or a cloud scheduler.

Example env vars:

```bash
export TX_RRC_EXPORT_URL="https://rrcsearch3.neubus.com/esd3-rrc/api/permit/drilling/export.csv"
export LA_SONRIS_EXPORT_URL="https://sonlite.dnr.state.la.us/sundown/cart_prod/cart_drillpermit.csv"
export POSTGRES_URL="postgresql+psycopg2://user:pass@host:5432/db"
export PERMITS_TABLE="drilling_permits"
python ingest_job.py
```

For Snowflake, use a SQLAlchemy Snowflake connection string in `POSTGRES_URL` (name retained for compatibility).
## What changed

This version supports **automatic source pulls** (CSV or JSON endpoints), plus CSV upload and sample-data fallback.

## Features

- Pull live data from Texas and Louisiana source URLs
- Normalize varying source schemas into one permit model
- Validate required permit columns
- Filter by state, status, operator, and expiration horizon
- View status distribution chart and expiring permits table
- Download filtered permit portfolio as CSV

## Required normalized schema

The app standardizes incoming feeds to these columns:

- `permit_id`
- `state` (TX/LA)
- `operator`
- `county_parish`
- `well_name`
- `permit_type`
- `status`
- `application_date`
- `approval_date`
- `expiration_date`

## Configure automatic pulls

In Streamlit secrets, set feed URLs:

```toml
[permit_sources]
tx_url = "https://<texas-source>.csv"
la_url = "https://<louisiana-source>.csv"
```

You can also enter/override URLs in the sidebar at runtime.

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Deploy to Streamlit Community Cloud

1. Push this repo to GitHub.
2. In Streamlit Cloud, create a new app pointing to `app.py`.
3. Ensure `requirements.txt` is detected.
4. Add source URLs in Streamlit Secrets under `[permit_sources]`.
