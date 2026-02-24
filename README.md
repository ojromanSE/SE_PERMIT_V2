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

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```
