# TX/LA Drilling Permit Tracker (Streamlit)

A lightweight Streamlit app for oil & gas operators to monitor drilling permits in **Texas** and **Louisiana**.

## Features

- Load permit data from CSV upload or built-in sample dataset
- Validate required permit columns
- Filter by state, status, operator, and expiration horizon
- View status distribution chart and expiring permits table
- Download filtered permit portfolio as CSV

## Required CSV schema

Your upload should include these columns:

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

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Deploy to Streamlit Community Cloud

1. Push this repo to GitHub.
2. In Streamlit Cloud, create a new app pointing to `app.py`.
3. Ensure `requirements.txt` is detected.
4. Optionally add secrets in `.streamlit/secrets.toml` for future API/database integration.
