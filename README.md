# Aviation Grievances India Analytics

This project provides an end-to-end data pipeline and analytics dashboard for aviation grievances in India. It ingests data from the official government API, loads it into BigQuery using [dlt](https://dlthub.com/), and visualizes key metrics and trends with an interactive [Streamlit](https://streamlit.io/) dashboard.

## Features

- **Automated Data Ingestion:** Fetches and standardizes grievance data daily from the government API.
- **BigQuery Integration:** Loads and partitions data efficiently for scalable analytics.
- **Interactive Dashboard:** Visualizes grievances by airline, type, resolution rates, satisfaction, and more.
- **Export & Reporting:** Download data and summary reports directly from the dashboard.

## Getting Started

1. **Install dependencies:**
   
   ```sh
   pip install -r requirements.txt
   ```

2. Configure secrets:

Add your API keys and BigQuery credentials to .dlt/secrets.toml and .streamlit/secrets.toml.

3. Run the data pipeline:
    ```bash
    python aviation_grievances.py
    ```

4. Launch the dashboard:
    ```bash
    streamlit run streamlit_app.py
    ```

## Project Main Components
- `aviation_grievances.py` — Data ingestion pipeline using dlt.
- `streamlit_app.py` — Streamlit dashboard for analytics.
- `.dlt/` and `.streamlit/` — Configuration and secrets
