from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import pandas as pd
import requests
from datetime import date

# --- Configurações da DAG ---
# ATENÇÃO: Substitua pelo ID do seu projeto no Google Cloud.
GCP_PROJECT   = "handy-diorama-470923-c0"
BQ_DATASET    = "dataset_fda"
BQ_TABLE      = "openfda_drug_events_test" # Tabela para os dados de medicamentos
BQ_LOCATION   = "US"
GCP_CONN_ID   = "google_cloud_default"
POOL_NAME     = "openfda_api"

# Período de teste que garantidamente terá dados de medicamentos
TEST_START = date(2024, 1, 1)
TEST_END   = date(2024, 12, 31)

# --- Funções Auxiliares ---
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "mda-openfda-etl/1.0 (contato: seu@email.com)"})

def _openfda_get(url: str) -> dict:
    r = SESSION.get(url, timeout=30)
    if r.status_code == 404:
        return {"results": []}
    r.raise_for_status()
    return r.json()

def _build_openfda_url(start: date, end: date) -> str:
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=receivedate:[{start_str}+TO+{end_str}]"
        "&count=receivedate"
    )

@task(pool=POOL_NAME, retries=0)
def fetch_drug_events_and_to_bq():
    print(f"Buscando contagem de eventos de medicamentos de {TEST_START} a {TEST_END}...")
    url = _build_openfda_url(TEST_START, TEST_END)
    data = _openfda_get(url)
    results = data.get("results", [])

    if not results:
        print("Nenhum evento de medicamento encontrado no período.")
        return

    print(f"Encontrados {len(results)} registros diários de eventos.")
    
    df = pd.DataFrame(results).rename(columns={"count": "events"})
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    df["win_start"] = pd.to_datetime(TEST_START)
    df["win_end"]   = pd.to_datetime(TEST_END)

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    
    print(f"Carregando {len(df)} registros para a tabela {BQ_DATASET}.{BQ_TABLE}...")
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=bq_hook.get_credentials(),
        table_schema=[
            {"name": "time", "type": "TIMESTAMP"},
            {"name": "events", "type": "INTEGER"},
            {"name": "win_start", "type": "DATE"},
            {"name": "win_end", "type": "DATE"},
        ],
        location=BQ_LOCATION,
        progress_bar=False
    )
    print("Carga para o BigQuery concluída com sucesso!")

@dag(
    dag_id="openfda_drug_events_test_range",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 26, tz="UTC"),
    catchup=False,
    tags=["openfda", "bigquery", "test", "drug"]
)
def openfda_drug_pipeline():
    fetch_drug_events_and_to_bq()

dag = openfda_drug_pipeline()
