from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import pandas as pd
import requests
from datetime import date

# --- Configurações da DAG ---
# ATENÇÃO: Substitua pelo ID do seu projeto no Google Cloud.
GCP_PROJECT   = "handy-diorama-470923-c0="
BQ_DATASET    = "dataset_fda"
BQ_TABLE      = "openfda_device_events_test" # Nova tabela para os dados de dispositivos
BQ_LOCATION   = "US" # Ou a localização do seu dataset
GCP_CONN_ID   = "google_cloud_default"
USE_POOL      = True
POOL_NAME     = "openfda_api" # Reutilizando o mesmo pool que já criamos

# Período de teste que garantidamente terá dados de dispositivos
TEST_START = date(2024, 1, 1)
TEST_END   = date(2024, 1, 31) # Apenas um mês para o teste ser rápido

# --- Funções Auxiliares ---
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "mda-openfda-etl/1.0 (contato: seu@email.com)"})

def _openfda_get(url: str) -> dict:
    """Faz uma requisição GET para a API, tratando o erro 404 como 'sem resultados'."""
    r = SESSION.get(url, timeout=30)
    if r.status_code == 404:
        return {"results": []}
    r.raise_for_status()
    return r.json()

def _build_openfda_url(start: date, end: date) -> str:
    """Constrói a URL para buscar a contagem de eventos de dispositivos médicos por dia."""
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Ym%d")
    return (
        "https://api.fda.gov/device/event.json"
        f"?search=date_received:[{start_str}+TO+{end_str}]" # Campo de data para dispositivos é 'date_received'
        "&count=date_received" # Contando pelo mesmo campo de data
    )

@task(pool=POOL_NAME if USE_POOL else None, retries=0)
def fetch_device_events_and_to_bq():
    """Busca dados agregados de eventos de dispositivos e os carrega no BigQuery."""
    print(f"Buscando contagem de eventos de dispositivos de {TEST_START} a {TEST_END}...")
    url = _build_openfda_url(TEST_START, TEST_END)
    data = _openfda_get(url)
    results = data.get("results", [])

    if not results:
        print("Nenhum evento de dispositivo encontrado no período.")
        return

    print(f"Encontrados {len(results)} registros diários de eventos.")
    
    # Converte a resposta JSON para um DataFrame do Pandas
    df = pd.DataFrame(results).rename(columns={"count": "events"})
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    
    # Adiciona colunas de contexto para registrar o período da busca
    df["win_start"] = pd.to_datetime(TEST_START)
    df["win_end"]   = pd.to_datetime(TEST_END)

    # Carrega o DataFrame no BigQuery
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

# --- Definição da DAG ---
@dag(
    dag_id="openfda_device_events_test_range", # Novo DAG ID
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 26, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "bigquery", "test", "device"] # Tag atualizada
)
def openfda_device_pipeline():
    """DAG para um teste pontual de extração de eventos de dispositivos e carga no BigQuery."""
    fetch_device_events_and_to_bq()

# Instancia a DAG para que o Airflow a reconheça
dag = openfda_device_pipeline()
