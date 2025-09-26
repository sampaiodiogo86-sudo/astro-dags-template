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
BQ_TABLE      = "openfda_food_recall_reason_test"
BQ_LOCATION   = "US" # Ou a localização do seu dataset
GCP_CONN_ID   = "google_cloud_default"
USE_POOL      = True
POOL_NAME     = "openfda_api"

# Período fixo para o teste
TEST_START = date(2025, 6, 1)
TEST_END   = date(2025, 7, 29)

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
    """Constrói a URL para buscar a contagem de recalls de alimentos por motivo."""
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    return (
        "https://api.fda.gov/food/enforcement.json"
        f"?search=recall_initiation_date:[{start_str}+TO+{end_str}]"
        "&count=reason_for_recall.exact"
    )

# --- Definição da Task ---
_task_kwargs = dict(retries=0)
if USE_POOL:
    _task_kwargs["pool"] = POOL_NAME

@task(**_task_kwargs)
def fetch_aggregated_reasons_and_to_bq():
    """Busca dados agregados da API OpenFDA e os carrega no BigQuery."""
    print(f"Buscando contagem de recalls por motivo de {TEST_START} a {TEST_END}...")
    url = _build_openfda_url(TEST_START, TEST_END)
    data = _openfda_get(url)
    results = data.get("results", [])

    if not results:
        print("Nenhum recall encontrado no período. A tarefa será concluída sem carregar dados.")
        return

    print(f"Encontrados {len(results)} motivos de recall distintos.")
    
    # Converte a resposta JSON para um DataFrame do Pandas
    df = pd.DataFrame(results).rename(columns={
        "term": "reason_for_recall",
        "count": "number_of_recalls"
    })
    
    # Adiciona colunas de contexto para registrar o período da busca
    df["win_start"] = pd.to_datetime(TEST_START)
    df["win_end"]   = pd.to_datetime(TEST_END)

    # Carrega o DataFrame no BigQuery
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    
    print(f"Carregando {len(df)} registros para a tabela {BQ_DATASET}.{BQ_TABLE}...")
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append", # Adiciona os dados. Em produção, considere 'replace' ou uma lógica de MERGE.
        credentials=bq_hook.get_credentials(),
        table_schema=[
            {"name": "reason_for_recall", "type": "STRING"},
            {"name": "number_of_recalls", "type": "INTEGER"},
            {"name": "win_start", "type": "DATE"},
            {"name": "win_end", "type": "DATE"},
        ],
        location=BQ_LOCATION,
        progress_bar=False
    )
    print("Carga para o BigQuery concluída com sucesso!")

# --- Definição da DAG ---
@dag(
    dag_id="openfda_food_recall_reason_test_range",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "bigquery", "test", "food", "recall"]
)
def openfda_food_pipeline_test_range():
    """DAG para um teste pontual de extração de motivos de recall e carga no BigQuery."""
    fetch_aggregated_reasons_and_to_bq()

# Instancia a DAG para que o Airflow a reconheça
dag = openfda_food_pipeline_test_range()
