"""
cotacoes_dag.py
---------------
DAG principal do pipeline ETL de cotações de moedas.

Fluxo:
    extract_task >> transform_task >> load_task >> notify_task

Agendamento: diário à meia-noite UTC (pode ser ajustado via variável DAG_SCHEDULE).
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Adiciona o diretório de scripts ao path para importar os módulos ETL
sys.path.insert(0, "/opt/airflow/scripts")

logger = logging.getLogger(__name__)

#configs padrões para dags
DEFAULT_ARGS = {
    "owner": "etl_team",
    "depends_on_past": False,          # cada execução é independente
    "email_on_failure": False,         # desabilitado (sem SMTP configurado)
    "email_on_retry": False,
    "retries": 3,                      # tenta 3x antes de marcar como falha
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True, # espera 5min, 10min, 20min entre tentativas
}



#funções que usa XCOM para passar dados entre si
def run_extract(**context) -> dict:
    """
    Task de extração: chama a API e empurra o resultado bruto via XCom.
    """
    from extract import extract_cotacoes

    logger.info("=== TASK: EXTRACT ===")
    raw_data = extract_cotacoes()

    # Empurra os dados para o XCom — a próxima task vai buscar por essa chave
    context["ti"].xcom_push(key="raw_data", value=raw_data)
    logger.info(f"Dados brutos empurrados ao XCom | chave: 'raw_data'")

    return raw_data


def run_transform(**context) -> list:
    """
    Task de transformação: puxa o JSON do XCom, transforma e empurra o
    resultado como lista de dicionários.
    """
    from transform import transform_cotacoes

    logger.info("=== TASK: TRANSFORM ===")

    # Puxa os dados da task anterior via XCom
    raw_data = context["ti"].xcom_pull(task_ids="extract_task", key="raw_data")

    if not raw_data:
        raise ValueError("XCom retornou vazio. A task de extração pode ter falhado.")

    df = transform_cotacoes(raw_data)

    # Converte para lista de dicts para serialização
    records = df.to_dict(orient="records")

    # Converte timestamps para string
    for record in records:
        if hasattr(record.get("timestamp_api"), "isoformat"):
            record["timestamp_api"] = record["timestamp_api"].isoformat()

    context["ti"].xcom_push(key="transformed_records", value=records)
    logger.info(f"{len(records)} registro(s) transformado(s) e empurrado(s) ao XCom.")

    return records


def run_load(**context) -> int:
    """
    Task de carga: puxa os registros transformados do XCom,
    reconstrói o DataFrame e carrega no PostgreSQL.
    """
    import pandas as pd
    from load import load_cotacoes

    logger.info("=== TASK: LOAD ===")

    records = context["ti"].xcom_pull(
        task_ids="transform_task", key="transformed_records"
    )

    if not records:
        raise ValueError("XCom retornou vazio.")

    # Reconstrói o DataFrame a partir da lista de dicts
    df = pd.DataFrame(records)

    # Restaura o tipo datetime
    df["timestamp_api"] = pd.to_datetime(df["timestamp_api"])

    inserted = load_cotacoes(df)
    logger.info(f"=== PIPELINE CONCLUÍDO | {inserted} registro(s) inserido(s) ===")

    return inserted


def run_notify(**context):
    """
    Task de notificação: loga um resumo da execução.
    Em produção, aqui entraria um envio de e-mail, ou algum tipo de comunicação caso tivesse um time especializado para isso.
    """
    inserted = context["ti"].xcom_pull(task_ids="load_task")
    execution_date = context["execution_date"]

    logger.info("=" * 50)
    logger.info("    PIPELINE ETL FINALIZADO COM SUCESSO")
    logger.info(f"   Data de execução : {execution_date}")
    logger.info(f"   Registros salvos : {inserted}")
    logger.info("=" * 50)

#definições basicas da dag
with DAG(
    dag_id="etl_cotacoes_moedas",
    description="Pipeline ETL: AwesomeAPI → Pandas → PostgreSQL",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * *", #sempre meia noite
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "cotacoes", "awesomeapi", "postgres"],
) as dag:

    # Task de início apenas visual para usar no grafo
    start = EmptyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=run_extract,
        doc_md="""
        ### Extract
        Faz requisição à AwesomeAPI e retorna cotações brutas de USD-BRL, EUR-BRL e BTC-BRL.
        """,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=run_transform,
        doc_md="""
        ### Transform
        Limpa, tipifica e normaliza os dados brutos com Pandas.
        Descarta registros inválidos e converte timestamps.
        """,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=run_load,
        doc_md="""
        ### Load
        Insere os registros transformados na tabela `cotacoes` do PostgreSQL.
        Usa append
        """,
    )

    notify_task = PythonOperator(
        task_id="notify_task",
        python_callable=run_notify,
        doc_md="""
        ### Notify
        Loga resumo da execução. Extensível para Slack/e-mail em produção.
        """,
    )

    # Task de fim apeas visual para o grafo
    end = EmptyOperator(task_id="end")

    #aqui temos o fluxo de acionamento da dag
    start >> extract_task >> transform_task >> load_task >> notify_task >> end