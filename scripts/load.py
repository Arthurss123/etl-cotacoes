"""
load.py
-------
Responsabilidade: receber o DataFrame transformado e carregá-lo
no PostgreSQL de forma segura, usando SQLAlchemy puro.
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

logger = logging.getLogger(__name__)


def _get_connection_string() -> str:
    host     = os.environ.get("POSTGRES_DATA_HOST", "localhost")
    port     = os.environ.get("POSTGRES_DATA_PORT", "5432")
    db       = os.environ.get("POSTGRES_DATA_DB", "cotacoes_db")
    user     = os.environ.get("POSTGRES_DATA_USER", "etl_user")
    password = os.environ.get("POSTGRES_DATA_PASSWORD", "etl_password")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def load_cotacoes(df: pd.DataFrame) -> int:
    """
    Carrega o DataFrame no PostgreSQL, na tabela `cotacoes`.

    Usa INSERT direto via SQLAlchemy para máxima compatibilidade entre versões.
    Acumula histórico a cada execução (não sobrescreve dados anteriores).
    """
    if df.empty:
        logger.warning("DataFrame vazio")
        return 0

    conn_str = _get_connection_string()
    logger.info(f"Postgres | host: {os.environ.get('POSTGRES_DATA_HOST', 'localhost')}")

    engine = None
    try:
        engine = create_engine(conn_str, pool_pre_ping=True)

        # Converte o DataFrame para lista de dicionários para inserção manual
        records = df.to_dict(orient="records")

        insert_sql = text("""
            INSERT INTO cotacoes (
                moeda_origem, moeda_destino,
                bid, ask, high, low, pct_change,
                timestamp_api
            ) VALUES (
                :moeda_origem, :moeda_destino,
                :bid, :ask, :high, :low, :pct_change,
                :timestamp_api
            )
        """)

        with engine.begin() as conn:
            conn.execute(insert_sql, records)

        rows_inserted = len(records)
        logger.info(f"Carga feita | {rows_inserted} registro(s) inserido(s) na tabela 'cotacoes'.")
        return rows_inserted

    except OperationalError as e:
        logger.error(f"Falha de connect com o PostgreSQL: {e}")
        raise
    except SQLAlchemyError as e:
        logger.error(f"Erro ao inserir os dados: {e}")
        raise
    finally:
        if engine:
            engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    from extract import extract_cotacoes
    from transform import transform_cotacoes

    raw = extract_cotacoes()
    df = transform_cotacoes(raw)
    inserted = load_cotacoes(df)
    print(f"\n {inserted} registros inseridos")