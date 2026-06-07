"""
transform.py
------------
Responsabilidade: receber o JSON bruto da API e devolver um DataFrame
pandas limpo, tipado e pronto para ser carregado no PostgreSQL.
"""

import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

# Mapeamento entre campos da API e colunas do banco
FIELD_MAP = {
    "code":       "moeda_origem",
    "codein":     "moeda_destino",
    "bid":        "bid",
    "ask":        "ask",
    "high":       "high",
    "low":        "low",
    "pctChange":  "pct_change",
    "timestamp":  "timestamp_api",
}


def transform_cotacoes(raw_data: dict) -> pd.DataFrame:
    """
    Transforma o JSON bruto da AwesomeAPI em um DataFrame normalizado.
    """
    logger.info("Iniciando transform nos dados recebidos.")

    #Achata o dicionário aninhado em lista de registros
    records = list(raw_data.values())
    df = pd.DataFrame(records)

    logger.info(f"Registros recebidos: {len(df)}")

    #Seleciona apenas os campos mapeados e renomeia as colunas
    df = df[list(FIELD_MAP.keys())].rename(columns=FIELD_MAP)

    #Converte colunas numéricas de string para float
    numeric_cols = ["bid", "ask", "high", "low", "pct_change"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    #Converte timestamp para datetime com timezone UTC
    df["timestamp_api"] = pd.to_datetime(
        df["timestamp_api"].astype(int), unit="s", utc=True
    ).dt.tz_localize(None)  # remove tzinfo para compatibilidade com PostgreSQL

    #Remove linhas onde valores críticos são nulos
    before = len(df)
    df = df.dropna(subset=["bid", "ask", "timestamp_api"])
    dropped = before - len(df)

    if dropped > 0:
        logger.warning(f"{dropped} registro(s) descartado(s).")

    if df.empty:
        raise ValueError("Nenhum registro válido após a transformação.")

    logger.info(f"Transformação feita | {len(df)} registros válidos.")
    logger.debug(f"\n{df.to_string()}")

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    from extract import extract_cotacoes
    raw = extract_cotacoes()
    df = transform_cotacoes(raw)
    print(df.dtypes)
    print(df)