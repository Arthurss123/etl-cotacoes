"""
extract.py
----------
Responsabilidade: buscar dados brutos da AwesomeAPI e retorná-los como dicionário.
Nenhuma transformação acontece aqui — só requisição e validação básica.
"""

import logging
import requests

# Moedas que serão monitoradas
CURRENCY_PAIRS = ["USD-BRL", "EUR-BRL", "BTC-BRL"]

API_URL = "https://economia.awesomeapi.com.br/json/last/{pairs}"

logger = logging.getLogger(__name__)


def extract_cotacoes() -> dict:
    """
    Faz requisição à AwesomeAPI e retorna o JSON bruto das cotações.
    """
    pairs_str = ",".join(CURRENCY_PAIRS)
    url = API_URL.format(pairs=pairs_str)

    logger.info(f"Iniciando extração | URL: {url}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Timeout ao conectar com API.")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Sem conexão com API.")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro HTTP da API: {e.response.status_code} — {e.response.text}")
        raise

    data = response.json()

    if not data:
        raise ValueError("A API retornou uma resposta vazia.")

    logger.info(f"Extração feita | {len(data)} recebidos: {list(data.keys())}")
    return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    resultado = extract_cotacoes()
    import json
    print(json.dumps(resultado, indent=2, ensure_ascii=False))