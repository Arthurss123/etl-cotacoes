# 💱 ETL Pipeline — Cotações de Moedas

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?style=flat&logo=pandas&logoColor=white)

Pipeline de dados completo que ingere cotações de moedas em tempo real da [AwesomeAPI](https://docs.awesomeapi.com.br/api-de-moedas), transforma com Pandas e armazena no PostgreSQL — tudo orquestrado com Apache Airflow rodando em Docker Compose.

---

## 📐 Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
│                                                                 │
│   ┌──────────────┐     ┌──────────────────────────────────┐    │
│   │              │     │         Apache Airflow            │    │
│   │  AwesomeAPI  │────▶│                                  │    │
│   │  (externa)   │     │  ┌─────────┐  DAG: diária        │    │
│   │              │     │  │Scheduler│  00:00 UTC           │    │
│   └──────────────┘     │  └────┬────┘                     │    │
│                        │       │                           │    │
│                        │  ┌────▼─────────────────────┐    │    │
│                        │  │        ETL Pipeline        │    │    │
│                        │  │                           │    │    │
│                        │  │  [Extract] ──▶ [Transform]│    │    │
│                        │  │       └──────▶ [Load]     │    │    │
│                        │  │                           │    │    │
│                        │  └────────────┬──────────────┘    │    │
│                        │               │                   │    │
│                        └───────────────┼───────────────────┘    │
│                                        │                        │
│   ┌────────────────────┐   ┌───────────▼──────────────┐        │
│   │  postgres-airflow  │   │      postgres-data        │        │
│   │  (metadados do     │   │   banco: cotacoes_db      │        │
│   │   Airflow)         │   │   tabela: cotacoes        │        │
│   │   porta interna    │   │   porta: 5433             │        │
│   └────────────────────┘   └──────────────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

### Fluxo da DAG

```
start ──▶ extract_task ──▶ transform_task ──▶ load_task ──▶ notify_task ──▶ end
```

| Task | Descrição |
|---|---|
| `extract_task` | Requisição HTTP à AwesomeAPI — retorna JSON bruto de USD-BRL, EUR-BRL e BTC-BRL |
| `transform_task` | Limpeza, tipagem e normalização com Pandas. Converte timestamps Unix → datetime |
| `load_task` | INSERT direto no PostgreSQL via SQLAlchemy. Acumula histórico a cada execução |
| `notify_task` | Loga resumo da execução. Extensível para Slack/e-mail em produção |

---

## Estrutura do Projeto

```
etl-cotacoes/
├── dags/
│   └── cotacoes_dag.py       # DAG do Airflow — orquestra o pipeline
├── scripts/
│   ├── __init__.py
│   ├── extract.py            # Extração: requisição à AwesomeAPI
│   ├── transform.py          # Transformação: limpeza e tipagem com Pandas
│   └── load.py               # Carga: INSERT no PostgreSQL via SQLAlchemy
├── sql/
│   └── init.sql              # Criação da tabela cotacoes (roda no boot do banco)
├── logs/                     # Logs do Airflow (gerado em runtime)
├── plugins/                  # Plugins customizados do Airflow (reservado)
├── docker-compose.yml        # Infraestrutura completa: Airflow + 2x PostgreSQL
├── Dockerfile                # Imagem customizada do Airflow com dependências
├── requirements.txt          # Dependências Python do projeto
└── README.md
```

---

## Como Rodar

### Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e rodando
- Git

### 1. Clone o repositório

```bash
git clone https://github.com/Arthurss123/etl-cotacoes.git
cd etl-cotacoes
```

### 2. Crie o arquivo de ambiente

```bash
# Linux / Mac
echo "AIRFLOW_UID=$(id -u)" > .env

# Windows (PowerShell)
[System.IO.File]::WriteAllText(".env", "AIRFLOW_UID=50000`n", [System.Text.Encoding]::UTF8)
```

### 3. Suba a infraestrutura

```bash
docker compose up --build -d
```

> A primeira execução baixa as imagens e instala dependências — pode levar 3 a 5 minutos.

### 4. Acesse o Airflow

abra: **http://localhost:8080**

| Campo | Valor |
|---|---|
| Usuário | `admin` |
| Senha | `admin` |

### 5. Ative e dispare o pipeline

1. Ative o toggle da DAG `etl_cotacoes_moedas`
2. Clique em **▶ Trigger DAG** para disparar manualmente
3. Clique no nome da DAG → **Graph** para acompanhar em tempo real

### 6. Verifique os dados no banco

```bash
docker exec -it postgres-data psql -U etl_user -d cotacoes_db \
  -c "SELECT moeda_origem, bid, ask, pct_change, timestamp_api FROM cotacoes ORDER BY inserted_at DESC;"
```

Resultado esperado:

```
 moeda_origem |    bid     |    ask     | pct_change |    timestamp_api
--------------+------------+------------+------------+---------------------
 USD          |   5.167100 |   5.177100 |     2.1025 | 2026-06-05 21:30:21
 EUR          |   5.963030 |   5.977290 |     1.5504 | 2026-06-05 21:00:05
 BTC          | 320129.000 | 320130.000 |     3.0030 | 2026-06-07 04:31:54
```

---

## ⚙️ Configuração

### Moedas monitoradas

Edite a lista `CURRENCY_PAIRS` em `scripts/extract.py`:

```python
CURRENCY_PAIRS = ["USD-BRL", "EUR-BRL", "BTC-BRL"]
```

Qualquer par suportado pela [AwesomeAPI](https://docs.awesomeapi.com.br/api-de-moedas) pode ser adicionado (ex: `GBP-BRL`, `ARS-BRL`).

### Agendamento

Edite `schedule_interval` na DAG em `dags/cotacoes_dag.py`:

```python
schedule_interval="0 0 * * *"   # diário à meia-noite UTC (padrão)
schedule_interval="0 */6 * * *" # a cada 6 horas
schedule_interval="0 * * * *"   # a cada hora
```

### Variáveis de ambiente (banco de dados)

Configuradas no `docker-compose.yml` — altere conforme necessário:

| Variável | Padrão | Descrição |
|---|---|---|
| `POSTGRES_DATA_HOST` | `postgres-data` | Host do banco de dados |
| `POSTGRES_DATA_PORT` | `5432` | Porta interna |
| `POSTGRES_DATA_DB` | `cotacoes_db` | Nome do banco |
| `POSTGRES_DATA_USER` | `etl_user` | Usuário |
| `POSTGRES_DATA_PASSWORD` | `etl_password` | Senha |

---

## 🗄️ Esquema do Banco de Dados

```sql
CREATE TABLE cotacoes (
    id              SERIAL PRIMARY KEY,
    moeda_origem    VARCHAR(10)    NOT NULL,  -- ex: USD, EUR, BTC
    moeda_destino   VARCHAR(10)    NOT NULL,  -- ex: BRL
    bid             NUMERIC(18,6)  NOT NULL,  -- preço de compra
    ask             NUMERIC(18,6)  NOT NULL,  -- preço de venda
    high            NUMERIC(18,6)  NOT NULL,  -- máxima do dia
    low             NUMERIC(18,6)  NOT NULL,  -- mínima do dia
    pct_change      NUMERIC(8,4),             -- variação percentual
    timestamp_api   TIMESTAMP      NOT NULL,  -- horário da cotação na fonte
    inserted_at     TIMESTAMP      NOT NULL DEFAULT NOW()
);
```

---

## 🛠️ Tecnologias

| Tecnologia | Versão | Uso |
|---|---|---|
| Python | 3.11 | Linguagem principal |
| Apache Airflow | 2.9.1 | Orquestração do pipeline |
| PostgreSQL | 15 | Armazenamento dos dados |
| Pandas | 2.x | Transformação dos dados |
| SQLAlchemy | 2.x | Abstração do banco de dados |
| Docker Compose | — | Infraestrutura containerizada |

---

## 📋 Comandos Úteis

```bash
# Ver status dos containers
docker compose ps

# Ver logs do scheduler (execuções das DAGs)
docker compose logs airflow-scheduler --tail=50 -f

# Acessar o banco de dados diretamente
docker exec -it postgres-data psql -U etl_user -d cotacoes_db

# Parar o ambiente (preserva os dados)
docker compose down

# Parar e apagar todos os dados
docker compose down -v

# Rebuildar após mudanças no Dockerfile ou requirements.txt
docker compose up --build -d
```

---

## 📌 Próximos Passos

- [ ] Adicionar alertas via Slack/e-mail na `notify_task`
- [ ] Criar dashboard de visualização com Metabase ou Grafana
- [ ] Adicionar testes unitários para os scripts ETL
- [ ] Publicar imagem no Docker Hub
- [ ] Migrar para Airflow com CeleryExecutor para execução distribuída

---

## 👤 Autor

Feito por **Arthur** — [LinkedIn](https://www.linkedin.com/in/arthurmoura233/) · [GitHub](https://github.com/Arthurss123)