# Imagem base oficial do Airflow — versão fixada para garantir reprodutibilidade
FROM apache/airflow:2.9.1-python3.11

# Muda para root para instalar dependências do sistema, se necessário
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Volta para o usuário padrão do Airflow (boa prática de segurança)
USER airflow

# Copia e instala as dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt