-- Script de inicialização do banco de dados
-- Executado automaticamente pelo PostgreSQL na primeira vez que o container sobe

-- Tabela principal que armazena as cotações extraídas da API
CREATE TABLE IF NOT EXISTS cotacoes (
    id              SERIAL PRIMARY KEY,

    -- Identificação da moeda (ex: USD, EUR, BTC)
    moeda_origem    VARCHAR(10)    NOT NULL,
    moeda_destino   VARCHAR(10)    NOT NULL,

    -- Valores da cotação
    bid             NUMERIC(18, 6) NOT NULL,  -- preço de compra
    ask             NUMERIC(18, 6) NOT NULL,  -- preço de venda
    high            NUMERIC(18, 6) NOT NULL,  -- máxima do dia
    low             NUMERIC(18, 6) NOT NULL,  -- mínima do dia
    pct_change      NUMERIC(8, 4),            -- variação percentual

    -- Timestamps
    timestamp_api   TIMESTAMP      NOT NULL,  -- horário da cotação na fonte
    inserted_at     TIMESTAMP      NOT NULL DEFAULT NOW()  -- horário da inserção no banco
);

-- Índice para acelerar consultas por moeda e data
CREATE INDEX IF NOT EXISTS idx_cotacoes_moeda
    ON cotacoes (moeda_origem, moeda_destino);

CREATE INDEX IF NOT EXISTS idx_cotacoes_timestamp
    ON cotacoes (timestamp_api DESC);

-- Comentários nas colunas (boa prática para documentação do banco)
COMMENT ON TABLE cotacoes IS 'Cotações de moedas extraídas da AwesomeAPI via pipeline ETL';
COMMENT ON COLUMN cotacoes.bid IS 'Preço de compra da moeda';
COMMENT ON COLUMN cotacoes.ask IS 'Preço de venda da moeda';
COMMENT ON COLUMN cotacoes.timestamp_api IS 'Timestamp original retornado pela API';
COMMENT ON COLUMN cotacoes.inserted_at IS 'Momento em que o registro foi inserido pelo pipeline';