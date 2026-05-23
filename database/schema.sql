CREATE TABLE IF NOT EXISTS candles (
    time   TIMESTAMP        NOT NULL,
    symbol VARCHAR(50)      NOT NULL,
    open   DOUBLE PRECISION,
    high   DOUBLE PRECISION,
    low    DOUBLE PRECISION,
    close  DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    CONSTRAINT candles_time_symbol_unique UNIQUE (time, symbol)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('candles', 'time', if_not_exists => TRUE);
