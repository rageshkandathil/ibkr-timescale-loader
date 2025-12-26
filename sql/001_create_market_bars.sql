CREATE TABLE IF NOT EXISTS market.market_bars (
  symbol        text        NOT NULL,
  timeframe     text        NOT NULL,
  ts            timestamptz NOT NULL,
  open          double precision,
  high          double precision,
  low           double precision,
  close         double precision,
  volume        double precision,
  wap           double precision,
  bar_count     integer,
  source        text        NOT NULL DEFAULT 'IBKR',
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, timeframe, ts)
);

SELECT create_hypertable('market.market_bars', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS ix_market_bars_symbol_ts
ON market.market_bars (symbol, ts DESC);
