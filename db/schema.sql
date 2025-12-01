CREATE TYPE trade_side AS ENUM ('BUY', 'SELL');
CREATE TABLE trades (
    trade_id UUID PRIMARY KEY,
    portfolio_id UUID NOT NULL,
    symbol VARCHAR(255) NOT NULL,
    side VARCHAR(10) NOT NULL,
    price_per_stock  NUMERIC(19,4) NOT NULL,
    quantity BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE transactions (
    transaction_id      UUID PRIMARY KEY,
    trade_id            UUID NOT NULL,
    buy_price           NUMERIC(19,4),
    sell_price          NUMERIC(19,4),
    quantity            BIGINT NOT NULL,
    remaining_quantity  BIGINT NOT NULL,

    CONSTRAINT fk_trade
        FOREIGN KEY (trade_id)
        REFERENCES trades(trade_id)
        ON DELETE CASCADE
);

