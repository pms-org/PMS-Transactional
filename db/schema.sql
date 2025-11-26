CREATE TYPE trade_side AS ENUM ('BUY', 'SELL');
CREATE TABLE trades (
    trade_id UUID PRIMARY KEY,
    portfolio_id UUID,
    cusip_id VARCHAR(255),
    side trade_side,
    unit_price FLOAT,
    cusip_name VARCHAR(255),
    quantity BIGINT,
    timestamp TIMESTAMPTZ

);

CREATE TABLE transactions (
    transactionId UUID PRIMARY KEY,
    trade_id UUID REFERENCES trades(trade_id),
    portfolio_id UUID,
    cusip_id VARCHAR(100),
    sector VARCHAR(100),
    side trade_side,
    buy_price FLOAT,
    sell_price FLOAT,
    quantity BIGINT,
    timestamp TIMESTAMPTZ,
    remaining_quantity BIGINT
);
