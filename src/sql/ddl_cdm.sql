-- ============================================================
-- CDM layer: витрина расчётов с курьерами
-- ============================================================

CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id                     serial4 PRIMARY KEY,
    courier_id             varchar NOT NULL,
    courier_name           varchar NOT NULL,
    settlement_year        smallint NOT NULL,
    settlement_month       smallint NOT NULL,
    orders_count           integer NOT NULL DEFAULT 0,
    orders_total_sum       numeric(14,2) NOT NULL DEFAULT 0,
    rate_avg               numeric(4,2) NOT NULL DEFAULT 0,
    order_processing_fee   numeric(14,2) NOT NULL DEFAULT 0,
    courier_order_sum      numeric(14,2) NOT NULL DEFAULT 0,
    courier_tips_sum       numeric(14,2) NOT NULL DEFAULT 0,
    courier_reward_sum     numeric(14,2) NOT NULL DEFAULT 0,
    CONSTRAINT dm_courier_ledger_month_check CHECK (settlement_month BETWEEN 1 AND 12),
    CONSTRAINT dm_courier_ledger_year_check  CHECK (settlement_year BETWEEN 2020 AND 2100),
    CONSTRAINT dm_courier_ledger_uindex UNIQUE (courier_id, settlement_year, settlement_month)
);
