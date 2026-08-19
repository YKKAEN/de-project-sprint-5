-- ============================================================
-- DDS layer: модель "снежинка" для подсистемы доставки
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dds;

-- Справочник курьеров (НОВАЯ таблица)
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id            serial4 PRIMARY KEY,
    courier_id    varchar NOT NULL,   -- бизнес-ключ, _id курьера из API
    courier_name  varchar NOT NULL,
    CONSTRAINT dm_couriers_courier_id_uindex UNIQUE (courier_id)
);

-- Факт доставки (НОВАЯ таблица)
CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
    id            serial4 PRIMARY KEY,
    delivery_id   varchar NOT NULL,                       -- бизнес-ключ доставки
    order_id      varchar NOT NULL,                        -- бизнес-ключ заказа (== dds.dm_orders.order_key)
    order_ts      timestamp NOT NULL,                       -- дата заказа — по НЕЙ считаем отчётный период
    courier_id    integer NOT NULL REFERENCES dds.dm_couriers(id),
    address       varchar NOT NULL,
    delivery_ts   timestamp NOT NULL,
    rate          smallint NOT NULL,
    tip_sum       numeric(14,2) NOT NULL DEFAULT 0,
    sum           numeric(14,2) NOT NULL DEFAULT 0,        -- сумма заказа по данным API, справочно (не используется в витрине)
    CONSTRAINT dm_deliveries_delivery_id_uindex UNIQUE (delivery_id),
    CONSTRAINT dm_deliveries_rate_check CHECK (rate BETWEEN 1 AND 5)
);

CREATE INDEX IF NOT EXISTS idx_dm_deliveries_order_id    ON dds.dm_deliveries (order_id);
CREATE INDEX IF NOT EXISTS idx_dm_deliveries_courier_id  ON dds.dm_deliveries (courier_id);
CREATE INDEX IF NOT EXISTS idx_dm_deliveries_order_ts    ON dds.dm_deliveries (order_ts);

-- Доработка существующей сущности "Заказ": добавляем ссылку на курьера
-- ПРОВЕРЬ: имя PK/бизнес-ключа в твоей dds.dm_orders — здесь предполагается "id" (surrogate PK)
ALTER TABLE dds.dm_orders
    ADD COLUMN IF NOT EXISTS courier_id integer REFERENCES dds.dm_couriers(id);
