-- ============================================================
-- STG layer: сырые данные из API подсистемы доставки
-- Данные хранятся "как пришли" (JSON), без трансформаций.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
    id            serial4 PRIMARY KEY,
    object_id     varchar NOT NULL,       -- _id курьера из API
    object_value  text    NOT NULL,       -- сырой JSON курьера
    update_ts     timestamp NOT NULL DEFAULT now(),
    CONSTRAINT deliverysystem_couriers_object_id_uindex UNIQUE (object_id)
);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
    id            serial4 PRIMARY KEY,
    object_id     varchar NOT NULL,       -- delivery_id из API
    object_value  text    NOT NULL,       -- сырой JSON доставки
    update_ts     timestamp NOT NULL DEFAULT now(),
    CONSTRAINT deliverysystem_deliveries_object_id_uindex UNIQUE (object_id)
);

CREATE INDEX IF NOT EXISTS idx_deliverysystem_deliveries_update_ts
    ON stg.deliverysystem_deliveries (update_ts);
