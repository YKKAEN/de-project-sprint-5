"""
DAG: dds_delivery_dag

Разбирает JSON из STG в DDS-слой ("снежинка"):
- dds.dm_couriers   — справочник курьеров
- dds.dm_deliveries — факт доставки (заказ ↔ курьер, рейтинг, чаевые)
- dds.dm_orders     — простановка courier_id по уже существующим заказам

Идемпотентно: все вставки через ON CONFLICT DO UPDATE, можно гонять каждый день.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "pg_dwh"  # ПРОВЕРЬ: имя твоего Airflow Connection на DWH

UPSERT_COURIERS_SQL = """
    INSERT INTO dds.dm_couriers (courier_id, courier_name)
    SELECT
        object_value::json ->> '_id'  AS courier_id,
        object_value::json ->> 'name' AS courier_name
    FROM stg.deliverysystem_couriers
    ON CONFLICT (courier_id) DO UPDATE
        SET courier_name = EXCLUDED.courier_name;
"""

UPSERT_DELIVERIES_SQL = """
    INSERT INTO dds.dm_deliveries (
        delivery_id, order_id, order_ts, courier_id,
        address, delivery_ts, rate, tip_sum, sum
    )
    SELECT
        d.object_value::json ->> 'delivery_id'                     AS delivery_id,
        d.object_value::json ->> 'order_id'                        AS order_id,
        (d.object_value::json ->> 'order_ts')::timestamp           AS order_ts,
        c.id                                                       AS courier_id,
        d.object_value::json ->> 'address'                         AS address,
        (d.object_value::json ->> 'delivery_ts')::timestamp        AS delivery_ts,
        (d.object_value::json ->> 'rate')::smallint                AS rate,
        COALESCE((d.object_value::json ->> 'tip_sum')::numeric, 0) AS tip_sum,
        COALESCE((d.object_value::json ->> 'sum')::numeric, 0)     AS sum
    FROM stg.deliverysystem_deliveries d
    JOIN dds.dm_couriers c
        ON c.courier_id = d.object_value::json ->> 'courier_id'
    ON CONFLICT (delivery_id) DO UPDATE
        SET rate        = EXCLUDED.rate,
            tip_sum      = EXCLUDED.tip_sum,
            delivery_ts  = EXCLUDED.delivery_ts,
            address      = EXCLUDED.address;
"""

# ПРОВЕРЬ: имя бизнес-ключа заказа в твоей dds.dm_orders (тут — order_key)
UPDATE_ORDERS_COURIER_SQL = """
    UPDATE dds.dm_orders o
    SET courier_id = dd.courier_id
    FROM dds.dm_deliveries dd
    WHERE dd.order_id = o.order_key
      AND o.courier_id IS DISTINCT FROM dd.courier_id;
"""


@dag(
    dag_id="dds_delivery_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["dds", "delivery"],
)
def dds_delivery_dag():

    @task
    def load_dm_couriers():
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(UPSERT_COURIERS_SQL)

    @task
    def load_dm_deliveries():
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(UPSERT_DELIVERIES_SQL)

    @task
    def link_orders_to_couriers():
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(UPDATE_ORDERS_COURIER_SQL)

    load_dm_couriers() >> load_dm_deliveries() >> link_orders_to_couriers()


dds_delivery_dag_instance = dds_delivery_dag()
