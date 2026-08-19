"""
DAG: cdm_courier_ledger_dag

Считает витрину cdm.dm_courier_ledger по всем курьерам и всем
отчётным месяцам, которые есть в dds.dm_deliveries.

Ключевые правила:
- Период (year/month) считается по order_ts (дата ЗАКАЗА, не доставки) —
  важно для заказов, сделанных ночью и доставленных на следующий день/
  в следующем месяце.
- rate_avg считается за месяц ОДИН раз на курьера и определяет ставку/
  минимум, которые применяются к каждому его заказу в этом месяце.
- courier_order_sum = сумма по всем заказам курьера за месяц от
  max(order_sum * ставка, минимум_за_заказ) — минимум применяется на
  КАЖДЫЙ заказ, а не на весь месяц целиком.

Идемпотентно (ON CONFLICT DO UPDATE) — можно пересчитывать каждый день.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "pg_dwh"  # ПРОВЕРЬ: имя твоего Airflow Connection на DWH

# ПРОВЕРЬ: имя таблицы/полей с суммой заказа в твоём DDS.
# Здесь предполагается dds.fct_product_sales(order_id, total_sum) —
# сумма по одной товарной позиции заказа, агрегируем по order_id.
CDM_UPSERT_SQL = """
WITH order_sums AS (
    SELECT
        order_id,
        SUM(total_sum) AS order_sum
    FROM dds.fct_product_sales
    GROUP BY order_id
),
courier_month_rate AS (
    SELECT
        courier_id,
        EXTRACT(YEAR  FROM order_ts)::smallint AS settlement_year,
        EXTRACT(MONTH FROM order_ts)::smallint AS settlement_month,
        AVG(rate) AS rate_avg
    FROM dds.dm_deliveries
    GROUP BY courier_id, settlement_year, settlement_month
),
order_calc AS (
    SELECT
        dd.courier_id,
        EXTRACT(YEAR  FROM dd.order_ts)::smallint AS settlement_year,
        EXTRACT(MONTH FROM dd.order_ts)::smallint AS settlement_month,
        dd.order_id,
        dd.tip_sum,
        COALESCE(os.order_sum, 0) AS order_sum,
        cmr.rate_avg,
        CASE
            WHEN cmr.rate_avg < 4   THEN GREATEST(COALESCE(os.order_sum, 0) * 0.05, 100)
            WHEN cmr.rate_avg < 4.5 THEN GREATEST(COALESCE(os.order_sum, 0) * 0.07, 150)
            WHEN cmr.rate_avg < 4.9 THEN GREATEST(COALESCE(os.order_sum, 0) * 0.08, 175)
            ELSE                          GREATEST(COALESCE(os.order_sum, 0) * 0.10, 200)
        END AS courier_order_payment
    FROM dds.dm_deliveries dd
    JOIN courier_month_rate cmr
        ON cmr.courier_id = dd.courier_id
       AND cmr.settlement_year  = EXTRACT(YEAR  FROM dd.order_ts)::smallint
       AND cmr.settlement_month = EXTRACT(MONTH FROM dd.order_ts)::smallint
    LEFT JOIN order_sums os ON os.order_id = dd.order_id
),
courier_month_agg AS (
    SELECT
        courier_id,
        settlement_year,
        settlement_month,
        COUNT(*)                     AS orders_count,
        SUM(order_sum)                AS orders_total_sum,
        MAX(rate_avg)                 AS rate_avg,
        SUM(courier_order_payment)    AS courier_order_sum,
        SUM(tip_sum)                  AS courier_tips_sum
    FROM order_calc
    GROUP BY courier_id, settlement_year, settlement_month
)
INSERT INTO cdm.dm_courier_ledger (
    courier_id, courier_name, settlement_year, settlement_month,
    orders_count, orders_total_sum, rate_avg,
    order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum
)
SELECT
    c.courier_id,
    c.courier_name,
    a.settlement_year,
    a.settlement_month,
    a.orders_count,
    ROUND(a.orders_total_sum, 2),
    ROUND(a.rate_avg, 2),
    ROUND(a.orders_total_sum * 0.25, 2)                        AS order_processing_fee,
    ROUND(a.courier_order_sum, 2)                               AS courier_order_sum,
    ROUND(a.courier_tips_sum, 2)                                AS courier_tips_sum,
    ROUND(a.courier_order_sum + a.courier_tips_sum * 0.95, 2)   AS courier_reward_sum
FROM courier_month_agg a
JOIN dds.dm_couriers c ON c.id = a.courier_id
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
    SET orders_count          = EXCLUDED.orders_count,
        orders_total_sum      = EXCLUDED.orders_total_sum,
        rate_avg              = EXCLUDED.rate_avg,
        order_processing_fee  = EXCLUDED.order_processing_fee,
        courier_order_sum     = EXCLUDED.courier_order_sum,
        courier_tips_sum      = EXCLUDED.courier_tips_sum,
        courier_reward_sum    = EXCLUDED.courier_reward_sum,
        courier_name          = EXCLUDED.courier_name;
"""


@dag(
    dag_id="cdm_courier_ledger_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["cdm", "courier-ledger"],
)
def cdm_courier_ledger_dag():

    @task
    def build_courier_ledger():
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(CDM_UPSERT_SQL)

    build_courier_ledger()


cdm_courier_ledger_dag_instance = cdm_courier_ledger_dag()
