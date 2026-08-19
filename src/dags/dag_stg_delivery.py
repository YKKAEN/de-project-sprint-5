"""
DAG: stg_delivery_api_dag

Наполняет STG-слой сырыми данными из API курьерской службы:
- couriers   — полный список (справочник маленький, грузим целиком каждый запуск)
- deliveries — инкрементально, окно 7 дней перед датой запуска, с пагинацией

Пагинация: limit/offset + обязательная сортировка (sort_field/sort_direction),
чтобы offset не "плавал" при параллельных изменениях данных в источнике.
"""
from datetime import datetime, timedelta
import json

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
PG_CONN_ID = "pg_dwh"  # ПРОВЕРЬ: имя твоего Airflow Connection на DWH

DEFAULT_HEADERS = {
    "X-Nickname": Variable.get("delivery_api_nickname"),
    "X-Cohort": Variable.get("delivery_api_cohort"),
    "X-API-KEY": Variable.get("delivery_api_key"),
}

PAGE_LIMIT = 50


def fetch_paginated(endpoint: str, extra_params: dict | None = None) -> list[dict]:
    """Постранично вычитывает все записи эндпоинта, соблюдая сортировку."""
    params = {
        "limit": PAGE_LIMIT,
        "offset": 0,
        "sort_field": "_id",
        "sort_direction": "asc",
        **(extra_params or {}),
    }
    results: list[dict] = []
    while True:
        resp = requests.get(
            f"{API_BASE_URL}/{endpoint}",
            headers=DEFAULT_HEADERS,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        results.extend(page)
        if len(page) < params["limit"]:
            break
        params["offset"] += params["limit"]
    return results


@dag(
    dag_id="stg_delivery_api_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["stg", "delivery-api"],
)
def stg_delivery_api_dag():

    @task
    def load_couriers():
        couriers = fetch_paginated("couriers")
        rows = [(c["_id"], json.dumps(c, ensure_ascii=False)) for c in couriers]

        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        pg.insert_rows(
            table="stg.deliverysystem_couriers",
            rows=rows,
            target_fields=["object_id", "object_value"],
            replace=True,
            replace_index="object_id",
        )
        return len(rows)

    @task
    def load_deliveries(data_interval_end=None):
        date_to = data_interval_end or datetime.utcnow()
        date_from = date_to - timedelta(days=7)

        deliveries = fetch_paginated(
            "deliveries",
            extra_params={
                "from": date_from.strftime("%Y-%m-%d %H:%M:%S"),
                "to": date_to.strftime("%Y-%m-%d %H:%M:%S"),
                "sort_field": "date",
            },
        )
        rows = [(d["delivery_id"], json.dumps(d, ensure_ascii=False)) for d in deliveries]

        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        pg.insert_rows(
            table="stg.deliverysystem_deliveries",
            rows=rows,
            target_fields=["object_id", "object_value"],
            replace=True,
            replace_index="object_id",
        )
        return len(rows)

    load_couriers() >> load_deliveries()


stg_delivery_api_dag_instance = stg_delivery_api_dag()
