# api_entities.md — проектирование витрины расчётов с курьерами

## 1. Поля витрины cdm.dm_courier_ledger

| Поле | Источник |
|---|---|
| id | генерируется (serial) |
| courier_id | dds.dm_couriers.courier_id |
| courier_name | dds.dm_couriers.courier_name |
| settlement_year | dds.dm_deliveries.order_ts |
| settlement_month | dds.dm_deliveries.order_ts |
| orders_count | COUNT(*) по dds.dm_deliveries за курьера/месяц |
| orders_total_sum | SUM по сумме заказов (dds.fct_product_sales, сгруппировано по order_id) |
| rate_avg | AVG(dds.dm_deliveries.rate) за курьера/месяц |
| order_processing_fee | orders_total_sum * 0.25 (расчёт в CDM-DAG) |
| courier_order_sum | по ставке в зависимости от rate_avg, минимум на заказ (расчёт в CDM-DAG) |
| courier_tips_sum | SUM(dds.dm_deliveries.tip_sum) |
| courier_reward_sum | courier_order_sum + courier_tips_sum * 0.95 |

## 2. Таблицы DDS, из которых берём поля

| Таблица | Статус | Комментарий |
|---|---|---|
| dds.dm_orders | уже есть | дорабатываем: добавляем courier_id (FK на dds.dm_couriers) |
| dds.fct_product_sales | уже есть | источник orders_total_sum (сумма по товарным позициям заказа) |
| dds.dm_timestamps | уже есть | не обязателен для этой витрины, order_ts берём из dm_deliveries |
| dds.dm_couriers | НОВАЯ | справочник курьеров |
| dds.dm_deliveries | НОВАЯ | факт доставки: заказ ↔ курьер, рейтинг, чаевые, даты |

Модель «снежинка»: dm_deliveries — центральная таблица факта доставки,
ссылается на dm_couriers (курьер) и логически связана с dm_orders по
бизнес-ключу заказа (order_id / order_key).

## 3. Сущности и поля, которые нужно забрать из API доставки

Используем только 2 из 3 методов — `/couriers` и `/deliveries`.
`/restaurants` не нужен: рестораны уже есть в хранилище из подсистемы заказов.

### GET /couriers → stg.deliverysystem_couriers
- `_id` → courier_id (бизнес-ключ)
- `name` → courier_name

### GET /deliveries → stg.deliverysystem_deliveries
- `order_id`
- `order_ts`
- `delivery_id`
- `courier_id`
- `address`
- `delivery_ts`
- `rate`
- `tip_sum`
- `sum` (не используем в витрине — сумма заказа уже есть в DWH из MongoDB, но сохраняем в STG "как есть", т.к. STG = сырые данные без потерь)

## 4. Важные нюансы логики

1. Отчёт собирается **по дате заказа (order_ts)**, а не по дате доставки —
   поэтому settlement_year/settlement_month считаем от order_ts, а не delivery_ts.
   Это критично для заказов "через полночь" на стыке месяцев.
2. Загрузка STG за доставки — инкрементальная, окно 7 дней (`from`/`to`),
   курьеры — полная перезагрузка (справочник маленький).
3. Пагинация через `sort_field` + `sort_direction` + `limit`/`offset`,
   иначе offset может "поехать" при изменении данных между запросами.
