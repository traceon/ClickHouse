# system.query_log {#system_tables-query_log}

Содержит информацию о выполняемых запросах, например, время начала обработки, продолжительность обработки, сообщения об ошибках.

!!! note "Внимание"
    Таблица не содержит входных данных для запросов `INSERT`.

Настойки логгирования можно изменить в секции серверной конфигурации [query_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log).

Можно отключить логгирование настройкой [log_queries = 0](../settings/settings.md#settings-log-queries). По-возможности, не отключайте логгирование, поскольку информация из таблицы важна при решении проблем.

Период сброса данных в таблицу задаётся параметром `flush_interval_milliseconds` в конфигурационной секции [query_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log). Чтобы принудительно записать логи из буффера памяти в таблицу, используйте запрос [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs).

ClickHouse не удаляет данные из таблица автоматически. Смотрите [Введение](#system-tables-introduction).

Таблица `system.query_log` содержит информацию о двух видах запросов:

1.  Первоначальные запросы, которые были выполнены непосредственно клиентом.
2.  Дочерние запросы, инициированные другими запросами (для выполнения распределенных запросов). Для дочерних запросов информация о первоначальном запросе содержится в столбцах `initial_*`.

В зависимости от статуса (столбец `type`) каждый запрос создаёт одну или две строки в таблице `query_log`:

1.  Если запрос выполнен успешно, создаются два события типа `QueryStart` и `QueryFinish`.
2.  Если во время обработки запроса возникла ошибка, создаются два события с типами `QueryStart` и `ExceptionWhileProcessing`.
3.  Если ошибка произошла ещё до запуска запроса, создается одно событие с типом `ExceptionBeforeStart`.

Столбцы:

-   `type` ([Enum8](../../sql-reference/data-types/enum.md)) — тип события, произошедшего при выполнении запроса. Значения:
    -   `'QueryStart' = 1` — успешное начало выполнения запроса.
    -   `'QueryFinish' = 2` — успешное завершение выполнения запроса.
    -   `'ExceptionBeforeStart' = 3` — исключение перед началом обработки запроса.
    -   `'ExceptionWhileProcessing' = 4` — исключение во время обработки запроса.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — дата начала запроса.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время начала запроса.
-   `event_time_microseconds` ([DateTime](../../sql-reference/data-types/datetime.md)) — время начала запроса с точностью до микросекунд.
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время начала обработки запроса.
-   `query_start_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — время начала обработки запроса с точностью до микросекунд.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — длительность выполнения запроса в миллисекундах.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — общее количество строк, считанных из всех таблиц и табличных функций, участвующих в запросе. Включает в себя обычные подзапросы, подзапросы для `IN` и `JOIN`. Для распределенных запросов `read_rows` включает в себя общее количество строк, прочитанных на всех репликах. Каждая реплика передает собственное значение `read_rows`, а сервер-инициатор запроса суммирует все полученные и локальные значения. Объемы кэша не учитываюся.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — общее количество байтов, считанных из всех таблиц и табличных функций, участвующих в запросе. Включает в себя обычные подзапросы, подзапросы для `IN` и `JOIN`. Для распределенных запросов `read_bytes` включает в себя общее количество байтов, прочитанных на всех репликах. Каждая реплика передает собственное значение `read_bytes`, а сервер-инициатор запроса суммирует все полученные и локальные значения. Объемы кэша не учитываюся.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество записанных строк для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — объём записанных данных в байтах для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `result_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество строк в результате запроса `SELECT` или количество строк в запросе `INSERT`.
-   `result_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — объём RAM в байтах, использованный для хранения результата запроса.
-   `memory_usage` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — потребление RAM запросом.
-   `current_database` ([String](../../sql-reference/data-types/string.md)) — имя текущей базы данных.
-   `query` ([String](../../sql-reference/data-types/string.md)) — текст запроса.
-   `normalized_query_hash` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — идентичная хэш-сумма без значений литералов для аналогичных запросов.
-   `query_kind` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) — тип запроса.
-   `databases` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — имена баз данных, присутствующих в запросе.
-   `tables` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — имена таблиц, присутствующих в запросе.
-   `columns` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — имена столбцов, присутствующих в запросе.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — код исключения.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — сообщение исключения, если запрос завершился по исключению.
-   `stack_trace` ([String](../../sql-reference/data-types/string.md)) — [stack trace](https://en.wikipedia.org/wiki/Stack_trace). Пустая строка, если запрос успешно завершен.
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md)) — вид запроса. Возможные значения:
    -   1 — запрос был инициирован клиентом.
    -   0 — запрос был инициирован другим запросом при выполнении распределенного запроса.
-   `user` ([String](../../sql-reference/data-types/string.md)) — пользователь, запустивший текущий запрос.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — ID запроса.
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP адрес, с которого пришел запрос.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — порт, с которого клиент сделал запрос
-   `initial_user` ([String](../../sql-reference/data-types/string.md)) — пользователь, запустивший первоначальный запрос (для распределенных запросов).
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — ID родительского запроса.
-   `initial_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP адрес, с которого пришел родительский запрос.
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — порт, с которого клиент сделал родительский запрос.
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md)) — интерфейс, с которого ушёл запрос. Возможные значения:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([String](../../sql-reference/data-types/string.md)) — имя пользователя операционной системы, который запустил [clickhouse-client](../../interfaces/cli.md).
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — имя сервера, с которого присоединился [clickhouse-client](../../interfaces/cli.md) или другой TCP клиент.
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) или другой TCP клиент.
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ревизия [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — старшая версия [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — младшая версия [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — патч [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `http_method` ([UInt8](../../sql-reference/data-types/int-uint.md)) — HTTP метод, инициировавший запрос. Возможные значения:
    -   0 — запрос запущен с интерфейса TCP.
    -   1 — `GET`.
    -   2 — `POST`.
-   `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — HTTP заголовок `UserAgent`.
-   `http_referer` ([String](../../sql-reference/data-types/string.md)) — HTTP заголовок `Referer` (содержит полный или частичный адрес страницы, с которой был выполнен запрос).
-   `forwarded_for` ([String](../../sql-reference/data-types/string.md)) — HTTP заголовок `X-Forwarded-For`.
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — `ключ квоты` из настроек [квот](quotas.md) (см. `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ревизия ClickHouse.
-   `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — счетчики для изменения различных метрик. Описание метрик можно получить из таблицы [system.events](#system_tables-events)(#system_tables-events
-   `Settings` ([Map(String, String)](../../sql-reference/data-types/array.md)) — имена настроек, которые меняются, когда клиент выполняет запрос. Чтобы разрешить логирование изменений настроек, установите параметр `log_query_settings` равным 1.
-   `log_comment` ([String](../../sql-reference/data-types/string.md)) — комментарий к записи в логе. Представляет собой произвольную строку, длина которой должна быть не больше, чем [max_query_size](../../operations/settings/settings.md#settings-max_query_size). Если нет комментария, то пустая строка.
-   `thread_ids` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — идентификаторы потоков, участвующих в обработке запросов.
-   `used_aggregate_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `агрегатных функций`, использованных при выполнении запроса.
-   `used_aggregate_function_combinators` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `комбинаторов агрегатных функций`, использованных при выполнении запроса.
-   `used_database_engines` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `движков баз данных`, использованных при выполнении запроса.
-   `used_data_type_families` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `семейств типов данных`, использованных при выполнении запроса.
-   `used_dictionaries` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `источников словарей`, использованных при выполнении запроса.
-   `used_formats` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `форматов`, использованных при выполнении запроса.
-   `used_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `функций`, использованных при выполнении запроса.
-   `used_storages` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `движков таблиц`, использованных при выполнении запроса.
-   `used_table_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `табличных функций`, использованных при выполнении запроса.

**Пример**

``` sql
SELECT * FROM system.query_log WHERE type = 'QueryFinish' AND (query LIKE '%toDate(\'2000-12-05\')%') ORDER BY query_start_time DESC LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
type:                          QueryStart
event_date:                    2020-09-11
event_time:                    2020-09-11 10:08:17
event_time_microseconds:       2020-09-11 10:08:17.063321
query_start_time:              2020-09-11 10:08:17
query_start_time_microseconds: 2020-09-11 10:08:17.063321
query_duration_ms:             0
read_rows:                     0
read_bytes:                    0
written_rows:                  0
written_bytes:                 0
result_rows:                   0
result_bytes:                  0
memory_usage:                  0
current_database:              default
query:                         INSERT INTO test1 VALUES
exception_code:                0
exception:
stack_trace:
is_initial_query:              1
user:                          default
query_id:                      50a320fd-85a8-49b8-8761-98a86bcbacef
address:                       ::ffff:127.0.0.1
port:                          33452
initial_user:                  default
initial_query_id:              50a320fd-85a8-49b8-8761-98a86bcbacef
initial_address:               ::ffff:127.0.0.1
initial_port:                  33452
interface:                     1
os_user:                       bharatnc
client_hostname:               tower
client_name:                   ClickHouse
client_revision:               54437
client_version_major:          20
client_version_minor:          7
client_version_patch:          2
http_method:                   0
http_user_agent:
quota_key:
revision:                      54440
thread_ids:                    []
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
Settings:             {'background_pool_size':'32','load_balancing':'random','allow_suspicious_low_cardinality_types':'1','distributed_aggregation_memory_efficient':'1','skip_unavailable_shards':'1','log_queries':'1','max_bytes_before_external_group_by':'20000000000','max_bytes_before_external_sort':'20000000000','allow_introspection_functions':'1'}
```

**Смотрите также**

-   [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — в этой таблице содержится информация о цепочке каждого выполненного запроса.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/query_log) <!--hide-->
