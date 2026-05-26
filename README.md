# Trending Search Service («Сейчас ищут»)

**Тестовое задание — Backend-разработчик (Go), Wildberries Поиск**

Сервис агрегации популярных поисковых запросов в реальном времени.
Читает события из Kafka, фильтрует накрутки (4 уровня антифрода),
управляет стоп-листом, отдаёт топ-N через HTTP (fasthttp) с latency < 1ms (p99)
благодаря pre-serialized JSON cache.

---

## Быстрый старт

### Требования

- Go 1.22+
- Docker и Docker Compose (плагин `docker compose`)

### 1. Запустить инфраструктуру

```bash
make docker-up
```

Поднимает Redpanda (Kafka), создаёт топик `search_log`, запускает сервис.

### 2. Проверить health

```bash
curl -s http://localhost:8080/health
# => {"status":"ok","ready":true}
```

### 3. Отправить тестовые события

```bash
docker exec -it deployments-redpanda-1 rpk topic produce search_log --brokers localhost:9092
```

Скопируйте строки ниже (каждая с новой строки, Ctrl+D для завершения):

```json
{"event_id":"e1","timestamp":"2026-05-26T12:00:00Z","query_normalized":"iphone 15","user_id":"u1","session_id":"s1","ip_hash":"ip1","device_type":"mobile","platform":"ios","source":"main"}
{"event_id":"e2","timestamp":"2026-05-26T12:00:00Z","query_normalized":"iphone 15","user_id":"u2","session_id":"s2","ip_hash":"ip2","device_type":"mobile","platform":"ios","source":"main"}
{"event_id":"e3","timestamp":"2026-05-26T12:00:00Z","query_normalized":"iphone 15","user_id":"u3","session_id":"s3","ip_hash":"ip3","device_type":"mobile","platform":"ios","source":"main"}
{"event_id":"e4","timestamp":"2026-05-26T12:00:00Z","query_normalized":"samsung s25","user_id":"u4","session_id":"s4","ip_hash":"ip4","device_type":"mobile","platform":"android","source":"main"}
{"event_id":"e5","timestamp":"2026-05-26T12:00:00Z","query_normalized":"airpods","user_id":"u5","session_id":"s5","ip_hash":"ip5","device_type":"mobile","platform":"ios","source":"main"}
```

### 4. Получить тренды

```bash
curl -s "http://localhost:8080/api/v1/trends?limit=10"
```

```json
{"total_queries":3,"trends":[
  {"query":"iphone 15","count":3,"rank":1},
  {"query":"samsung s25","count":1,"rank":2},
  {"query":"airpods","count":1,"rank":3}
]}
```

### 5. Prometheus метрики

```bash
curl -s http://localhost:8080/metrics | grep trending_search
```

### 6. Остановить

```bash
make docker-down
```

---

## API Endpoints

| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/api/v1/trends?limit=N` | Топ-N популярных запросов (по умолчанию 100) |
| `POST` | `/api/v1/stoplist` | Добавить стоп-слово `{"word":"xxx","match_type":"exact\|contains"}` |
| `DELETE` | `/api/v1/stoplist/{word}` | Удалить точное стоп-слово |
| `DELETE` | `/api/v1/stoplist/pattern/{word}` | Удалить паттерн из стоп-листа |
| `GET` | `/api/v1/stoplist` | Список стоп-слов |
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus метрики |

### Примеры стоп-листа

```bash
# Добавить точное совпадение
curl -s -X POST http://localhost:8080/api/v1/stoplist \
  -H "Content-Type: application/json" \
  -d '{"word":"airpods","match_type":"exact"}'

# Добавить подстроку
curl -s -X POST http://localhost:8080/api/v1/stoplist \
  -H "Content-Type: application/json" \
  -d '{"word":"samsung","match_type":"contains"}'

# Удалить точное стоп-слово
curl -s -X DELETE http://localhost:8080/api/v1/stoplist/airpods

# Удалить паттерн
curl -s -X DELETE http://localhost:8080/api/v1/stoplist/pattern/samsung

# Посмотреть список
curl -s http://localhost:8080/api/v1/stoplist
```

---

## Контракт данных (Kafka Payload)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-05-23T17:54:32.123Z",
  "query_normalized": "iphone 15 pro max",
  "user_id": "wb_user_948291",
  "session_id": "sess_a1b2c3d4e5",
  "ip_hash": "sha256:7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730",
  "device_type": "mobile",
  "platform": "ios",
  "source": "main_search"
}
```

### Обоснование каждого поля

| Поле | Зачем нужно |
|------|-------------|
| `event_id` | Идемпотентность: позволяет consumer'у детектить дубликаты при replay/rebalance. В текущей реализации не используется, но закладывает основу для exactly-once |
| `timestamp` | Event time для корректного позиционирования во временном окне. Используется как fallback, если время обработки отличается от времени события |
| `query_normalized` | **Основные данные для трендов.** Уже очищенная строка запроса (lowercase, trim, лишние пробелы удалены) от смежного сервиса. Храним и агрегируем по ней |
| `user_id` | **User rate limiting** (уровень 2 антифрода) и **дедупликация** (уровень 3): один пользователь не может накрутить один и тот же запрос дважды в течение минуты |
| `ip_hash` | **IP rate limiting** (уровень 1 антифрода). SHA256-хеш — никакого хранения реальных IP (PII) |
| `device_type` / `platform` / `source` | Для будущей аналитики и сегментации трендов (напр., «Сейчас ищут на iOS»). Позволяют фильтровать или взвешивать запросы по категориям без изменения контракта |
| `session_id` | Для будущей дедупликации по сессии (например, один запрос от сессии, а не от пользователя) |

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                     Kafka / Redpanda                         │
│                      topic: search_log                       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Consumer (franz-go, cooperative-sticky rebalance)           │
│  PollRecords(1000), batch ~5MB                               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Pipeline (5 уровней фильтрации)                              │
│  0. Stop-list exact match (на входе)                        │
│  1. IP Rate Limit  (0.5 rps, burst=5)                      │
│  2. User Rate Limit (0.166 rps, burst=5)                    │
│  3. Dedup User+Query (30s слот × 2 = 1 мин)                │
│  4. Burst Detection (>20 событий/10s → карантин 60s)       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  In-Memory Storage                                           │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────────┐       │
│  │  Window   │  │   Global     │  │   TopHeap        │       │
│  │ ring buf  │  │   Counter    │  │   min-heap       │       │
│  │ 30×10s    │  │   map[str]   │  │   K=300          │       │
│  └──────────┘  └──────┬───────┘  └────────┬─────────┘       │
│                       │                   │                  │
│                       ▼                   ▼                  │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  Recalc (каждые 500ms):                               │    │
│  │  1. Heap candidates → verify через GlobalCounter     │    │
│  │  2. Если < 200 кандидатов — SnapshotTopCandidates    │    │
│  │  3. Sort + стоп-лист filter → cache.Store            │    │
│  └───────────────────────────┬──────────────────────────┘    │
│                              │                               │
│                              ▼                               │
│  ┌──────────────────────────────────────────────────────┐    │
│  │  TrendCache (atomic.Pointer)                          │    │
│  │  ┌─────────────────┐   ┌──────────────────────────┐   │    │
│  │  │  TrendsData     │   │  jsonBin[0..100]         │   │    │
│  │  │  (структура)    │   │  pre-serialized JSON     │   │    │
│  │  └─────────────────┘   └──────────────────────────┘   │    │
│  └───────────────────────┬──────────────────────────────┘    │
└──────────────────────────┼───────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  HTTP API (fasthttp)                                         │
│  GET /api/v1/trends                                          │
│  → cache.LoadJSON(limit) — 0.5 ns, 0 allocs                 │
└─────────────────────────────────────────────────────────────┘
```

### Ключевые архитектурные решения

**1. In-memory storage вместо БД**
- Запрос: SPEC запрещает managed облачные решения. In-memory даёт latency < 1 мкс (vs 1-10 мс для Redis/MySQL).
- Компромисс: данные теряются при рестарте. Решение: Kafka как persistent log — replay при старте.

**2. 30 слотов × 10 секунд = 5 минут**
- Почему не 1 слот на 5 минут? Нужна ротация: старые данные должны вытесняться постепенно.
- Почему не 300 слотов по 1 секунде? Избыточно: 300 слотов × N запросов = лишняя память при ротации.
- 30 × 10s: компромисс — достаточно гранулярно для burst detection, достаточно крупно для эффективной ротации.

**3. Hot heap (K = 300 = 3 × N)**
- Полный SnapshotTopCandidates (O(M log N)) может быть дорогим при M=10⁶ запросов.
- Hot heap: O(log K) на вставку, O(K) на сбор кандидатов. Обновляется параллельно с инкрементами.
- K = 3N: чтобы горячие запросы не вытеснялись при флуктуациях счётчика.
- Компромисс: heap может быть stale (не учитывает декременты при ротации). Решение: верификация через GlobalCounter при recalc.

**4. Pre-serialized JSON cache + atomic.Pointer**
- Hot path чтения: просто `LoadJSON(limit)` — **0.53 ns, 0 allocs**.
- Cold path: ресериализация с обрезанием до нужного limit.
- Обновление: каждые 500ms фоновая горутина пересчитывает топ и атомарно подменяет указатель.
- `jsonBin[0..100]`: для limit 1..100 — готовые pre-serialized []byte (самые частые запросы клиента).

**5. Двухфазный recalc**
- Фаза 1: собрать кандидаты из heap (O(K)) и верифицировать через GlobalCounter.Get (O(1)).
- Фаза 2: если кандидатов < 2N — снапшот GlobalCounter (O(M log 2N)).
- Решает проблему stale heap + подстраховывает на случай, если heap слишком мал.

---

## Антифрод: 5 уровней защиты

```
Level 0: Stop-list exact    — блокировка на ingress (O(1)), до попадания в счётчик
Level 1: IP Rate Limit      — 30 запросов/мин с одного IP (Token Bucket, burst=5)
Level 2: User Rate Limit    — 10 запросов/мин от одного пользователя
Level 3: Dedup User+Query   — 1 мин скользящее окно (2 слота по 30 сек)
Level 4: Burst Detection    — >20 событий запроса за 10 сек → карантин 60 сек
```

**Burst detection** использует два порога:
1. **Относительный**: `currentSlotCount > 5 × sum(prev 2 slots)` — ловит резкие всплески на фоне обычной активности.
2. **Абсолютный**: `currentSlotCount > 20` в одном 10-сек слоте — ловит ситуации, когда все 100500 событий приходят в одном poll-пакете Kafka и prevTotal = 0 (relative threshold не сработает).

---

## Prometheus метрики

| Метрика | Тип | Лейблы |
|---------|-----|--------|
| `trending_search_events_total` | Counter | `status: processed\|dropped_stoplisted\|dropped_ip_limit\|dropped_user_limit\|dropped_dedup\|dropped_burst\|dropped_quarantine\|dropped_normalize` |
| `trending_search_api_requests_total` | Counter | `endpoint: trends\|stoplist_add\|stoplist_remove\|stoplist_remove_pattern\|stoplist_list`, `status: 200\|201\|400\|500\|503` |
| `trending_search_recalc_duration_seconds` | Histogram | — |
| `trending_search_memory_unique_queries` | Gauge | — |
| `trending_search_top_cache_timestamp` | Gauge | — |

---

## Бенчмарки (результаты нагрузочного тестирования)

Тестирование на `AMD Ryzen 5 6600H, Windows 11`:

```
BenchmarkTopHeapUpdate-12          32806979   42.1 ns/op    0 B/op    0 allocs/op
BenchmarkGlobalCounterInc-12       20500685   53.2 ns/op    0 B/op    0 allocs/op
BenchmarkGlobalCounterSnapshot-100  207862   6054 ns/op  4472 B/op  60 allocs/op
BenchmarkTrendCacheStore-12           1057  1.18 ms/op   272 KB    400 allocs/op
BenchmarkTrendCacheLoadJSON-12  1000000000   0.53 ns/op    0 B/op    0 allocs/op
BenchmarkPipelineProcess-12        4611086    267 ns/op   16 B/op    1 allocs/op
```

**Выводы:**
- **Чтение топа (TrendCache.LoadJSON)**: 0.53 ns — практически zero-cost.
- **Обработка события (Pipeline.Process)**: 267 ns на событие — ~3.7 млн событий/сек на одном ядре.
- **Hot heap**: 42 ns на обновление — легко держит миллионы инкрементов в секунду.
- **Cache.Store**: 1.18 ms на полный пересчёт топа из 100 элементов — 847 раз/сек (при recalcInterval=500ms это более чем достаточно).

---

## Конфигурация (переменные окружения)

| Переменная | По умолчанию | Описание |
|-----------|--------|----------|
| `KAFKA_BROKERS` | `localhost:9092` | Брокеры Kafka |
| `KAFKA_TOPIC` | `search_log` | Топик для чтения |
| `KAFKA_GROUP_ID` | `trending-search-consumer` | Consumer group |
| `HTTP_PORT` | `8080` | Порт HTTP API |
| `WINDOW_SLOTS` | `30` | Количество слотов окна |
| `SLOT_DURATION_SEC` | `10` | Длительность слота (сек) |
| `TOP_N` | `100` | Сколько трендов возвращать |
| `TOP_K` | `300` | Размер hot heap (K=3×N) |
| `RECALC_INTERVAL_MS` | `500` | Интервал пересчёта (мс) |
| `RATE_LIMIT_IP` | `0.5` | IP лимит (запросов/сек = 30/мин) |
| `RATE_LIMIT_USER` | `0.166` | User лимит (запросов/сек = 10/мин) |
| `RATE_LIMIT_BURST` | `5` | Burst token bucket |
| `QUARANTINE_SEC` | `60` | Длительность карантина (сек) |

---

## Trade-offs, компромиссы и бизнес-логика

### Компромиссы ради производительности

| Компромисс | Что теряем | Что выигрываем |
|-----------|-----------|----------------|
| In-memory без БД | Нет durability при краше (кроме Kafka log) | ~1000× latency vs Redis/MySQL |
| Pre-serialized cache не для всех limit | Для limit > 100 — cold path (~1 μs) | Hot path для 90% запросов: **0.5 ns** |
| Stale top heap (не учитывает декременты) | Верификация при recalc раз в 500ms | Инкремент: **42 ns**, 0 allocs |
| Dedup окно ротации 30s × 2 слот | Пропускает дубликат через > 60 сек | Нет отдельного timer — ротация совмещена с window ticker |
| Stop-list exact на ingress + contains на recalc | Exact match на каждое событие (O(1)) | Блокирует спам до попадания в счётчик |

### Почему не использовали альтернативные подходы

- **Redis / Sorted Sets**: внешняя зависимость + RTT 1-10ms на запрос. In-memory даёт ~0.5 μs.
- **sync.Map**: не подходит для high-contention сценариев. RWMutex + sharding (GlobalCounter + TopHeap) эффективнее.
- **Goroutine per event**: пайплайн один — consumer уже батчит PollRecords (до 1000). Горнины на каждое событие — лишний оверхед.
- **gRPC вместо HTTP**: fasthttp даёт ~10 μs на запрос, gRPC ~50 μs (для нашего кейса read-heavy HTTP проще).

---

## Тестирование

```bash
# Unit-тесты
go test ./...

# С race detector (Linux/macOS, на Windows требует CGO)
go test -race ./...

# Бенчмарки
go test -bench=Benchmark -benchmem ./internal/storage/ ./internal/processor/
```

---

## Структура проекта

```
trending-search-service/
├── api/openapi.yaml                 // OpenAPI 3.0 спецификация
├── cmd/service/main.go              // Точка входа, wiring, фоновые горутины
├── internal/
│   ├── api/                         // fasthttp handlers + Prometheus метрики
│   ├── config/config.go             // Env-конфигурация
│   ├── consumer/consumer.go         // franz-go Kafka consumer
│   ├── models/event.go              // SearchEvent структура
│   ├── processor/                   // Пайплайн: антифрод → storage
│   ├── storage/                     // In-memory хранилище + Sliding Window
│   └── stoplist/manager.go          // Динамический стоп-лист
├── deployments/
│   └── docker-compose.yml           // Redpanda + service
├── scripts/
│   ├── load-test-read.sh            // Load test чтения (vegeta)
│   └── load-test-write.sh           // Load test записи (rpk)
├── Dockerfile
├── Makefile
├── go.mod
└── README.md
```

---

## Зависимости (Go 1.22)

| Пакет | Версия | Назначение |
|-------|--------|------------|
| `github.com/valyala/fasthttp` | v1.58.0 | HTTP-сервер (high performance) |
| `github.com/fasthttp/router` | v1.5.4 | Маршрутизация для fasthttp |
| `github.com/twmb/franz-go` | v1.16.1 | Kafka клиент (cooperative sticky) |
| `github.com/prometheus/client_golang` | v1.19.1 | Prometheus метрики |

---
