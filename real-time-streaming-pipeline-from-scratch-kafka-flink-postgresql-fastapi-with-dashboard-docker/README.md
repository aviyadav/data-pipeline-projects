# Real-Time Streaming Pipeline: Kafka → Flink → PostgreSQL → FastAPI → Dashboard

A complete, end-to-end real-time analytics pipeline. Simulated sensor events
are produced onto Kafka, aggregated in real time by Apache Flink (PyFlink
DataStream API) into rolling per-device windows, persisted to PostgreSQL,
served over a FastAPI endpoint, and visualized live in a Streamlit
dashboard.

This project is based on the article **"Building a Real-Time Streaming
Pipeline From Scratch"** (see the PDF of the same name in the project root
for the original write-up/design).

## Architecture

```
┌──────────┐     ┌───────┐     ┌──────────────────────────────┐     ┌────────────┐
│ producer │────▶│ Kafka │────▶│         Flink job             │────▶│ PostgreSQL │
│  (Python)│     │ topic │     │ JSON → Sensor Event → Watermark│     │ (sensor_   │
└──────────┘     │sensor-│     │ → keyBy(device_id)             │     │ aggregates)│
                 │events │     │ → 10-min tumbling window        │     └─────┬──────┘
                 └───────┘     │ → AVG / MIN / MAX               │           │
                                └──────────────────────────────┘           │
                                                                            ▼
                                                                    ┌───────────────┐
                                                                    │ FastAPI        │
                                                                    │ GET /sensors   │
                                                                    └───────┬───────┘
                                                                            │
                                                                            ▼
                                                                    ┌───────────────┐
                                                                    │ Streamlit      │
                                                                    │ dashboard      │
                                                                    └───────────────┘
```

## Tech stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka (KRaft mode, `apache/kafka:4.0.1`) |
| Stream processing | Apache Flink 2.1.0, PyFlink DataStream API |
| Storage | PostgreSQL 17 |
| API | FastAPI + Uvicorn |
| Dashboard | Streamlit + Plotly |
| Producer/tooling | Python, `uv` |

## Project layout

```
.
├── Building a Real-Time Streaming Pipeline From Scratch.pdf  # source article
├── docker-compose.yml       # Kafka, Postgres, Flink JobManager/TaskManager
├── flink/
│   ├── Dockerfile           # Flink image + PyFlink + Kafka/Postgres client jars
│   └── job.py               # The PyFlink DataStream job
├── producer/
│   └── producer.py          # Generates fake sensor events onto Kafka
├── api/
│   └── main.py              # FastAPI: GET /sensors reads from Postgres
├── dashboard/
│   └── app.py               # Streamlit dashboard: charts data from the API
├── sql/
│   └── init.sql             # sensor_aggregates table DDL (auto-applied)
└── RUN-BOOK.md              # Original, chronological build notes/run log
```

> `RUN-BOOK.md` contains the step-by-step notes recorded while building this
> project (in order) and is kept for history. The instructions below are the
> current, consolidated, and verified way to run the whole system.

## Prerequisites

- Docker + Docker Compose v2
- [`uv`](https://docs.astral.sh/uv/) (to run the producer, API, and dashboard)

No local Python/PyFlink installation is required for the Flink job — PyFlink
runs inside the Flink containers, which `flink/Dockerfile` builds with
Python 3 and `apache-flink==2.1.0` pre-installed (matching the `flink:2.1.0`
base image).

## Running the project (in order)

Run these steps in sequence from the project root. Steps 4–7 each run in
their own terminal/process and are long-running.

### 1. Install Python dependencies

```sh
uv sync
```

### 2. Build and start the infrastructure (Kafka, Postgres, Flink)

```sh
docker compose up -d --build
```

This starts 4 containers: `kafka`, `postgres`, `flink-jobmanager`,
`flink-taskmanager`. The first build takes a few minutes (installs PyFlink).

Check everything is up:

```sh
docker compose ps
```

### 3. Verify the infrastructure

Postgres auto-created the results table (`sql/init.sql` is mounted into
Postgres's init directory and runs automatically on first startup):

```sh
docker exec postgres psql -U flink -d streaming -c "\d sensor_aggregates"
```

Flink cluster is healthy:

```sh
curl -s http://localhost:8081/overview
```

Expect `"taskmanagers":1` and `"slots-total":4`. You can also open the Flink
Web UI at http://localhost:8081.

> If the table is missing (e.g. reusing an old volume from before this table
> existed), reset it: `docker compose down -v`, then repeat step 2.

### 4. Start the producer

In its own terminal (runs forever, sending one simulated sensor event every
200ms):

```sh
cd producer
uv run producer.py
```

Verify Kafka is receiving events:

```sh
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic sensor-events \
  --from-beginning \
  --max-messages 5
```

### 5. Submit the Flink job

The job is a continuous streaming job, so submit it **detached** (`-d`) —
otherwise the CLI blocks forever attached to the job:

```sh
docker exec flink-jobmanager /opt/flink/bin/flink run -d -py /opt/flink/usrlib/job.py
```

On success you'll see `Job has been submitted with JobID <...>`.

Verify it's running:

```sh
curl -s http://localhost:8081/jobs/overview
```

Look for `"name":"Sensor Analytics"` with `"state":"RUNNING"`. You can also
check the Web UI and confirm `numRecordsIn` is increasing on the source
task, and check for errors with:

```sh
docker logs flink-taskmanager --tail 100
docker logs flink-jobmanager --tail 100
```

### 6. Start the FastAPI server

In its own terminal:

```sh
cd api
uv run uvicorn main:app --reload
```

Visit http://localhost:8000/sensors — it queries `sensor_aggregates`
directly and returns the latest 100 rows as JSON.

### 7. Start the dashboard

In its own terminal:

```sh
uv run streamlit run dashboard/app.py
```

Open http://localhost:8501 — it polls the FastAPI `/sensors` endpoint and
plots average temperature per device over time.

### 8. Check results end-to-end

The Flink job uses a **10-minute** tumbling window, so the first rows appear
roughly 10 minutes after the producer starts sending data for a given
device. You can check the raw data directly at any time:

```sh
docker exec postgres psql -U flink -d streaming -c \
  "SELECT device_id, window_start, window_end, avg_temperature, max_temperature, min_temperature, event_count FROM sensor_aggregates ORDER BY window_start DESC LIMIT 10;"
```

Or via the API (`curl http://localhost:8000/sensors`) or the dashboard.

#### Faster local verification (optional)

To confirm the pipeline works without waiting 10 minutes, temporarily
shorten the window in `flink/job.py`:

```python
.window(TumblingEventTimeWindows.of(Time.seconds(20)))  # TEMP: for quick local verification
```

Then cancel the running job and resubmit:

```sh
docker exec flink-jobmanager /opt/flink/bin/flink list
docker exec flink-jobmanager /opt/flink/bin/flink cancel <JOB_ID>
docker exec flink-jobmanager /opt/flink/bin/flink run -d -py /opt/flink/usrlib/job.py
```

Wait ~30 seconds, then re-check Postgres/the API/the dashboard. **Remember
to revert the window back to `Time.minutes(10)` and resubmit afterward.**

### 9. Stop everything

```sh
# Ctrl+C the producer, uvicorn, and streamlit processes, then:
docker exec flink-jobmanager /opt/flink/bin/flink cancel <JOB_ID>
docker compose down          # keep data (Postgres volume persists)
docker compose down -v       # or wipe all data, including Postgres
```

## Running everything at once (summary)

Once the infrastructure is up (steps 1–3), open 4 terminals:

```sh
# Terminal 1
docker compose up -d --build   # if not already up

# Terminal 2
cd producer && uv run producer.py

# Terminal 3
cd api && uv run uvicorn main:app --reload

# Terminal 4
uv run streamlit run dashboard/app.py
```

Plus one one-off command to submit the Flink job (only needs to be run once
per cluster restart):

```sh
docker exec flink-jobmanager /opt/flink/bin/flink run -d -py /opt/flink/usrlib/job.py
```

## Known upstream issues already worked around in this repo

These were only discoverable by actually running the job end-to-end, and are
already fixed in `flink/job.py` / `flink/Dockerfile`:

- **`Types.TIMESTAMP()` doesn't exist** in `apache-flink==2.1.0`; the correct
  API is `Types.SQL_TIMESTAMP()`.
- **`flink-connector-kafka` doesn't bundle `kafka-clients`** — the Dockerfile
  downloads `kafka-clients-4.0.1.jar` separately.
- **`JdbcSink.sink()` is broken in PyFlink 2.x**: its Python wrapper reflects
  on a method (`JdbcOutputFormat.createRowJdbcStatementBuilder`) that every
  current `flink-connector-jdbc` release has moved to a different class
  (`RowJdbcOutputFormat`). `job.py` instead writes to Postgres with a custom
  `PostgresSink(MapFunction)` using `psycopg2` directly.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `flink run` hangs with no output | You forgot `-d` (detached mode); the CLI stays attached to streaming jobs by default. |
| `NoClassDefFoundError: org/apache/kafka/...` | Rebuild the Flink image (`docker compose build`) — the `kafka-clients` jar didn't get added. |
| `sensor_aggregates` table missing | Postgres volume was already initialized before `init.sql` was mounted. Run `docker compose down -v` and start again. |
| No rows in Postgres after 10+ minutes | Confirm the producer is running and the job is `RUNNING` with `numRecordsIn` increasing (step 5). Check `docker logs flink-taskmanager` for exceptions in `PostgresSink`. |
| Two "Sensor Analytics" jobs running | List and cancel the stale one: `docker exec flink-jobmanager /opt/flink/bin/flink list`, then `flink cancel <JOB_ID>`. |
| Dashboard shows "Waiting for data..." | The API returned no rows yet — the first 10-minute window hasn't closed, or the FastAPI/producer/Flink job isn't running. |
