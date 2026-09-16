## Run book

#### Step 1 — Project Structure

```text
flink-pipeline/
│
├── docker-compose.yml
│
├── producer/
│   └── producer.py
│
├── flink/
│   ├── Dockerfile
│   └── job.py
│
├── api/
│   └── main.py
│
├── dashboard/
│   └── app.py
│
└── sql/
    └── init.sql

```
#### Step 2 — Docker Compose: Kafka, Postgres, and Flink

docker-compose.yml

### Bring it up:

```
docker compose up -d
docker compose ps
```

#### Step 3 — Verify Each Piece Independently

Flink dashboard: open http://localhost:8081

VERIFY:l Apache Flink
Task Managers: 1
Slots Available: 4
Jobs Running: 0

POSTGRESQL:

```
docker exec -it postgres psql -U flink -d streaming
```

#### Step 4 — Create the Kafka Topic

```
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic sensor-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

confirm:

```
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

#### Step 5 — The Producer: Simulating a Sensor Fleet

Dependencies:

```
uv add kafka-python
```

producer/producer.py

** run producer **

```
uv run python producer\producer.py
```

#### Step 6 — Confirm Kafka Is Actually Receiving It

```
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor-events --from-beginning
```

### Python → Kafka → sensor-events

#### Step 7 — Giving Flink a Kafka + Postgres Connector

flink/Dockerfile

```
replace 

image: flink:2.1.0-scala_2.12-java17

with 
build:
    context: ./flink
    dockerfile: Dockerfile

```

Rebuild and restart:
```
docker compose down
docker compose build
docker compose up -d
```

#### Step 8 — The PostgreSQL Sink Table

sql/init.sql

load it

```
docker cp sql\init.sql postgres:/tmp/init.sql
docker exec postgres psql -U flink -d streaming -f /tmp/init.sql 
```
Verify the schema landed:

```
docker exec postgres psql -U flink -d streaming -c "\d sensor_aggregates"
```

#### Step 9 — The Flink Job: Windows, Keys, and Aggregation

flink/job.py

Kafka
  │
  ▼
JSON
  │
  ▼
Sensor Event
  │
  ▼
Watermark
  │
  ▼
keyBy(device_id)
  │
  ▼
10-minute window
  │
  ▼
AVG / MIN / MAX
  │
  ▼
PostgreSQL


#### Step 10 — FastAPI: Serving the Aggregates

Add dependencies:
```
uv add fastapi uvicorn psycopg2-binary
```

code : api/main.py

Run the FastAPI server:
``` 
uv run uvicorn main:app --reload
```


visit - http://localhost:8000/sensors


#### Step 11 — The Live Dashboard


add dependencies:
```
uv add streamlit plotly requests pandas
```


dashboard/app.py

Run the dashboard:
```
uv run streamlit run dashboard/app.py
```

http://localhost:8501


#### Step 12 — Running the Full System

```
Terminal 1 - docker compose up
Terminal 2 - uv run python producer/producer.py
Terminal 3 - uv run uvicorn main:app --reload
Terminal 4 - uv run streamlit run dashboard/app.py
```


The complete project:

```text
SENSOR
                      │
                      │ JSON
                      ▼
             ┌─────────────────┐
             │      KAFKA      │
             │  sensor-events  │
             └────────┬────────┘
                      │ stream
                      ▼
             ┌─────────────────┐
             │      FLINK      │
             │  Parse          │
             │  Validate       │
             │  Watermarks     │
             │  KeyBy          │
             │  Windows        │
             │  Aggregation    │
             │  Anomaly        │
             └────────┬────────┘
                      │ aggregated events
                      ▼
             ┌─────────────────┐
             │   POSTGRESQL    │
             │ sensor_         │
             │ aggregates      │
             └────────┬────────┘
                      ▼
               ┌─────────────┐
               │   FASTAPI   │
               └──────┬──────┘
                      ▼
               ┌─────────────┐
               │  STREAMLIT  │
               │  DASHBOARD  │
               └─────────────┘
```
