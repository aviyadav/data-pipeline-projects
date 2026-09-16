"""Sensor analytics job.

Kafka -> JSON -> Sensor Event -> Watermark -> keyBy(device_id)
-> 10-minute tumbling window -> AVG / MIN / MAX -> PostgreSQL.
"""
import json
from datetime import datetime, timezone

import psycopg2
from pyflink.common import (
    Duration,
    Row,
    Time,
    Types,
    WatermarkStrategy,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "streaming"
POSTGRES_USER = "flink"
POSTGRES_PASSWORD = "flink"

SENSOR_EVENT_TYPE = Types.ROW([
    Types.STRING(),         # device_id
    Types.SQL_TIMESTAMP(),  # timestamp
    Types.DOUBLE(),         # temperature
    Types.DOUBLE(),         # pressure
    Types.DOUBLE(),         # battery
])

# Matches the sensor_aggregates table in sql/init.sql
RESULT_TYPE = Types.ROW([
    Types.STRING(),         # device_id
    Types.SQL_TIMESTAMP(),  # window_start
    Types.SQL_TIMESTAMP(),  # window_end
    Types.DOUBLE(),         # avg_temperature
    Types.DOUBLE(),         # max_temperature
    Types.DOUBLE(),         # min_temperature
    Types.INT(),            # event_count
])

# (device_id, temperature_sum, event_count, min_temperature, max_temperature)
ACCUMULATOR_TYPE = Types.TUPLE([
    Types.STRING(),
    Types.DOUBLE(),
    Types.LONG(),
    Types.DOUBLE(),
    Types.DOUBLE(),
])


def parse_event(raw: str) -> Row:
    event = json.loads(raw)
    # Keep timestamps timezone-aware (UTC): PyFlink's TIMESTAMP type converter
    # only performs a timezone-safe conversion (calendar.timegm) for aware
    # datetimes. Naive datetimes are interpreted using the local system
    # timezone, which would silently corrupt the event time on a non-UTC host.
    ts = datetime.fromisoformat(event["timestamp"]).astimezone(timezone.utc)
    return Row(
        event["device_id"],
        ts,
        float(event["temperature"]),
        float(event["pressure"]),
        float(event["battery"]),
    )


def ms_to_utc(milliseconds: int) -> datetime:
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)


class EventTimeAssigner(TimestampAssigner):
    """Extracts the event time (ms since epoch) from the parsed Sensor Event."""

    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        # value[1] is a timezone-aware UTC datetime
        return int(value[1].timestamp() * 1000)


class TemperatureStats(AggregateFunction):
    """Incrementally accumulates the sum, count, min and max of temperature."""

    def create_accumulator(self):
        return ("", 0.0, 0, float("inf"), float("-inf"))

    def add(self, event: Row, acc):
        device_id, total, count, min_t, max_t = acc
        temperature = event[2]
        return (
            event[0],
            total + temperature,
            count + 1,
            min(min_t, temperature),
            max(max_t, temperature),
        )

    def get_result(self, acc):
        # Keep the accumulator shape; averages are computed in the window function
        return acc

    def merge(self, a, b):
        return (
            a[0] or b[0],  # empty string is falsy, so this picks whichever side has a key
            a[1] + b[1],
            a[2] + b[2],
            min(a[3], b[3]),
            max(a[4], b[4]),
        )


class ToSensorAggregateRow(ProcessWindowFunction):
    """Attaches window metadata and computes the final average."""

    def process(self, key, context, elements):
        device_id, total, count, min_t, max_t = elements[0]
        window = context.window()
        yield Row(
            device_id,
            ms_to_utc(window.start),
            ms_to_utc(window.end),
            total / count,
            max_t,
            min_t,
            count,
        )


class PostgresSink(MapFunction):
    """Writes each finalized window aggregate row to Postgres.

    NOTE: PyFlink's built-in JdbcSink.sink() is broken against every current
    flink-connector-jdbc release, including the officially matched
    flink-connector-jdbc-core:4.1.0-2.1 for Flink 2.1: its Python wrapper
    reflects on org.apache.flink.connector.jdbc.internal.JdbcOutputFormat
    looking for a static createRowJdbcStatementBuilder(int[]) method, but the
    connector moved that method to RowJdbcOutputFormat. Writing via psycopg2
    directly sidesteps that bug.
    """

    def open(self, runtime_context):
        self._conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        self._conn.autocommit = True
        self._cursor = self._conn.cursor()

    def close(self):
        self._cursor.close()
        self._conn.close()

    def map(self, value: Row) -> str:
        self._cursor.execute(
            "INSERT INTO sensor_aggregates "
            "(device_id, window_start, window_end, avg_temperature, "
            "max_temperature, min_temperature, event_count) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (value[0], value[1], value[2], value[3], value[4], value[5], value[6]),
        )
        return "{}:{}".format(value[0], value[1])


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics("sensor-events") \
        .set_group_id("sensor-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    raw_events = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source",
    )

    # JSON string -> Sensor Event
    events = raw_events.map(parse_event, output_type=SENSOR_EVENT_TYPE)

    # The event timestamp lives inside the parsed event, so watermarks are
    # assigned after parsing. Idleness keeps windows firing if a Kafka
    # partition goes quiet.
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_idleness(Duration.of_seconds(30)) \
        .with_timestamp_assigner(EventTimeAssigner())

    result = (
        events
        .assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda event: event[0], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(10)))
        .aggregate(
            TemperatureStats(),
            ToSensorAggregateRow(),
            accumulator_type=ACCUMULATOR_TYPE,
            output_type=RESULT_TYPE,
        )
    )

    result.map(PostgresSink(), output_type=Types.STRING()).print()

    env.execute("Sensor Analytics")


if __name__ == "__main__":
    main()
