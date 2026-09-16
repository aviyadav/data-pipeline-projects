from fastapi import FastAPI
import psycopg2

app = FastAPI()

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="streaming",
        user="flink",
        password="flink"
    )

@app.get("/sensors")
def sensors():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            device_id,
            window_start,
            window_end,
            avg_temperature,
            max_temperature,
            min_temperature,
            event_count
        FROM sensor_aggregates
        ORDER BY window_start DESC
        LIMIT 100
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {
            "device_id": row[0],
            "window_start": row[1],
            "window_end": row[2],
            "avg_temperature": row[3],
            "max_temperature": row[4],
            "min_temperature": row[5],
            "event_count": row[6]
        }
        for row in rows
    ]
