import streamlit as st
import requests
import pandas as pd
import plotly.express as px

st.title("Real-Time Sensor Dashboard")
response = requests.get(
    "http://localhost:8000/sensors"
)
data = response.json()
df = pd.DataFrame(data)
if not df.empty:
    df["window_start"] = pd.to_datetime(
        df["window_start"]
    )
    fig = px.line(
        df,
        x="window_start",
        y="avg_temperature",
        color="device_id",
        title="Average Temperature"
    )
    st.plotly_chart(
        fig,
        width="stretch"
    )
    st.dataframe(df)
else:
    st.info("Waiting for data...")
