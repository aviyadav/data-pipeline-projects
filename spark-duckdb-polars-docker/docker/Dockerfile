FROM apache/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark==3.5.0 pandas "duckdb<=1.1.3" polars

WORKDIR /opt/spark/work-dir

COPY scripts/ /opt/spark/work-dir/scripts/
COPY config/ /opt/spark/work-dir/config/

RUN chmod -R 755 /opt/spark/work-dir

USER spark