FROM flink:2.1.0-scala_2.12-java17
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.1-2.0/flink-connector-kafka-4.0.1-2.0.jar
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar
RUN wget -P /opt/flink/lib \
    https://jdbc.postgresql.org/download/postgresql-42.7.7.jar
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.0.1/kafka-clients-4.0.1.jar

# Python + PyFlink runtime, required to run job.py.
# The apache-flink version below must match the Flink version above.
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir --timeout 180 --retries 10 apache-flink==2.1.0 psycopg2-binary
