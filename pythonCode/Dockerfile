FROM apache/airflow:2.11.0

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==2.11.0" \
    "pyspark==3.5.3" \
    "delta-spark==3.2.0" \
    python-dotenv \
    apache-airflow-providers-microsoft-mssql \
    apache-airflow-providers-postgres
