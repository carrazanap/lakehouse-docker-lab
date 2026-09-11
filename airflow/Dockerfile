FROM apache/airflow:2.9.3-python3.10

USER root
# El nombre de la carpeta de instalacion de Java depende de la arquitectura
# (amd64/arm64), por eso resolvemos JAVA_HOME con un symlink dinamico.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps python3-venv && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -sfn "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Pre-crear el directorio con los permisos del usuario "airflow" (uid 50000,
# gid 0): el volumen nombrado usado para cachear los jars de Ivy/Maven se
# "pre-puebla" con el contenido/permisos que ya existan en esta ruta dentro
# de la imagen la primera vez que se monta; si no existiera, Docker lo crea
# vacio y root:root, y el contenedor (que corre como uid 50000) no podria
# escribir ahi.
RUN mkdir -p /home/airflow/.ivy2 && chown airflow:root /home/airflow/.ivy2 && chmod 0775 /home/airflow/.ivy2

USER airflow

# Entorno principal de Airflow: solo los providers (no pyspark/dbt aca).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --constraint \
      "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt" \
      -r /requirements.txt || \
    pip install --no-cache-dir -r /requirements.txt

# PySpark + dbt viven en un virtualenv aparte: apache-airflow-providers-databricks
# y dbt-databricks exigen versiones incompatibles de databricks-sql-connector,
# asi que no pueden convivir en el mismo entorno de pip que Airflow.
RUN python3 -m venv /home/airflow/spark_venv && \
    /home/airflow/spark_venv/bin/pip install --no-cache-dir \
        pyspark==3.5.1 \
        delta-spark==3.2.0 \
        dbt-core==1.8.6 \
        "dbt-spark[session]==1.8.0" \
        dbt-databricks==1.8.6
