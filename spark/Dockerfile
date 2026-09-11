FROM python:3.10-slim-bookworm

# Java es requerido por PySpark. El nombre del paquete/carpeta de instalacion
# depende de la arquitectura (amd64/arm64), por eso resolvemos JAVA_HOME con
# un symlink dinamico en vez de hardcodear la ruta.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -sfn "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /opt/spark

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY jobs/ /opt/spark/jobs/
COPY conf/spark-defaults.conf /opt/spark-conf/spark-defaults.conf
ENV SPARK_CONF_DIR=/opt/spark-conf

# Cache de paquetes ivy/maven para no re-descargar delta/kafka en cada arranque
ENV SPARK_JARS_IVY=/opt/ivy2

CMD ["/bin/bash"]
