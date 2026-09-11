FROM python:3.10-slim-bookworm

# Java es requerido por PySpark. Resolvemos JAVA_HOME con un symlink dinamico
# porque el path de instalacion depende de la arquitectura (amd64/arm64).
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -sfn "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /opt/notebooks

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8888

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--allow-root", "--ServerApp.token=", "--ServerApp.password="]
