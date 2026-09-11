FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cdc_producer.py .

CMD ["python", "-u", "cdc_producer.py"]
