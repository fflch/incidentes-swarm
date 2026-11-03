FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir docker mysql-connector-python python-dateutil

COPY monitor.py .

CMD ["python", "-u", "monitor.py"]