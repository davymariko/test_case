FROM python:3.10-alpine

WORKDIR /app

COPY stock_analysis/main.py /app/main.py

COPY stock_analysis/data /app/data

WORKDIR /app

RUN pip install --no-cache-dir pandas

CMD ["python3", "main.py"]
