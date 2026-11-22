FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline.py upsert.py analytics.py test_pipeline.py ./

RUN mkdir -p /app/data

CMD ["sh", "-c", "python pipeline.py && python analytics.py"]