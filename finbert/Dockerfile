FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements_finbert.txt .
RUN pip install --no-cache-dir -r requirements_finbert.txt

COPY finbert_api.py ./finbert_api.py
COPY finbert_model ./finbert_model

EXPOSE 8080

CMD ["uvicorn", "finbert_api:app", "--host", "0.0.0.0", "--port", "8080"]
