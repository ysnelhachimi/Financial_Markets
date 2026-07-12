# Image du backend FastAPI.
# Contexte de build attendu : racine du dépôt (accès à packages/ et backend/).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dépendances système minimales pour psycopg2.
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Installe le package kanyon (données + pricer) puis les dépendances backend.
COPY packages/kanyon /app/packages/kanyon
RUN pip install --no-cache-dir -e "/app/packages/kanyon[postgres]"

COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

COPY backend /app/backend
COPY scripts /app/scripts
WORKDIR /app/backend

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
