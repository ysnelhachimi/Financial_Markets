"""Point d'entrée de l'application FastAPI.

Lancement en développement :
    uvicorn app.main:app --reload
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import init_db
from app.routers import auth, billing, market, plans

settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Crée les tables applicatives et amorce les plans au démarrage.
    init_db()
    yield


app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(plans.router)
app.include_router(billing.router)
app.include_router(market.router)


@app.get("/api/health", tags=["health"])
def health() -> dict[str, str]:
    """Sonde de disponibilité."""
    return {"status": "ok", "app": settings.app_name}
