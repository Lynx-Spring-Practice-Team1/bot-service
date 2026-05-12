from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.routers import bots
from app.services.bot_engine import stop_all_bots


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    stop_all_bots()


app = FastAPI(title="Bot Service", version="1.0.0", lifespan=lifespan)

app.include_router(bots.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
