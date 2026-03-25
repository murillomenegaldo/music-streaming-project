from fastapi import FastAPI
from api import models, finance_models
from data.db import engine, Base
from api.routes import router as song_router
from api.scraping_routes import router as scraping_router
from api.debug_routes import router as debug_router
from api.finance_routes import router as finance_router
from scripts.seed_finances import seed as seed_finances

# Cria automaticamente o banco e as tabelas
Base.metadata.create_all(bind=engine)

# Popula transações financeiras se ainda não existirem
seed_finances()

app = FastAPI(title="Music Streaming API")

app.include_router(song_router)
app.include_router(scraping_router)
app.include_router(debug_router)
app.include_router(finance_router)

