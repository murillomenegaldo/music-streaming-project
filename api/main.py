from fastapi import FastAPI
from api import models
from data.db import engine
from api.routes import router as song_router
from api.scraping_routes import router as scraping_router

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Music Streaming API")

app.include_router(song_router)
app.include_router(scraping_router)
