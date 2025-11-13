from fastapi import APIRouter
from sqlalchemy import text
from data.db import engine

router = APIRouter(prefix="/debug", tags=["Debug"])

@router.get("/tables")
def listar_tabelas():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
        return [row[0] for row in result]

@router.get("/artist/count")
def contar_artist_songs():
    with engine.connect() as conn:
        try:
            result = conn.execute(text("SELECT COUNT(*) FROM spotify_artist_songs;"))
            return {"total": result.fetchone()[0]}
        except Exception as e:
            return {"erro": str(e)}
