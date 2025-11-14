from fastapi import APIRouter, Query, BackgroundTasks
from sqlalchemy import text
from data.db import engine
from scripts.scrapping import run_scraping
from scripts.artist_scraping import run_artist_scraping

router = APIRouter(prefix="/scraping", tags=["Scraping"])

# ==========================================================
# 🌎 SCRAPING GLOBAL (EM BACKGROUND)
# ==========================================================

def tarefa_scraping_global():
    try:
        run_scraping()
    except Exception as e:
        print("[ERRO NO SCRAPING GLOBAL]:", e)

@router.get("/", summary="Inicia o scraping do Kworb Global Top 200 (background)")
async def scraping_global(background: BackgroundTasks):
    background.add_task(tarefa_scraping_global)
    return {
        "mensagem": "Scraping global iniciado em background!",
        "status": "continue usando a API normalmente"
    }

# ==========================================================
# 📊 RANKING GLOBAL
# ==========================================================

@router.get("/ranking", summary="Lista músicas coletadas do Kworb")
def listar_ranking(artist: str | None = Query(None)):
    query = "SELECT * FROM spotify_global_daily"
    params = {}

    if artist:
        query += " WHERE artist LIKE :artist"
        params["artist"] = f"%{artist}%"

    query += " ORDER BY position ASC"

    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(text(query), params)]

    # formatação
    for r in rows:
        if "streams" in r and isinstance(r["streams"], (int, float)):
            r["streams"] = f"{r['streams']:,}".replace(",", ".")
        if "total_streams" in r and isinstance(r["total_streams"], (int, float)):
            r["total_streams"] = f"{r['total_streams']:,}".replace(",", ".")

    return rows

# ==========================================================
# 🎤 SCRAPING DE ARTISTA (BACKGROUND)
# ==========================================================

def tarefa_scraping_artista(url: str):
    try:
        run_artist_scraping(url)
    except Exception as e:
        print("[ERRO NO SCRAPING DO ARTISTA]:", e)

@router.get("/artist", summary="Inicia scraping de um artista (background)")
async def scraping_artista(
    background: BackgroundTasks,
    url: str = Query(..., description="URL do artista no Kworb")
):
    background.add_task(tarefa_scraping_artista, url)
    return {
        "mensagem": "Scraping de artista iniciado em background!",
        "processando": url,
        "status": "continue usando a API normalmente"
    }

# ==========================================================
# 🎧 RANKING DO ARTISTA
# ==========================================================

@router.get("/artist/ranking", summary="Lista músicas do artista raspadas")
def listar_musicas_artista(artist: str | None = Query(None)):
    query = "SELECT * FROM spotify_artist_songs WHERE 1=1"
    params = {}

    if artist:
        query += " AND artist LIKE :artist"
        params["artist"] = f"%{artist}%"

    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(text(query), params)]

    for r in rows:
        for campo in ["streams_total", "chart_points"]:
            if campo in r and isinstance(r[campo], int):
                r[campo] = f"{r[campo]:,}".replace(",", ".")

    return {
        "total_registros": len(rows),
        "dados": rows
    }
