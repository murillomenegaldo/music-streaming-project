from fastapi import APIRouter, Query, BackgroundTasks
from sqlalchemy import text
from data.db import engine
from scripts.scrapping import run_scraping
from scripts.artist_scraping import run_artist_scraping

router = APIRouter(prefix="/scraping", tags=["Scraping"])

# ==========================================================
# 🌎 SCRAPING GLOBAL DO KWORB
# ==========================================================
@router.get("/", summary="Executa o scraping do Kworb Global Top 200")
def scraping_data():
    result = run_scraping()
    return {
        "mensagem": "Scraping executado com sucesso!",
        "dados": result
    }


# ==========================================================
# 📊 RANKING GLOBAL - TOP 200
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

    # Formatar números
    for r in rows:
        if "streams" in r:
            try:
                r["streams"] = f"{int(r['streams']):,}".replace(",", ".")
            except:
                pass

        if "total_streams" in r:
            try:
                r["total_streams"] = f"{int(r['total_streams']):,}".replace(",", ".")
            except:
                pass

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
    url: str = Query(
        "https://kworb.net/spotify/artist/06HL4z0CvFAxyc27GXpf02_songs.html",
        description="URL do artista no Kworb"
    )
):
    background.add_task(tarefa_scraping_artista, url)

    return {
        "mensagem": "Scraping de artista iniciado em background!",
        "processando": url,
        "status": "continue usando a API normalmente"
    }


# ==========================================================
# 🎧 RANKING DO ARTISTA — TABELA COMPLETA 'Songs'
# ==========================================================
@router.get("/artist/ranking", summary="Lista TODAS as músicas do artista raspadas")
def listar_estatisticas_artista(artist: str | None = Query(None)):
    query = "SELECT * FROM spotify_artist_songs WHERE 1=1"
    params = {}

    if artist:
        query += " AND artist LIKE :artist"
        params["artist"] = f"%{artist}%"

    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(text(query), params)]

    # Formatar números da tabela Songs
    for r in rows:
        for campo in ["streams", "peak_position", "days_on_chart", "chart_points"]:
            if campo in r:
                try:
                    r[campo] = f"{int(r[campo]):,}".replace(",", ".")
                except:
                    pass

    return {
        "total_registros": len(rows),
        "dados": rows
    }
