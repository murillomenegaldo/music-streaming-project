from fastapi import APIRouter, Query
from sqlalchemy import text
from data.db import engine
from scripts.scrapping import run_scraping
import subprocess

router = APIRouter(prefix="/scraping", tags=["Scraping"])


# ==========================================================
# 🌎 SCRAPING GLOBAL DO KWORB
# ==========================================================
@router.get("/", summary="Executa o scraping do Kworb Global Top 200")
def scraping_data():
    """Executa o scraping do Kworb e retorna o total de músicas coletadas."""
    result = run_scraping()
    return {
        "mensagem": "Scraping executado com sucesso!",
        "dados": result
    }


# ==========================================================
# 📊 RANKING GLOBAL - TOP 200
# ==========================================================
@router.get("/ranking", summary="Lista músicas coletadas do Kworb")
def listar_ranking(artist: str | None = Query(None, description="Filtrar por nome do artista")):
    """Retorna as músicas armazenadas na tabela spotify_global_daily, com opção de filtrar por artista."""
    query = "SELECT * FROM spotify_global_daily"
    params = {}

    if artist:
        query += " WHERE artist LIKE :artist"
        params["artist"] = f"%{artist}%"

    query += " ORDER BY position ASC"

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = [dict(row._mapping) for row in result]

    # ✅ Formatar streams e total_streams com separador de milhar (pontuação)
    for r in rows:
        if "streams" in r and isinstance(r["streams"], (int, float)):
            r["streams"] = f"{r['streams']:,}".replace(",", ".")
        if "total_streams" in r and isinstance(r["total_streams"], (int, float)):
            r["total_streams"] = f"{r['total_streams']:,}".replace(",", ".")

    return rows


# ==========================================================
# 🎤 SCRAPING DE ARTISTA
# ==========================================================
@router.get("/artist", summary="Executa o scraping completo de um artista do Kworb")
def scraping_artista(
    url: str = Query(
        "https://kworb.net/spotify/artist/06HL4z0CvFAxyc27GXpf02_songs.html",
        description="URL do artista no Kworb (ex: https://kworb.net/spotify/artist/ID_songs.html)",
    )
):
    """
    Executa o scraping completo de todas as músicas de um artista no Kworb.
    - Faz a coleta da página de músicas do artista.
    - Salva os dados no banco (tabela spotify_artist_songs).
    - Retorna um resumo com total de músicas coletadas.
    """
    try:
        print(f"Iniciando scraping via API: {url}")
        import sys
        result = subprocess.run(
            [sys.executable, "-m", "scripts.artist_scraping"],
            capture_output=True,
            text=True,
            check=True,
        )
        return {
            "mensagem": "Scraping do artista executado com sucesso!",
            "saida": result.stdout.splitlines()[-10:],  # mostra as últimas linhas do log
        }
    except subprocess.CalledProcessError as e:
        return {"erro": "Falha ao executar scraping", "detalhes": e.stderr}



# ==========================================================
# 🎧 RANKING DE MÚSICAS DO ARTISTA
# ==========================================================
@router.get("/artist/ranking", summary="Lista as estatísticas de artistas (tabela spotify_artist_songs)")
def listar_estatisticas_artista(
    artist: str | None = Query(None, description="Filtrar por nome do artista")
):
    """
    Retorna os dados da tabela `spotify_artist_songs`,
    contendo totais de streams e participações (lead, solo, feature).
    """
    query = "SELECT * FROM spotify_artist_songs WHERE 1=1"
    params = {}

    if artist:
        query += " AND artist LIKE :artist"
        params["artist"] = f"%{artist}%"

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = [dict(row._mapping) for row in result]

    # ✅ Formatar os números com pontuação
    for r in rows:
        for campo in ["Total", "As lead", "Solo", "As feature"]:
            if campo in r and isinstance(r[campo], str) and r[campo].replace(",", "").isdigit():
                valor = int(r[campo].replace(",", ""))
                r[campo] = f"{valor:,}".replace(",", ".")

    return {
        "total_registros": len(rows),
        "dados": rows
    }

