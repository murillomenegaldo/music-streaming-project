"""
Scraping do Spotify Global Daily Top 200 via Kworb.
Compatível com Render + SQLite e integrado com a API.
"""

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from datetime import datetime

# URL fixa do Top 200 Global no Kworb
KWORB_URL = "https://kworb.net/spotify/daily/"

# Banco SQLite
DB_URL = "sqlite:///data/banco.db"
engine = create_engine(DB_URL)


def tratar_numero(valor):
    """Transforma números com vírgulas do Kworb em int."""
    try:
        return int(str(valor).replace(",", "").strip())
    except:
        return 0


def run_scraping():
    """Executa o scraping global do Top 200 e salva no banco."""
    print("\n[SCRAPING GLOBAL] Acessando:", KWORB_URL)

    response = requests.get(KWORB_URL, timeout=15, verify=False)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Tabela principal
    table = soup.find("table")
    if not table:
        raise ValueError("Tabela do Top 200 não encontrada!")

    headers = [th.text.strip() for th in table.find_all("th")]

    rows = []
    for tr in table.find_all("tr")[1:]:
        cols = [td.text.strip() for td in tr.find_all("td")]
        if cols:
            rows.append(cols)

    df = pd.DataFrame(rows, columns=headers)

    # Renomear colunas importantes
    rename_map = {
        "Pos": "position",
        "Artist and Title": "title",
        "Artist": "artist",
        "Title": "song_title",
        "Streams": "streams",
        "Total": "total_streams",
        "Days": "days_on_chart",
        "Peak": "peak_position",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Processar números
    for col in ["streams", "total_streams", "days_on_chart", "peak_position"]:
        if col in df.columns:
            df[col] = df[col].apply(tratar_numero)

    # Data da coleta
    df["rank_date"] = datetime.now().strftime("%Y-%m-%d")

    # Salvar CSV local para auditoria
    df.to_csv("data/spotify_global_daily.csv", index=False, encoding="utf-8")

    # Salvar no banco SQLite
    df.to_sql("spotify_global_daily", engine, if_exists="replace", index=False)

    print(f"[OK] Scraping global concluído: {len(df)} músicas salvas.\n")
    return {"total_musicas": len(df), "data": df["rank_date"].iloc[0]}
