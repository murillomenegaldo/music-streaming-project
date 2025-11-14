import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

DB_URL = "sqlite:///data/banco.db"
engine = create_engine(DB_URL)

def run_artist_scraping(url: str):
    print(f"🔍 Scraping artista: {url}")

    # Baixa página
    response = requests.get(url, timeout=15, verify=False)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Seleciona apenas a tabela de músicas (a segunda tabela da pagina)
    tables = soup.find_all("table")

    if len(tables) < 2:
        raise ValueError("❌ Não encontrei a tabela de músicas na página do artista.")

    music_table = tables[1]  # A tabela correta é sempre a segunda

    # Extrai cabeçalhos
    headers = [th.text.strip() for th in music_table.find_all("th")]

    rows = []
    for tr in music_table.find_all("tr")[1:]:
        cols = [td.text.strip() for td in tr.find_all("td")]
        if cols:
            rows.append(cols)

    df = pd.DataFrame(rows, columns=headers)

    # Limpa + converte colunas numéricas
    num_cols = ["Streams", "Daily", "Points", "Peak", "Days"]
    for col in num_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "")
                .replace("", "0")
                .astype(int)
            )

    # Nome do artista
    artist = soup.find("h1").text.strip() if soup.find("h1") else "Desconhecido"
    df["artist"] = artist
    df["rank_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    # Salva no banco
    df.to_sql("spotify_artist_songs", engine, if_exists="replace", index=False)

    print(f"✅ {len(df)} músicas salvas para o artista: {artist}")
    return len(df)
