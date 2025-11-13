"""Scraping completo de músicas de um artista no Kworb (ex: Taylor Swift)."""

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from tqdm import tqdm

# 🔗 URL base de exemplo (Taylor Swift)
ARTIST_URL = "https://kworb.net/spotify/artist/06HL4z0CvFAxyc27GXpf02_songs.html"

# Banco local
DB_URL = "sqlite:///data/banco.db"
engine = create_engine(DB_URL)


def get_artist_songs(url: str) -> pd.DataFrame:
    """Raspa todas as músicas de um artista do Kworb e retorna um DataFrame."""
    response = requests.get(url, timeout=10, verify=False)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Encontra a tabela principal
    table = soup.find("table")
    if table is None:
        raise ValueError("Tabela de músicas não encontrada na página do artista.")

    # Extrai cabeçalhos
    headers = [th.text.strip() if th.text.strip() else f"col_{i}" for i, th in enumerate(table.find_all("th"))]
    rows = []
    for tr in tqdm(table.find_all("tr")[1:], desc="Lendo músicas"):
        cols = [td.text.strip() for td in tr.find_all("td")]
        if cols:
            rows.append(cols)

    df = pd.DataFrame(rows, columns=headers)

    # Renomeia e limpa colunas principais
    rename_map = {
        "Title": "title",
        "Streams": "streams_total",
        "Peak": "peak_position",
        "Days": "days_on_chart",
        "First": "first_seen",
        "Last": "last_seen",
        "Points": "chart_points",
    }

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Converte números
    for col in ["streams_total", "peak_position", "days_on_chart", "chart_points"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "")
                .replace("", "0")
                .astype(float)
                .astype(int)
            )

    # Adiciona artista e data da coleta
    artist_name = soup.find("h1").text.strip() if soup.find("h1") else "Desconhecido"
    df["artist"] = artist_name
    df["rank_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    return df


def main():
    """Executa o scraping e salva no banco."""
    print(f"Iniciando scraping de artista: {ARTIST_URL}")
    df = get_artist_songs(ARTIST_URL)

    # Formata colunas e salva
    df.to_csv("data/spotify_artist_songs.csv", index=False, encoding="utf-8")
    df.to_sql("spotify_artist_songs", engine, if_exists="replace", index=False)

    print(f"Scraping concluído: {len(df)} músicas coletadas.")
    return df


if __name__ == "__main__":
    main()
