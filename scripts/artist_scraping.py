"""Scraping completo de músicas de um artista no Kworb (ex: Taylor Swift)."""

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from tqdm import tqdm

# Banco SQLite
DB_URL = "sqlite:///data/banco.db"
engine = create_engine(DB_URL)


def get_artist_songs(url: str) -> pd.DataFrame:
    """Raspa todas as músicas de um artista do Kworb e retorna um DataFrame."""

    print(f"[SCRAPING] Acessando URL: {url}")

    response = requests.get(url, timeout=10, verify=False)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Encontra a tabela principal
    table = soup.find("table")
    if table is None:
        raise ValueError("Tabela de músicas não encontrada na página do artista.")

    # Extrai cabeçalhos
    headers = [
        th.text.strip() if th.text.strip() else f"col_{i}"
        for i, th in enumerate(table.find_all("th"))
    ]

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
                df[col]
                .astype(str)
                .str.replace(",", "")
                .replace("", "0")
                .astype(float)
                .astype(int)
            )

    # Nome do artista (H1 da página)
    artist_name = soup.find("h1").text.strip() if soup.find("h1") else "Desconhecido"
    df["artist"] = artist_name

    # Data da coleta
    df["rank_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    return df


# FUNÇÃO NOVA – usada pela API (background)
def run_artist_scraping(url: str) -> int:
    """
    Executa todo o scraping para qualquer artista e salva no banco.
    Retorna o número de músicas coletadas.
    """

    print(f"[SCRAPING] Iniciando scraping do artista: {url}")

    df = get_artist_songs(url)

    # Salvar CSV
    df.to_csv("data/spotify_artist_songs.csv", index=False, encoding="utf-8")

    # Salvar no SQLite
    df.to_sql("spotify_artist_songs", engine, if_exists="replace", index=False)

    print(f"[SCRAPING] Finalizado: {len(df)} músicas coletadas.")
    return len(df)


# Apenas se rodar pelo terminal
if __name__ == "__main__":
    run_artist_scraping("https://kworb.net/spotify/artist/06HL4z0CvFAxyc27GXpf02_songs.html")
