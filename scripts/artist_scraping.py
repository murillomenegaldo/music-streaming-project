"""
Scraping completo das músicas de um artista no Kworb (tabela Songs).
Compatível com Render + SQLite + execução em background.
"""

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from datetime import datetime
from tqdm import tqdm

# Banco SQLite local
DB_URL = "sqlite:///data/banco.db"
engine = create_engine(DB_URL)


def tratar_numero(valor):
    """Converte valores numéricos do Kworb (com vírgulas) para int."""
    try:
        return int(str(valor).replace(",", "").strip())
    except:
        return 0


def get_artist_songs(url: str) -> pd.DataFrame:
    """Raspa TODAS as músicas da tabela Songs de um artista do Kworb."""
    print(f"\n[RASPAGEM] Acessando URL: {url}")

    response = requests.get(url, timeout=15, verify=False)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Encontrar tabela principal
    table = soup.find("table")
    if not table:
        raise ValueError("Tabela de músicas não encontrada na página!")

    # Cabeçalhos da tabela
    headers = [th.text.strip() if th.text.strip() else f"col_{i}"
               for i, th in enumerate(table.find_all("th"))]

    print(f"[INFO] Colunas encontradas: {headers}")

    # Linhas da tabela
    rows = []
    for tr in tqdm(table.find_all("tr")[1:], desc="Lendo músicas"):
        cols = [td.text.strip() for td in tr.find_all("td")]
        if cols:
            rows.append(cols)

    # Converter para DataFrame
    df = pd.DataFrame(rows, columns=headers)

    # Renomear colunas importantes
    rename_map = {
        "Title": "title",
        "Streams": "streams",
        "Peak": "peak_position",
        "Days": "days_on_chart",
        "First": "first_seen",
        "Last": "last_seen",
        "Points": "chart_points",
    }
    df = df.rename(columns=rename_map)

    # Converter colunas numéricas
    for col in ["streams", "peak_position", "days_on_chart", "chart_points"]:
        if col in df.columns:
            df[col] = df[col].apply(tratar_numero)

    # Nome do artista
    artist_name = soup.find("h1").text.strip() if soup.find("h1") else "Desconhecido"
    df["artist"] = artist_name

    # Data do scraping
    df["rank_date"] = datetime.now().strftime("%Y-%m-%d")

    return df


def run_artist_scraping(url: str):
    """Executa o scraping e salva no banco."""
    try:
        df = get_artist_songs(url)

        # Salvar CSV local
        df.to_csv("data/spotify_artist_songs.csv", index=False, encoding="utf-8")

        # Salvar no banco
        df.to_sql("spotify_artist_songs", engine, if_exists="replace", index=False)

        print(f"\n✔ Scraping finalizado ({len(df)} músicas) e salvo no banco.")
        return len(df)

    except Exception as e:
        print("\n❌ ERRO DURANTE O SCRAPING DO ARTISTA:")
        print(e)
        return None


# Execução direta pelo terminal
if __name__ == "__main__":
    URL_PADRAO = "https://kworb.net/spotify/artist/06HL4z0CvFAxyc27GXpf02_songs.html"
    run_artist_scraping(URL_PADRAO)
