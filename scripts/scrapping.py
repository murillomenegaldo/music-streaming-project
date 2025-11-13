"""Scraping do Spotify Global Daily (Kworb) com limpeza e persistência."""

# %%
import re
from typing import List

import certifi
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from tqdm import tqdm

KWORB_URL = "https://kworb.net/spotify/country/global_daily.html"
DB_URL = "sqlite:///data/banco.db"

engine = create_engine(DB_URL)

# %% Helpers ---------------------------------------------------------------

def fetch_html(url: str) -> str:
    """Baixa o HTML com verificação SSL; faz fallback para verify=False se necessário."""
    if not isinstance(url, str):
        raise TypeError("URL precisa ser string.")

    try:
        # Caminho mais seguro: usar a CA do certifi
        resp = requests.get(url, timeout=15, verify=certifi.where())
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.SSLError:
        # Fallback (menos seguro, mas útil quando a cadeia SSL falha)
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        resp = requests.get(url, timeout=15, verify=False)
        resp.raise_for_status()
        return resp.text

def scrapper(url: str) -> BeautifulSoup:
    """Retorna BeautifulSoup da página."""
    html = fetch_html(url)
    return BeautifulSoup(html, "html.parser")

def first_table_or_fail(soup: BeautifulSoup):
    """Pega a primeira <table> da página."""
    table = soup.find("table")
    if table is None:
        raise ValueError("Nenhuma <table> encontrada na página Kworb.")
    return table

def clean_int(val: str) -> int:
    """Converte string numérica da Kworb para int (remove vírgulas, sinais, etc.)."""
    if val is None:
        return 0
    s = str(val).strip()
    # remove tudo exceto dígitos
    s = re.sub(r"[^0-9]", "", s)
    return int(s) if s.isdigit() else 0

# %% Extração -------------------------------------------------------------

def get_spotify_table(soup: BeautifulSoup) -> pd.DataFrame:
    """Extrai a tabela principal do ranking em um DataFrame, com cabeçalhos dinâmicos."""
    table = first_table_or_fail(soup)

    # Cabeçalhos
    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    # Às vezes a linha de cabeçalho vem toda “grudada”. Mantemos mesmo assim.
    # Ex.: ["Pos", "P+", "Artist and Title", "Days", "Pk(x?)", "Streams", "Streams+",
    #       "7Day", "7Day+", "Total"]

    # Linhas
    rows: List[List[str]] = []
    trs = table.find_all("tr")
    for tr in tqdm(trs[1:], desc="Lendo linhas da tabela"):  # pula header
        tds = tr.find_all("td")
        if not tds:
            continue
        cols = [td.get_text(strip=True) for td in tds]
        # Garante comprimento (algumas linhas podem ter menos colunas)
        if len(cols) < len(headers):
            cols += [""] * (len(headers) - len(cols))
        rows.append(cols[: len(headers)])

    if not rows:
        raise ValueError("A tabela foi encontrada, mas não há linhas com dados.")

    df = pd.DataFrame(rows, columns=headers)

    return df

# %% Limpeza e padronização ----------------------------------------------

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza nomes que nos interessam. A Kworb varia um pouco os nomes.
    Tentamos mapear o que for possível e ignoramos o resto.
    """
    rename_map_candidates = [
        {
            "Pos": "position",
            "P+": "pos_change",                # pode existir
            "Artist and Title": "artist_title",
            "Days": "days",
            "Pk": "peak",
            "Pk(x?)": "peak",                 # variação
            "Streams": "streams",
            "Total": "total_streams",         # pode existir
        },
        # fallback mínimo
        {
            "Pos": "position",
            "Artist and Title": "artist_title",
            "Days": "days",
            "Streams": "streams",
        },
    ]

    for rmap in rename_map_candidates:
        intersect = {k: v for k, v in rmap.items() if k in df.columns}
        if intersect:
            df = df.rename(columns=intersect)

    return df

def split_artist_title(df: pd.DataFrame) -> pd.DataFrame:
    """Separa 'artist_title' em 'artist' e 'title' (formato 'ARTIST - TITLE' ou 'ARTIST-TITLE')."""
    if "artist_title" not in df.columns:
        possible = [c for c in df.columns if "Artist" in c and "Title" in c]
        if possible:
            df = df.rename(columns={possible[0]: "artist_title"})
        else:
            df["artist_title"] = ""

    # Permite dividir tanto em " - " quanto em "-"
    parts = df["artist_title"].str.split(r"\s*-\s*", n=1, expand=True)

    if parts.shape[1] == 2:
        df["artist"] = parts[0].str.strip()
        df["title"] = parts[1].str.strip()
    else:
        df["artist"] = df["artist_title"].str.strip()
        df["title"] = ""
    return df


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas numéricas com segurança."""
    for col in ["position", "days", "peak", "streams", "total_streams"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_int)
    return df

def finalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Seleciona/ordena colunas principais e adiciona data do ranking."""
    df["rank_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    wanted = ["position", "artist", "title", "streams", "days", "rank_date"]

    # inclui colunas extras, se existirem
    if "peak" in df.columns:
        wanted.insert(4, "peak")
    if "total_streams" in df.columns:
        wanted.insert(len(wanted) - 1, "total_streams")

    out = [c for c in wanted if c in df.columns]
    return df[out]

def clean_spotify_data(df: pd.DataFrame) -> pd.DataFrame:
    """Pipeline de limpeza completo."""
    df = normalize_columns(df)
    df = split_artist_title(df)
    df = clean_numeric_columns(df)
    df = finalize_columns(df)
    df["streams"] = df["streams"].apply(lambda x: f"{x:,}".replace(",", "."))
    return df

# %% Orquestração ---------------------------------------------------------

def main() -> pd.DataFrame:
    """Executa o scraping do Kworb e salva CSV + DB."""
    print("Iniciando scraping do Kworb...")
    soup = scrapper(KWORB_URL)
    raw = get_spotify_table(soup)
    df = clean_spotify_data(raw)

    # Persistência
    df.to_csv("data/spotify_global_daily.csv", index=False, encoding="utf-8")
    df.to_sql("spotify_global_daily", engine, if_exists="replace", index=False)

    print(f"Scraping concluído. Total de músicas: {len(df)}")
    return df

def run_scraping():
    """Para ser chamado pela API se quiser expor uma rota."""
    df = main()
    return {"total_musicas": len(df)}

# %% Execução direta ------------------------------------------------------

if __name__ == "__main__":
    main()
