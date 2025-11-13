from pydantic import BaseModel

# 🔹 Classe base com os campos comuns
class SongBase(BaseModel):
    title: str
    artist: str
    album: str | None = None
    duration: float | None = None

# 🔹 Modelo de criação (entrada)
class SongCreate(SongBase):
    pass

# 🔹 Modelo de leitura (saída)
class Song(SongBase):
    id: int

    class Config:
        orm_mode = True
