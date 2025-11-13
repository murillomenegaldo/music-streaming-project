from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from data.db import SessionLocal
from api import models, schemas

router = APIRouter(prefix="/songs", tags=["Songs"])

# Dependência para abrir e fechar sessão
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 🟢 Criar música
@router.post("/", response_model=schemas.Song)
def create_song(song: schemas.SongCreate, db: Session = Depends(get_db)):
    new_song = models.Song(**song.dict())
    db.add(new_song)
    db.commit()
    db.refresh(new_song)
    return new_song

# 🔵 Listar músicas
@router.get("/", response_model=list[schemas.Song])
def list_songs(db: Session = Depends(get_db)):
    return db.query(models.Song).all()

# 🟣 Buscar música por ID
@router.get("/{song_id}", response_model=schemas.Song)
def get_song(song_id: int, db: Session = Depends(get_db)):
    song = db.query(models.Song).filter(models.Song.id == song_id).first()
    if not song:
        raise HTTPException(status_code=404, detail="Música não encontrada")
    return song

# 🟠 Atualizar música
@router.put("/{song_id}", response_model=schemas.Song)
def update_song(song_id: int, updated: schemas.SongCreate, db: Session = Depends(get_db)):
    song = db.query(models.Song).filter(models.Song.id == song_id).first()
    if not song:
        raise HTTPException(status_code=404, detail="Música não encontrada")
    for key, value in updated.dict().items():
        setattr(song, key, value)
    db.commit()
    db.refresh(song)
    return song

# 🔴 Deletar música
@router.delete("/{song_id}")
def delete_song(song_id: int, db: Session = Depends(get_db)):
    song = db.query(models.Song).filter(models.Song.id == song_id).first()
    if not song:
        raise HTTPException(status_code=404, detail="Música não encontrada")
    db.delete(song)
    db.commit()
    return {"message": f"Música '{song.title}' deletada com sucesso."}

from fastapi import APIRouter
from scripts.scrapping import run_scraping

router = APIRouter(prefix="/songs", tags=["Songs"])

# ... (suas rotas anteriores aqui) ...



