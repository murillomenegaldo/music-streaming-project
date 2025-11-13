from sqlalchemy.orm import Session
from data.db import SessionLocal
from api.models import Song

# 🟢 Criar música
def create_song(title, artist, album=None, duration=None):
    session = SessionLocal()
    new_song = Song(title=title, artist=artist, album=album, duration=duration)
    session.add(new_song)
    session.commit()
    session.refresh(new_song)
    session.close()
    return new_song

# 🔵 Listar músicas
def get_songs():
    session = SessionLocal()
    songs = session.query(Song).all()
    session.close()
    return songs

# 🟠 Atualizar música
def update_song(song_id, title=None, artist=None, album=None, duration=None):
    session = SessionLocal()
    song = session.query(Song).filter(Song.id == song_id).first()
    if song:
        if title:
            song.title = title
        if artist:
            song.artist = artist
        if album:
            song.album = album
        if duration:
            song.duration = duration
        session.commit()
        session.refresh(song)
    session.close()
    return song

# 🔴 Deletar música
def delete_song(song_id):
    session = SessionLocal()
    song = session.query(Song).filter(Song.id == song_id).first()
    if song:
        session.delete(song)
        session.commit()
    session.close()
    return song
