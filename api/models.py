from sqlalchemy import Column, Integer, String, Float
from data.db import Base

class Song(Base):
    __tablename__ = "songs"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    artist = Column(String, nullable=False)
    album = Column(String, nullable=True)
    duration = Column(Float, nullable=True)  # duração da música em minutos
