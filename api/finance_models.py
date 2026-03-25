from sqlalchemy import Column, Integer, String, Float, Date
from data.db import Base


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False)
    description = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    category = Column(String, nullable=False)
    card = Column(String, nullable=True, default="Bradesco VISA Infinite Prime")
    installment = Column(String, nullable=True)  # e.g. "2/6"
