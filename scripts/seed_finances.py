"""
Seed script: populates the transactions table with data from the
Bradesco VISA Infinite Prime statement (24/03/2026).
Run from the project root: python -m scripts.seed_finances
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.db import SessionLocal, engine, Base
from api import finance_models

Base.metadata.create_all(bind=engine)

TRANSACTIONS = [
    {
        "date": "2026-03-18",
        "description": "CLAUDE.AI SUBSCRIPTION",
        "amount": 110.00,
        "category": "Tecnologia",
        "installment": None,
    },
    {
        "date": "2026-03-16",
        "description": "PETLOVE SAUD*Petl",
        "amount": 107.91,
        "category": "Pet",
        "installment": None,
    },
    {
        "date": "2026-03-16",
        "description": "FIAP",
        "amount": 700.00,
        "category": "Educação",
        "installment": None,
    },
    {
        "date": "2026-03-15",
        "description": "Pier Seguradora",
        "amount": 90.40,
        "category": "Seguros",
        "installment": None,
    },
    {
        "date": "2026-03-03",
        "description": "PETLOVE SAUD*Petl",
        "amount": 142.36,
        "category": "Pet",
        "installment": None,
    },
    {
        "date": "2026-03-01",
        "description": "TIM*tim",
        "amount": 109.58,
        "category": "Telecomunicações",
        "installment": None,
    },
    {
        "date": "2026-02-12",
        "description": "AIRBNB * HMASQB5",
        "amount": 78.36,
        "category": "Viagem",
        "installment": "2/6",
    },
    {
        "date": "2026-02-07",
        "description": "AIRBNB * HMBKESH",
        "amount": 242.66,
        "category": "Viagem",
        "installment": "2/6",
    },
    {
        "date": "2026-01-26",
        "description": "SHOPEE *ELVORANewpet",
        "amount": 67.57,
        "category": "Compras",
        "installment": "2/6",
    },
    {
        "date": "2025-12-15",
        "description": "MERCADOLIVRE*MERCADOLIVRE",
        "amount": 52.24,
        "category": "Compras",
        "installment": "4/5",
    },
]

def seed():
    db = SessionLocal()
    try:
        existing = db.query(finance_models.Transaction).count()
        if existing > 0:
            print(f"Database already has {existing} transaction(s). Skipping seed.")
            return

        from datetime import date
        for t in TRANSACTIONS:
            year, month, day = map(int, t["date"].split("-"))
            obj = finance_models.Transaction(
                date=date(year, month, day),
                description=t["description"],
                amount=t["amount"],
                category=t["category"],
                card="Bradesco VISA Infinite Prime",
                installment=t.get("installment"),
            )
            db.add(obj)

        db.commit()
        print(f"Seeded {len(TRANSACTIONS)} transactions successfully.")
    finally:
        db.close()


if __name__ == "__main__":
    seed()
