from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import date
from typing import Optional
import os

from data.db import get_db
from api import finance_models
from api.finance_schemas import (
    TransactionCreate,
    TransactionOut,
    CategorySummary,
    MonthlySummary,
    FinancialReport,
)

router = APIRouter(prefix="/finances", tags=["Finances"])


@router.post("/transactions", response_model=TransactionOut, status_code=201)
def create_transaction(payload: TransactionCreate, db: Session = Depends(get_db)):
    transaction = finance_models.Transaction(**payload.model_dump())
    db.add(transaction)
    db.commit()
    db.refresh(transaction)
    return transaction


@router.get("/transactions", response_model=list[TransactionOut])
def list_transactions(
    category: Optional[str] = None,
    db: Session = Depends(get_db),
):
    query = db.query(finance_models.Transaction)
    if category:
        query = query.filter(finance_models.Transaction.category == category)
    return query.order_by(finance_models.Transaction.date.desc()).all()


@router.get("/reports", response_model=FinancialReport)
def get_report(db: Session = Depends(get_db)):
    transactions = db.query(finance_models.Transaction).all()

    if not transactions:
        raise HTTPException(status_code=404, detail="No transactions found")

    total = sum(t.amount for t in transactions)
    avg = total / len(transactions)

    # By category
    cat_data: dict[str, dict] = {}
    for t in transactions:
        if t.category not in cat_data:
            cat_data[t.category] = {"total": 0.0, "count": 0}
        cat_data[t.category]["total"] += t.amount
        cat_data[t.category]["count"] += 1

    by_category = [
        CategorySummary(
            category=cat,
            total=round(data["total"], 2),
            count=data["count"],
            percentage=round((data["total"] / total) * 100, 1),
        )
        for cat, data in sorted(cat_data.items(), key=lambda x: x[1]["total"], reverse=True)
    ]

    # By month
    month_data: dict[str, dict] = {}
    for t in transactions:
        key = t.date.strftime("%Y-%m")
        if key not in month_data:
            month_data[key] = {"total": 0.0, "count": 0}
        month_data[key]["total"] += t.amount
        month_data[key]["count"] += 1

    by_month = [
        MonthlySummary(month=month, total=round(data["total"], 2), count=data["count"])
        for month, data in sorted(month_data.items())
    ]

    largest = max(transactions, key=lambda t: t.amount)

    return FinancialReport(
        total=round(total, 2),
        transaction_count=len(transactions),
        by_category=by_category,
        by_month=by_month,
        largest_expense=largest,
        average_transaction=round(avg, 2),
    )


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(db: Session = Depends(get_db)):
    dashboard_path = os.path.join(os.path.dirname(__file__), "..", "dashboard", "index.html")
    with open(dashboard_path, encoding="utf-8") as f:
        return HTMLResponse(content=f.read())
