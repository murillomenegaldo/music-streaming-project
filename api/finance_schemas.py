from pydantic import BaseModel
from datetime import date
from typing import Optional


class TransactionBase(BaseModel):
    date: date
    description: str
    amount: float
    category: str
    card: Optional[str] = "Bradesco VISA Infinite Prime"
    installment: Optional[str] = None


class TransactionCreate(TransactionBase):
    pass


class TransactionOut(TransactionBase):
    id: int

    class Config:
        from_attributes = True


class CategorySummary(BaseModel):
    category: str
    total: float
    count: int
    percentage: float


class MonthlySummary(BaseModel):
    month: str
    total: float
    count: int


class FinancialReport(BaseModel):
    total: float
    transaction_count: int
    by_category: list[CategorySummary]
    by_month: list[MonthlySummary]
    largest_expense: Optional[TransactionOut]
    average_transaction: float
