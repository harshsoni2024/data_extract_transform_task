import sqlite3
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()

class DatabaseManager:
    def __init__(self, db_url="sqlite:///data_warehouse.db"):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        
    def create_tables(self):
        """Create all tables in the database"""
        Base.metadata.create_all(self.engine)
        logging.info("All tables created successfully")
    
    def get_session(self):
        """Get a new database session"""
        return self.Session()

# Dimension Tables
class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    
    customer_key = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(50), unique=True, nullable=False)
    customer_name = Column(String(100), nullable=False)
    email = Column(String(100))
    phone = Column(String(20))
    address = Column(Text)
    city = Column(String(50))
    state = Column(String(50))
    country = Column(String(50))
    postal_code = Column(String(20))
    effective_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime)
    is_current = Column(Boolean, default=True)
    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class DimProduct(Base):
    __tablename__ = 'dim_product'
    
    product_key = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String(50), unique=True, nullable=False)
    product_name = Column(String(100), nullable=False)
    category = Column(String(50))
    subcategory = Column(String(50))
    brand = Column(String(50))
    price = Column(Float)
    cost = Column(Float)
    effective_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime)
    is_current = Column(Boolean, default=True)
    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class DimDate(Base):
    __tablename__ = 'dim_date'
    
    date_key = Column(Integer, primary_key=True)
    date = Column(DateTime, unique=True, nullable=False)
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    month_name = Column(String(20))
    day = Column(Integer)
    day_of_week = Column(Integer)
    day_name = Column(String(20))
    is_weekend = Column(Boolean)
    is_holiday = Column(Boolean)

# Fact Tables
class FactOrder(Base):
    __tablename__ = 'fact_order'
    
    order_key = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(50), unique=True, nullable=False)
    customer_key = Column(Integer, nullable=False)
    product_key = Column(Integer, nullable=False)
    date_key = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    total_amount = Column(Float, nullable=False)
    discount_amount = Column(Float, default=0.0)
    tax_amount = Column(Float, default=0.0)
    shipping_amount = Column(Float, default=0.0)
    order_status = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)

class FactSales(Base):
    __tablename__ = 'fact_sales'
    
    sale_key = Column(Integer, primary_key=True, autoincrement=True)
    sale_id = Column(String(50), unique=True, nullable=False)
    customer_key = Column(Integer, nullable=False)
    product_key = Column(Integer, nullable=False)
    date_key = Column(Integer, nullable=False)
    quantity_sold = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    total_revenue = Column(Float, nullable=False)
    cost_of_goods = Column(Float)
    gross_profit = Column(Float)
    profit_margin = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow) 