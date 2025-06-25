import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from abc import ABC, abstractmethod

class DataTransformer(ABC):
    """Abstract base class for data transformers"""
    
    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform data and return processed DataFrame"""
        pass

class CustomerTransformer(DataTransformer):
    """Transform customer data for dimension table"""
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform customer data"""
        try:
            # Create a copy to avoid modifying original data
            df = data.copy()
            
            # Standardize column names
            column_mapping = {
                'id': 'customer_id',
                'name': 'customer_name',
                'email': 'email',
                'phone': 'phone',
                'address': 'address',
                'city': 'city',
                'state': 'state',
                'country': 'country',
                'postal_code': 'postal_code'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Ensure required columns exist
            required_columns = ['customer_id', 'customer_name']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Required column {col} not found in data")
            
            # Clean and validate data
            df['customer_name'] = df['customer_name'].str.strip()
            df['email'] = df['email'].str.lower().str.strip()
            
            # Handle missing values
            df['phone'] = df['phone'].fillna('')
            df['address'] = df['address'].fillna('')
            df['city'] = df['city'].fillna('')
            df['state'] = df['state'].fillna('')
            df['country'] = df['country'].fillna('')
            df['postal_code'] = df['postal_code'].fillna('')
            
            # Add metadata columns
            df['effective_date'] = datetime.utcnow()
            df['is_current'] = True
            df['version'] = 1
            df['created_at'] = datetime.utcnow()
            df['updated_at'] = datetime.utcnow()
            
            logging.info(f"Successfully transformed {len(df)} customer records")
            return df
            
        except Exception as e:
            logging.error(f"Error transforming customer data: {e}")
            raise

class ProductTransformer(DataTransformer):
    """Transform product data for dimension table"""
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform product data"""
        try:
            df = data.copy()
            
            # Standardize column names
            column_mapping = {
                'id': 'product_id',
                'name': 'product_name',
                'category': 'category',
                'subcategory': 'subcategory',
                'brand': 'brand',
                'price': 'price',
                'cost': 'cost'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Ensure required columns exist
            required_columns = ['product_id', 'product_name']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Required column {col} not found in data")
            
            # Clean and validate data
            df['product_name'] = df['product_name'].str.strip()
            df['category'] = df['category'].str.strip()
            df['brand'] = df['brand'].str.strip()
            
            # Convert price and cost to numeric
            df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0.0)
            df['cost'] = pd.to_numeric(df['cost'], errors='coerce').fillna(0.0)
            
            # Handle missing values
            df['subcategory'] = df['subcategory'].fillna('')
            
            # Add metadata columns
            df['effective_date'] = datetime.utcnow()
            df['is_current'] = True
            df['version'] = 1
            df['created_at'] = datetime.utcnow()
            df['updated_at'] = datetime.utcnow()
            
            logging.info(f"Successfully transformed {len(df)} product records")
            return df
            
        except Exception as e:
            logging.error(f"Error transforming product data: {e}")
            raise

class OrderTransformer(DataTransformer):
    """Transform order data for fact table"""
    
    def __init__(self, customer_dim_df: pd.DataFrame, product_dim_df: pd.DataFrame):
        self.customer_dim_df = customer_dim_df
        self.product_dim_df = product_dim_df
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform order data"""
        try:
            df = data.copy()
            
            # Standardize column names
            column_mapping = {
                'id': 'order_id',
                'customer_id': 'customer_id',
                'product_id': 'product_id',
                'quantity': 'quantity',
                'unit_price': 'unit_price',
                'order_date': 'order_date',
                'status': 'order_status'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Ensure required columns exist
            required_columns = ['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Required column {col} not found in data")
            
            # Convert data types
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(1)
            df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce').fillna(0.0)
            
            # Calculate derived fields
            df['total_amount'] = df['quantity'] * df['unit_price']
            df['discount_amount'] = 0.0  # Default value
            df['tax_amount'] = df['total_amount'] * 0.1  # 10% tax
            df['shipping_amount'] = 5.0  # Fixed shipping cost
            
            # Convert order_date to datetime
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
            df['date_key'] = df['order_date'].dt.strftime('%Y%m%d').astype(int)
            
            # Join with dimension tables to get surrogate keys
            df = df.merge(
                self.customer_dim_df[['customer_id', 'customer_key']], 
                on='customer_id', 
                how='left'
            )
            
            df = df.merge(
                self.product_dim_df[['product_id', 'product_key']], 
                on='product_id', 
                how='left'
            )
            
            # Handle missing dimension keys
            df = df.dropna(subset=['customer_key', 'product_key'])
            
            # Add metadata
            df['created_at'] = datetime.utcnow()
            
            logging.info(f"Successfully transformed {len(df)} order records")
            return df
            
        except Exception as e:
            logging.error(f"Error transforming order data: {e}")
            raise

class DateDimensionTransformer(DataTransformer):
    """Generate date dimension table"""
    
    def __init__(self, start_date: str, end_date: str):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
    
    def transform(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """Generate date dimension data"""
        try:
            # Generate date range
            date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
            
            df = pd.DataFrame({'date': date_range})
            
            # Extract date components
            df['date_key'] = df['date'].dt.strftime('%Y%m%d').astype(int)
            df['year'] = df['date'].dt.year
            df['quarter'] = df['date'].dt.quarter
            df['month'] = df['date'].dt.month
            df['month_name'] = df['date'].dt.strftime('%B')
            df['day'] = df['date'].dt.day
            df['day_of_week'] = df['date'].dt.dayofweek
            df['day_name'] = df['date'].dt.strftime('%A')
            df['is_weekend'] = df['date'].dt.dayofweek.isin([5, 6])
            df['is_holiday'] = False  # Could be enhanced with holiday calendar
            
            logging.info(f"Successfully generated {len(df)} date dimension records")
            return df
            
        except Exception as e:
            logging.error(f"Error generating date dimension: {e}")
            raise 