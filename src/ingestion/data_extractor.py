import requests
import pandas as pd
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import os
from abc import ABC, abstractmethod

class DataExtractor(ABC):
    """Abstract base class for data extractors"""
    
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data and return as DataFrame"""
        pass

class APIExtractor(DataExtractor):
    """Extract data from REST APIs"""
    
    def __init__(self, base_url: str, endpoint: str, params: Optional[Dict] = None):
        self.base_url = base_url
        self.endpoint = endpoint
        self.params = params or {}
        self.session = requests.Session()
    
    def extract(self) -> pd.DataFrame:
        """Extract data from API endpoint"""
        try:
            url = f"{self.base_url}{self.endpoint}"
            response = self.session.get(url, params=self.params)
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data)
            
            logging.info(f"Successfully extracted {len(df)} records from {url}")
            return df
            
        except requests.RequestException as e:
            logging.error(f"Error extracting data from API: {e}")
            raise

class CSVExtractor(DataExtractor):
    """Extract data from CSV files"""
    
    def __init__(self, file_path: str, encoding: str = 'utf-8'):
        self.file_path = file_path
        self.encoding = encoding
    
    def extract(self) -> pd.DataFrame:
        """Extract data from CSV file"""
        try:
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"CSV file not found: {self.file_path}")
            
            df = pd.read_csv(self.file_path, encoding=self.encoding)
            logging.info(f"Successfully extracted {len(df)} records from {self.file_path}")
            return df
            
        except Exception as e:
            logging.error(f"Error extracting data from CSV: {e}")
            raise

class JSONExtractor(DataExtractor):
    """Extract data from JSON files"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    def extract(self) -> pd.DataFrame:
        """Extract data from JSON file"""
        try:
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"JSON file not found: {self.file_path}")
            
            with open(self.file_path, 'r') as f:
                data = json.load(f)
            
            df = pd.DataFrame(data)
            logging.info(f"Successfully extracted {len(df)} records from {self.file_path}")
            return df
            
        except Exception as e:
            logging.error(f"Error extracting data from JSON: {e}")
            raise

class DatabaseExtractor(DataExtractor):
    """Extract data from relational databases"""
    
    def __init__(self, connection_string: str, query: str):
        self.connection_string = connection_string
        self.query = query
    
    def extract(self) -> pd.DataFrame:
        """Extract data from database using SQL query"""
        try:
            df = pd.read_sql(self.query, self.connection_string)
            logging.info(f"Successfully extracted {len(df)} records from database")
            return df
            
        except Exception as e:
            logging.error(f"Error extracting data from database: {e}")
            raise 