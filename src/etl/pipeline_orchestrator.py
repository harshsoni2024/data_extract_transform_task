import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import schedule
import time
import yaml
import os

from src.models.database import DatabaseManager
from src.ingestion.data_extractor import APIExtractor, CSVExtractor, JSONExtractor
from src.transformation.data_transformer import (
    CustomerTransformer, ProductTransformer, OrderTransformer, DateDimensionTransformer
)
from src.loading.data_loader import DataLoader
from src.scd.scd_manager import SCDManager

class ETLPipelineOrchestrator:
    """Orchestrates the entire ETL pipeline"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = self._load_config(config_path)
        self.db_manager = DatabaseManager()
        self.session = self.db_manager.get_session()
        self.data_loader = DataLoader(self.session)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize transformers
        self.customer_transformer = CustomerTransformer()
        self.product_transformer = ProductTransformer()
        self.date_transformer = DateDimensionTransformer(
            start_date="2020-01-01",
            end_date="2025-12-31"
        )
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logging.error(f"Error loading config: {e}")
            raise
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        log_file = log_config.get('file', 'logs/data_warehouse.log')
        
        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def run_full_pipeline(self):
        """Run the complete ETL pipeline"""
        try:
            logging.info("Starting full ETL pipeline")
            
            # Step 1: Create tables if they don't exist
            self.db_manager.create_tables()
            
            # Step 2: Load date dimension (one-time setup)
            self._load_date_dimension()
            
            # Step 3: Extract and load customer data
            self._process_customer_data()
            
            # Step 4: Extract and load product data
            self._process_product_data()
            
            # Step 5: Extract and load order data
            self._process_order_data()
            
            logging.info("Full ETL pipeline completed successfully")
            
        except Exception as e:
            logging.error(f"Error in full ETL pipeline: {e}")
            raise
    
    def run_incremental_pipeline(self):
        """Run incremental ETL pipeline"""
        try:
            logging.info("Starting incremental ETL pipeline")
            
            # Process only new/changed data
            self._process_customer_data_incremental()
            self._process_product_data_incremental()
            self._process_order_data_incremental()
            
            logging.info("Incremental ETL pipeline completed successfully")
            
        except Exception as e:
            logging.error(f"Error in incremental ETL pipeline: {e}")
            raise
    
    def _load_date_dimension(self):
        """Load date dimension table"""
        try:
            logging.info("Loading date dimension")
            
            # Generate date dimension data
            date_data = self.date_transformer.transform()
            
            # Load into database
            from src.models.database import DimDate
            self.data_loader.load_fact_table(DimDate, date_data)
            
        except Exception as e:
            logging.error(f"Error loading date dimension: {e}")
            raise
    
    def _process_customer_data(self):
        """Process customer data"""
        try:
            logging.info("Processing customer data")
            
            # Extract customer data from API
            api_config = self.config['data_sources']['api']
            extractor = APIExtractor(
                base_url=api_config['base_url'],
                endpoint=api_config['endpoints']['users']
            )
            
            customer_data = extractor.extract()
            
            # Transform customer data
            transformed_data = self.customer_transformer.transform(customer_data)
            
            # Load customer dimension with SCD Type 2
            from src.models.database import DimCustomer
            tracking_columns = ['customer_name', 'email', 'phone', 'address']
            self.data_loader.load_dimension_table(
                DimCustomer, 
                transformed_data, 
                scd_type="type_2",
                tracking_columns=tracking_columns
            )
            
        except Exception as e:
            logging.error(f"Error processing customer data: {e}")
            raise
    
    def _process_product_data(self):
        """Process product data"""
        try:
            logging.info("Processing product data")
            
            # For demo purposes, create sample product data
            # In real scenario, this would come from actual data source
            product_data = pd.DataFrame({
                'product_id': range(1, 101),
                'product_name': [f'Product {i}' for i in range(1, 101)],
                'category': ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'] * 20,
                'subcategory': ['Laptops', 'Phones', 'Shirts', 'Pants', 'Fiction'] * 20,
                'brand': ['Brand A', 'Brand B', 'Brand C', 'Brand D', 'Brand E'] * 20,
                'price': [100 + i * 10 for i in range(100)],
                'cost': [50 + i * 5 for i in range(100)]
            })
            
            # Transform product data
            transformed_data = self.product_transformer.transform(product_data)
            
            # Load product dimension with SCD Type 2
            from src.models.database import DimProduct
            tracking_columns = ['product_name', 'category', 'brand', 'price']
            self.data_loader.load_dimension_table(
                DimProduct, 
                transformed_data, 
                scd_type="type_2",
                tracking_columns=tracking_columns
            )
            
        except Exception as e:
            logging.error(f"Error processing product data: {e}")
            raise
    
    def _process_order_data(self):
        """Process order data"""
        try:
            logging.info("Processing order data")
            
            # Get dimension data for joins
            customer_dim = pd.read_sql(
                "SELECT customer_id, customer_key FROM dim_customer WHERE is_current = 1",
                self.db_manager.engine
            )
            
            product_dim = pd.read_sql(
                "SELECT product_id, product_key FROM dim_product WHERE is_current = 1",
                self.db_manager.engine
            )
            
            # Create sample order data
            # In real scenario, this would come from actual data source
            order_data = pd.DataFrame({
                'order_id': [f'ORD{i:06d}' for i in range(1, 1001)],
                'customer_id': np.random.choice(customer_dim['customer_id'], 1000),
                'product_id': np.random.choice(product_dim['product_id'], 1000),
                'quantity': np.random.randint(1, 10, 1000),
                'unit_price': np.random.uniform(10, 500, 1000),
                'order_date': pd.date_range('2023-01-01', periods=1000, freq='D'),
                'status': np.random.choice(['Completed', 'Pending', 'Cancelled'], 1000)
            })
            
            # Transform order data
            order_transformer = OrderTransformer(customer_dim, product_dim)
            transformed_data = order_transformer.transform(order_data)
            
            # Load order fact table
            from src.models.database import FactOrder
            self.data_loader.load_fact_table(FactOrder, transformed_data, incremental_key='order_id')
            
        except Exception as e:
            logging.error(f"Error processing order data: {e}")
            raise
    
    def _process_customer_data_incremental(self):
        """Process customer data incrementally"""
        # Similar to _process_customer_data but with incremental logic
        pass
    
    def _process_product_data_incremental(self):
        """Process product data incrementally"""
        # Similar to _process_product_data but with incremental logic
        pass
    
    def _process_order_data_incremental(self):
        """Process order data incrementally"""
        # Similar to _process_order_data but with incremental logic
        pass
    
    def schedule_pipeline(self, schedule_time: str = "00:00"):
        """Schedule the pipeline to run daily"""
        schedule.every().day.at(schedule_time).do(self.run_incremental_pipeline)
        
        logging.info(f"Pipeline scheduled to run daily at {schedule_time}")
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    def close(self):
        """Close database session"""
        self.session.close() 