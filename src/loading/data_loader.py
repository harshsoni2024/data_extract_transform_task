import pandas as pd
from sqlalchemy.orm import Session
from typing import Dict, List, Optional
import logging
from datetime import datetime
from sqlalchemy import text

class DataLoader:
    """Handles data loading into the data warehouse"""
    
    def __init__(self, db_session: Session):
        self.session = db_session
    
    def load_dimension_table(self, table_class, data: pd.DataFrame, 
                           scd_type: str = "type_1", tracking_columns: Optional[List[str]] = None):
        """Load data into dimension table with SCD handling"""
        try:
            from src.scd.scd_manager import SCDManager
            scd_manager = SCDManager(self.session)
            
            if scd_type == "type_1":
                business_key = self._get_business_key(table_class)
                scd_manager.handle_scd_type_1(table_class, data, business_key)
            elif scd_type == "type_2":
                business_key = self._get_business_key(table_class)
                if not tracking_columns:
                    tracking_columns = self._get_default_tracking_columns(table_class)
                scd_manager.handle_scd_type_2(table_class, data, business_key, tracking_columns)
            else:
                raise ValueError(f"Unsupported SCD type: {scd_type}")
                
            logging.info(f"Successfully loaded {len(data)} records into {table_class.__tablename__}")
            
        except Exception as e:
            logging.error(f"Error loading dimension table: {e}")
            raise
    
    def load_fact_table(self, table_class, data: pd.DataFrame, 
                       incremental_key: Optional[str] = None):
        """Load data into fact table with incremental loading support"""
        try:
            if incremental_key and incremental_key in data.columns:
                # Incremental loading - only load new records
                existing_keys = self._get_existing_keys(table_class, incremental_key)
                new_data = data[~data[incremental_key].isin(existing_keys)]
                
                if len(new_data) > 0:
                    for _, row in new_data.iterrows():
                        new_record = table_class(**row.to_dict())
                        self.session.add(new_record)
                    
                    self.session.commit()
                    logging.info(f"Successfully loaded {len(new_data)} new records into {table_class.__tablename__}")
                else:
                    logging.info("No new records to load")
            else:
                # Full load
                for _, row in data.iterrows():
                    new_record = table_class(**row.to_dict())
                    self.session.add(new_record)
                
                self.session.commit()
                logging.info(f"Successfully loaded {len(data)} records into {table_class.__tablename__}")
                
        except Exception as e:
            self.session.rollback()
            logging.error(f"Error loading fact table: {e}")
            raise
    
    def _get_business_key(self, table_class) -> str:
        """Get the business key column name for a table"""
        # This is a simplified implementation
        # In a real scenario, you might want to define this in the model or config
        if hasattr(table_class, 'customer_id'):
            return 'customer_id'
        elif hasattr(table_class, 'product_id'):
            return 'product_id'
        else:
            raise ValueError(f"Could not determine business key for {table_class.__name__}")
    
    def _get_default_tracking_columns(self, table_class) -> List[str]:
        """Get default tracking columns for SCD Type 2"""
        # This is a simplified implementation
        # In a real scenario, you might want to define this in the model or config
        if hasattr(table_class, 'customer_name'):
            return ['customer_name', 'email', 'phone', 'address']
        elif hasattr(table_class, 'product_name'):
            return ['product_name', 'category', 'brand', 'price']
        else:
            return []
    
    def _get_existing_keys(self, table_class, key_column: str) -> List:
        """Get existing keys from the table for incremental loading"""
        try:
            result = self.session.query(getattr(table_class, key_column)).all()
            return [row[0] for row in result]
        except Exception as e:
            logging.error(f"Error getting existing keys: {e}")
            return []
    
    def truncate_table(self, table_class):
        """Truncate a table (delete all records)"""
        try:
            self.session.query(table_class).delete()
            self.session.commit()
            logging.info(f"Successfully truncated table {table_class.__tablename__}")
        except Exception as e:
            self.session.rollback()
            logging.error(f"Error truncating table: {e}")
            raise 