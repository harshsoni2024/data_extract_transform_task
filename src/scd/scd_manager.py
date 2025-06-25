import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_

class SCDManager:
    """Manages Slowly Changing Dimensions"""
    
    def __init__(self, db_session: Session):
        self.session = db_session
    
    def handle_scd_type_1(self, table_class, data: pd.DataFrame, business_key: str):
        """Handle SCD Type 1 - Overwrite existing records"""
        try:
            for _, row in data.iterrows():
                # Check if record exists
                existing_record = self.session.query(table_class).filter(
                    getattr(table_class, business_key) == row[business_key]
                ).first()
                
                if existing_record:
                    # Update existing record
                    for column in data.columns:
                        if hasattr(existing_record, column) and column != business_key:
                            setattr(existing_record, column, row[column])
                    existing_record.updated_at = datetime.utcnow()
                else:
                    # Insert new record
                    new_record = table_class(**row.to_dict())
                    self.session.add(new_record)
            
            self.session.commit()
            logging.info(f"Successfully processed SCD Type 1 for {len(data)} records")
            
        except Exception as e:
            self.session.rollback()
            logging.error(f"Error in SCD Type 1 processing: {e}")
            raise
    
    def handle_scd_type_2(self, table_class, data: pd.DataFrame, business_key: str, 
                         tracking_columns: List[str]):
        """Handle SCD Type 2 - Track changes with versioning"""
        try:
            for _, row in data.iterrows():
                # Get current active record
                current_record = self.session.query(table_class).filter(
                    and_(
                        getattr(table_class, business_key) == row[business_key],
                        table_class.is_current == True
                    )
                ).first()
                
                if current_record:
                    # Check if any tracked columns have changed
                    has_changes = False
                    for column in tracking_columns:
                        if hasattr(current_record, column) and getattr(current_record, column) != row[column]:
                            has_changes = True
                            break
                    
                    if has_changes:
                        # End current record
                        current_record.is_current = False
                        current_record.end_date = datetime.utcnow()
                        current_record.updated_at = datetime.utcnow()
                        
                        # Create new version
                        new_version = current_record.version + 1
                        row_dict = row.to_dict()
                        row_dict['version'] = new_version
                        row_dict['effective_date'] = datetime.utcnow()
                        row_dict['is_current'] = True
                        row_dict['end_date'] = None
                        
                        new_record = table_class(**row_dict)
                        self.session.add(new_record)
                else:
                    # Create first record
                    row_dict = row.to_dict()
                    row_dict['version'] = 1
                    row_dict['effective_date'] = datetime.utcnow()
                    row_dict['is_current'] = True
                    row_dict['end_date'] = None
                    
                    new_record = table_class(**row_dict)
                    self.session.add(new_record)
            
            self.session.commit()
            logging.info(f"Successfully processed SCD Type 2 for {len(data)} records")
            
        except Exception as e:
            self.session.rollback()
            logging.error(f"Error in SCD Type 2 processing: {e}")
            raise
    
    def get_current_records(self, table_class, business_key: str, business_key_value: str):
        """Get current active record for a business key"""
        return self.session.query(table_class).filter(
            and_(
                getattr(table_class, business_key) == business_key_value,
                table_class.is_current == True
            )
        ).first()
    
    def get_historical_records(self, table_class, business_key: str, business_key_value: str):
        """Get all historical records for a business key"""
        return self.session.query(table_class).filter(
            getattr(table_class, business_key) == business_key_value
        ).order_by(table_class.version).all() 