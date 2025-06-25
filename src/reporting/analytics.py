import pandas as pd
import sqlite3
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

class DataAnalytics:
    """Provides analytics and reporting capabilities"""
    
    def __init__(self, db_path: str = "data_warehouse.db"):
        self.db_path = db_path
    
    def get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def get_sales_summary(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Get sales summary report"""
        try:
            query = """
            SELECT 
                d.year,
                d.month,
                d.month_name,
                COUNT(DISTINCT fo.order_id) as total_orders,
                SUM(fo.total_amount) as total_revenue,
                AVG(fo.total_amount) as avg_order_value,
                SUM(fo.quantity) as total_quantity_sold
            FROM fact_order fo
            JOIN dim_date d ON fo.date_key = d.date_key
            JOIN dim_customer c ON fo.customer_key = c.customer_key
            JOIN dim_product p ON fo.product_key = p.product_key
            WHERE c.is_current = 1 AND p.is_current = 1
            """
            
            if start_date and end_date:
                query += f" AND d.date BETWEEN '{start_date}' AND '{end_date}'"
            
            query += """
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month
            """
            
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn)
            
            return df
            
        except Exception as e:
            logging.error(f"Error getting sales summary: {e}")
            raise
    
    def get_customer_analysis(self) -> pd.DataFrame:
        """Get customer analysis report"""
        try:
            query = """
            SELECT 
                c.customer_name,
                c.city,
                c.country,
                COUNT(DISTINCT fo.order_id) as total_orders,
                SUM(fo.total_amount) as total_spent,
                AVG(fo.total_amount) as avg_order_value,
                MAX(fo.created_at) as last_order_date
            FROM fact_order fo
            JOIN dim_customer c ON fo.customer_key = c.customer_key
            WHERE c.is_current = 1
            GROUP BY c.customer_id, c.customer_name, c.city, c.country
            ORDER BY total_spent DESC
            """
            
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn)
            
            return df
            
        except Exception as e:
            logging.error(f"Error getting customer analysis: {e}")
            raise
    
    def get_product_performance(self) -> pd.DataFrame:
        """Get product performance report"""
        try:
            query = """
            SELECT 
                p.product_name,
                p.category,
                p.brand,
                COUNT(DISTINCT fo.order_id) as total_orders,
                SUM(fo.quantity) as total_quantity_sold,
                SUM(fo.total_amount) as total_revenue,
                AVG(fo.unit_price) as avg_unit_price
            FROM fact_order fo
            JOIN dim_product p ON fo.product_key = p.product_key
            WHERE p.is_current = 1
            GROUP BY p.product_id, p.product_name, p.category, p.brand
            ORDER BY total_revenue DESC
            """
            
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn)
            
            return df
            
        except Exception as e:
            logging.error(f"Error getting product performance: {e}")
            raise
    
    def get_scd_analysis(self, table_name: str = "dim_customer") -> pd.DataFrame:
        """Get SCD analysis for tracking changes"""
        try:
            query = f"""
            SELECT 
                customer_id,
                customer_name,
                version,
                effective_date,
                end_date,
                is_current,
                CASE 
                    WHEN end_date IS NULL THEN 'Current'
                    ELSE 'Historical'
                END as record_status
            FROM {table_name}
            ORDER BY customer_id, version
            """
            
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn)
            
            return df
            
        except Exception as e:
            logging.error(f"Error getting SCD analysis: {e}")
            raise
    
    def generate_sales_chart(self, save_path: Optional[str] = None):
        """Generate sales trend chart"""
        try:
            # Get sales data
            sales_data = self.get_sales_summary()
            
            # Create chart
            plt.figure(figsize=(12, 6))
            plt.plot(sales_data['month_name'], sales_data['total_revenue'], marker='o')
            plt.title('Monthly Sales Revenue')
            plt.xlabel('Month')
            plt.ylabel('Revenue ($)')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path)
            else:
                plt.show()
            
            plt.close()
            
        except Exception as e:
            logging.error(f"Error generating sales chart: {e}")
            raise
    
    def generate_customer_segment_chart(self, save_path: Optional[str] = None):
        """Generate customer segmentation chart"""
        try:
            # Get customer data
            customer_data = self.get_customer_analysis()
            
            # Create customer segments
            customer_data['segment'] = pd.cut(
                customer_data['total_spent'],
                bins=[0, 100, 500, 1000, float('inf')],
                labels=['Bronze', 'Silver', 'Gold', 'Platinum']
            )
            
            # Create chart
            plt.figure(figsize=(10, 6))
            segment_counts = customer_data['segment'].value_counts()
            plt.pie(segment_counts.values, labels=segment_counts.index, autopct='%1.1f%%')
            plt.title('Customer Segmentation by Total Spent')
            
            if save_path:
                plt.savefig(save_path)
            else:
                plt.show()
            
            plt.close()
            
        except Exception as e:
            logging.error(f"Error generating customer segment chart: {e}")
            raise
    
    def export_report_to_csv(self, report_type: str, output_path: str):
        """Export report to CSV file"""
        try:
            if report_type == "sales_summary":
                data = self.get_sales_summary()
            elif report_type == "customer_analysis":
                data = self.get_customer_analysis()
            elif report_type == "product_performance":
                data = self.get_product_performance()
            elif report_type == "scd_analysis":
                data = self.get_scd_analysis()
            else:
                raise ValueError(f"Unknown report type: {report_type}")
            
            data.to_csv(output_path, index=False)
            logging.info(f"Report exported to {output_path}")
            
        except Exception as e:
            logging.error(f"Error exporting report: {e}")
            raise 