import logging
import argparse
import sys
import os
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.etl.pipeline_orchestrator import ETLPipelineOrchestrator
from src.reporting.analytics import DataAnalytics

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/data_warehouse.log'),
            logging.StreamHandler()
        ]
    )

def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(description='Data Warehouse ETL Pipeline')
    parser.add_argument('--mode', choices=['full', 'incremental', 'schedule', 'report'], 
                       default='full', help='Pipeline execution mode')
    parser.add_argument('--config', default='config/config.yaml', 
                       help='Path to configuration file')
    parser.add_argument('--schedule-time', default='00:00', 
                       help='Time to run scheduled pipeline (HH:MM)')
    parser.add_argument('--report-type', choices=['sales_summary', 'customer_analysis', 
                                                 'product_performance', 'scd_analysis'],
                       help='Type of report to generate')
    parser.add_argument('--output-path', help='Output path for reports')
    parser.add_argument('--start-date', help='Start date for date range queries (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for date range queries (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    try:
        if args.mode in ['full', 'incremental', 'schedule']:
            # Initialize ETL pipeline
            orchestrator = ETLPipelineOrchestrator(args.config)
            
            if args.mode == 'full':
                logging.info("Starting full ETL pipeline")
                orchestrator.run_full_pipeline()
                logging.info("Full ETL pipeline completed successfully")
                
            elif args.mode == 'incremental':
                logging.info("Starting incremental ETL pipeline")
                orchestrator.run_incremental_pipeline()
                logging.info("Incremental ETL pipeline completed successfully")
                
            elif args.mode == 'schedule':
                logging.info(f"Starting scheduled ETL pipeline at {args.schedule_time}")
                orchestrator.schedule_pipeline(args.schedule_time)
            
            orchestrator.close()
            
        elif args.mode == 'report':
            # Initialize analytics
            analytics = DataAnalytics()
            
            if not args.report_type:
                logging.error("Report type must be specified for report mode")
                sys.exit(1)
            
            if args.report_type == 'sales_summary':
                logging.info("Generating sales summary report")
                report_data = analytics.get_sales_summary(args.start_date, args.end_date)
                print("\n=== Sales Summary Report ===")
                print(report_data.to_string(index=False))
                
            elif args.report_type == 'customer_analysis':
                logging.info("Generating customer analysis report")
                report_data = analytics.get_customer_analysis()
                print("\n=== Customer Analysis Report ===")
                print(report_data.to_string(index=False))
                
            elif args.report_type == 'product_performance':
                logging.info("Generating product performance report")
                report_data = analytics.get_product_performance()
                print("\n=== Product Performance Report ===")
                print(report_data.to_string(index=False))
                
            elif args.report_type == 'scd_analysis':
                logging.info("Generating SCD analysis report")
                report_data = analytics.get_scd_analysis()
                print("\n=== SCD Analysis Report ===")
                print(report_data.to_string(index=False))
            
            # Export to CSV if output path is specified
            if args.output_path:
                analytics.export_report_to_csv(args.report_type, args.output_path)
                print(f"\nReport exported to: {args.output_path}")
    
    except KeyboardInterrupt:
        logging.info("Pipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()