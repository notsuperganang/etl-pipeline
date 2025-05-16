"""
Main module for the Fashion Studio ETL Pipeline.

This module combines all ETL stages in a single pipeline:
- Extract: Scrape product data from Fashion Studio website
- Transform: Clean and transform the data
- Load: Save data to various repositories (CSV, Google Sheets, PostgreSQL)

Usage:
    python main.py  # Run the full pipeline with default settings
    python main.py --stages extract  # Run only extraction
    python main.py --stages transform  # Run only transformation
    python main.py --stages load  # Run only loading
    python main.py --repositories all  # Save to all repositories
"""

import os
import sys
import time
import argparse
import pandas as pd
from datetime import datetime
from colorama import Fore, Back, Style, init

# Import the modules
from utils.extract import scrape_all_products
from utils.transform import transform_data
from utils.load import load_to_csv, main as load_main

# Initialize colorama
init(autoreset=True)

# ASCII Art Banner
banner = """
╔═════════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                         ║
║  ███████╗████████╗██╗         ██████╗ ██╗██████╗ ███████╗██╗     ██╗███╗   ██╗███████╗  ║
║  ██╔════╝╚══██╔══╝██║         ██╔══██╗██║██╔══██╗██╔════╝██║     ██║████╗  ██║██╔════╝  ║
║  █████╗     ██║   ██║         ██████╔╝██║██████╔╝█████╗  ██║     ██║██╔██╗ ██║█████╗    ║
║  ██╔══╝     ██║   ██║         ██╔═══╝ ██║██╔═══╝ ██╔══╝  ██║     ██║██║╚██╗██║██╔══╝    ║
║  ███████╗   ██║   ███████╗    ██║     ██║██║     ███████╗███████╗██║██║ ╚████║███████╗  ║
║  ╚══════╝   ╚═╝   ╚══════╝    ╚═╝     ╚═╝╚═╝     ╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝╚══════╝  ║
║                                                                                         ║
║  ███████╗ █████╗ ███████╗██╗  ██╗██╗ ██████╗ ███╗   ██╗                                 ║
║  ██╔════╝██╔══██╗██╔════╝██║  ██║██║██╔═══██╗████╗  ██║                                 ║
║  █████╗  ███████║███████╗███████║██║██║   ██║██╔██╗ ██║                                 ║
║  ██╔══╝  ██╔══██║╚════██║██╔══██║██║██║   ██║██║╚██╗██║                                 ║
║  ██║     ██║  ██║███████║██║  ██║██║╚██████╔╝██║ ╚████║                                 ║
║  ╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝                                 ║
║                                                                                         ║
║  ███████╗████████╗██╗   ██╗██████╗ ██╗ ██████╗                                          ║
║  ██╔════╝╚══██╔══╝██║   ██║██╔══██╗██║██╔═══██╗                                         ║
║  ███████╗   ██║   ██║   ██║██║  ██║██║██║   ██║                                         ║
║  ╚════██║   ██║   ██║   ██║██║  ██║██║██║   ██║                                         ║
║  ███████║   ██║   ╚██████╔╝██████╔╝██║╚██████╔╝                                         ║
║  ╚══════╝   ╚═╝    ╚═════╝ ╚═════╝ ╚═╝ ╚═════╝                                          ║
║                                                                                         ║
╚═════════════════════════════════════════════════════════════════════════════════════════╝
"""

# Function to display fancy log messages
def log_message(message, level="INFO", emoji=""):
    """
    Display formatted log messages with timestamp, level, and emoji.
    
    Args:
        message: The message to log
        level: Log level (INFO, SUCCESS, WARNING, ERROR, PROCESSING)
        emoji: Optional emoji to display with the message
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if level == "INFO":
        color = Fore.CYAN
        level_str = f"{color}[INFO]{Style.RESET_ALL}"
    elif level == "SUCCESS":
        color = Fore.GREEN
        level_str = f"{color}[SUCCESS]{Style.RESET_ALL}"
    elif level == "WARNING":
        color = Fore.YELLOW
        level_str = f"{color}[WARNING]{Style.RESET_ALL}"
    elif level == "ERROR":
        color = Fore.RED
        level_str = f"{color}[ERROR]{Style.RESET_ALL}"
    elif level == "PROCESSING":
        color = Fore.MAGENTA
        level_str = f"{color}[PROCESSING]{Style.RESET_ALL}"
    else:
        color = Fore.WHITE
        level_str = f"{color}[{level}]{Style.RESET_ALL}"
    
    print(f"{timestamp} {level_str} {emoji} {message}")

def run_pipeline(args):
    """
    Run the complete ETL pipeline.
    
    Args:
        args: Command-line arguments
    """
    start_time = time.time()
    
    # Clear screen and show banner
    os.system('cls' if os.name == 'nt' else 'clear')
    print(Fore.GREEN + banner + Style.RESET_ALL)
    
    # Display header info
    print(f"{Fore.YELLOW}{'═' * 70}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  ETL Pipeline: {Fore.WHITE}Fashion Studio Data{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  Target Website: {Fore.WHITE}https://fashion-studio.dicoding.dev/{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  Start Time: {Fore.WHITE}{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  [👤] Code brewed by: {Fore.GREEN}notsuperganang 🔥{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{'═' * 70}{Style.RESET_ALL}\n")
    
    # Which stages to run
    run_extract = args.stages in ['extract', 'all']
    run_transform = args.stages in ['transform', 'all']
    run_load = args.stages in ['load', 'all']
    
    extracted_df = None
    transformed_df = None
    
    # 1. EXTRACT STAGE
    if run_extract:
        log_message("STAGE 1: EXTRACTION", "PROCESSING", "🔍")
        log_message("Starting data extraction from Fashion Studio website", "INFO", "🌐")
        
        try:
            # Set max pages based on command-line arg
            max_pages = args.max_pages if args.max_pages else 50
            
            extracted_df = scrape_all_products(base_url='https://fashion-studio.dicoding.dev', 
                                           max_pages=max_pages)
            
            if not extracted_df.empty:
                # Save raw data if requested
                if args.save_raw:
                    raw_output = args.raw_output if args.raw_output else f'raw_products_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                    # Use load module to save the raw data
                    load_to_csv(extracted_df, raw_output)
                    log_message(f"Raw data saved to '{raw_output}'", "SUCCESS", "💾")
            else:
                log_message("Extraction failed to produce any data!", "ERROR", "❌")
                return
        except Exception as e:
            log_message(f"Error during extraction stage: {e}", "ERROR", "❌")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return
    
    # 2. TRANSFORM STAGE
    if run_transform:
        log_message("STAGE 2: TRANSFORMATION", "PROCESSING", "🔄")
        
        try:
            # If we have extracted data, use it, otherwise try to load from file
            if extracted_df is not None:
                log_message("Using data from extraction stage", "INFO", "📋")
                df_to_transform = extracted_df
            elif args.input_file:
                log_message(f"Loading data from '{args.input_file}'", "INFO", "📂")
                df_to_transform = pd.read_csv(args.input_file)
            else:
                log_message("No input data for transformation. Either run extraction or specify input file.", "ERROR", "❌")
                return
            
            # Set exchange rate based on command-line arg
            exchange_rate = args.exchange_rate if args.exchange_rate else 16000.0
            
            transformed_df = transform_data(df_to_transform, exchange_rate=exchange_rate)
            
            if not transformed_df.empty:
                # Save transformed data if requested - use the load module
                if args.save_transformed:
                    transformed_output = args.transformed_output if args.transformed_output else f'transformed_products_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                    load_to_csv(transformed_df, transformed_output)
                    log_message(f"Transformed data saved to '{transformed_output}'", "SUCCESS", "💾")
            else:
                log_message("Transformation failed to produce any data!", "ERROR", "❌")
                return
        except Exception as e:
            log_message(f"Error during transformation stage: {e}", "ERROR", "❌")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return
    
    # 3. LOAD STAGE
    if run_load:
        log_message("STAGE 3: LOADING", "PROCESSING", "📥")
        
        try:
            if transformed_df is not None:
                log_message("Using data from transformation stage", "INFO", "📋")
                df_to_load = transformed_df
            elif args.input_file:
                log_message(f"Loading data from '{args.input_file}'", "INFO", "📂")
                df_to_load = pd.read_csv(args.input_file)
            else:
                log_message("No input data for loading. Either run transformation or specify input file.", "ERROR", "❌")
                return
            
            # Determine which repositories to use
            load_to_csv_flag = args.repositories in ['csv', 'all']
            load_to_sheets_flag = args.repositories in ['sheets', 'all']
            load_to_postgres_flag = args.repositories in ['postgres', 'all']
            
            # Set up database parameters
            db_params = {
                "dbname": args.db_name,
                "user": args.db_user,
                "password": args.db_pass,
                "host": args.db_host,
                "port": args.db_port
            }
            
            # Run the load process
            load_success = load_main(
                df=df_to_load,
                csv_output=args.output_file,
                load_to_csv_flag=load_to_csv_flag,
                load_to_sheets_flag=load_to_sheets_flag,
                load_to_postgres_flag=load_to_postgres_flag,
                google_sheets_credentials=args.google_creds,
                google_sheet_id=args.google_sheet_id,
                google_sheet_name=args.google_sheet_name,
                google_worksheet_name=args.google_worksheet_name,
                db_params=db_params,
                dry_run=args.dry_run
            )
            
            if not load_success:
                log_message("Loading stage completed with errors.", "WARNING", "⚠️")
            
        except Exception as e:
            log_message(f"Error during loading stage: {e}", "ERROR", "❌")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return
    
    # Pipeline completion
    total_time = time.time() - start_time
    print(f"\n{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}  ETL PIPELINE SUMMARY{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
    print(f"  ⏱️ {Fore.WHITE}Total processing time: {Fore.CYAN}{total_time:.2f} seconds{Style.RESET_ALL}")
    
    # Print status for each stage
    extract_status = f"{Fore.GREEN}✓ COMPLETED" if run_extract else f"{Fore.YELLOW}○ SKIPPED"
    print(f"  🔍 {Fore.WHITE}Extract: {extract_status}{Style.RESET_ALL}")
    
    transform_status = f"{Fore.GREEN}✓ COMPLETED" if run_transform else f"{Fore.YELLOW}○ SKIPPED"
    print(f"  🔄 {Fore.WHITE}Transform: {transform_status}{Style.RESET_ALL}")
    
    load_status = f"{Fore.GREEN}✓ COMPLETED" if run_load else f"{Fore.YELLOW}○ SKIPPED"
    print(f"  📥 {Fore.WHITE}Load: {load_status}{Style.RESET_ALL}")
    
    print(f"{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
    
    # Final success message
    print(f"\n{Fore.GREEN}★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★{Style.RESET_ALL}")
    print(f"{Fore.GREEN}★  ETL PIPELINE EXECUTION COMPLETED SUCCESSFULLY!          {Style.RESET_ALL}")
    print(f"{Fore.GREEN}★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★{Style.RESET_ALL}")

def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Fashion Studio ETL Pipeline',
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument('--stages', choices=['extract', 'transform', 'load', 'all'], 
                       default='all', help='Pipeline stages to run (default: all)')
    
    parser.add_argument('--input-file', '-i', help='Input file for transform or load stages')
    parser.add_argument('--output-file', '-o', default='products.csv', 
                       help='Output CSV file path (default: products.csv)')
    
    parser.add_argument('--max-pages', '-m', type=int, default=50,
                       help='Maximum number of pages to scrape (default: 50)')
    
    parser.add_argument('--save-raw', action='store_true', 
                       help='Save raw data after extraction')
    parser.add_argument('--raw-output', 
                       help='Output file for raw data (default: raw_products_TIMESTAMP.csv)')
    
    parser.add_argument('--save-transformed', action='store_true', 
                       help='Save transformed data after transformation')
    parser.add_argument('--transformed-output', 
                       help='Output file for transformed data (default: transformed_products_TIMESTAMP.csv)')
    
    parser.add_argument('--exchange-rate', '-e', type=float, default=16000.0, 
                       help='USD to IDR exchange rate (default: 16000.0)')
    
    parser.add_argument('--repositories', '-r', choices=['csv', 'sheets', 'postgres', 'all'], 
                       default='csv', help='Target repositories to load data (default: csv)')
    
    parser.add_argument('--google-creds', '-g', default='google-sheets-api.json', 
                       help='Google Sheets API credentials file (default: google-sheets-api.json)')
    
    parser.add_argument('--google-sheet-id', '--sheet-id', help='Google Sheets ID for loading data(Optional)')
    parser.add_argument('--google-sheet-name', default='Fashion Products Data', help='Google Sheet name (default: Fashion Products Data)')
    parser.add_argument('--google-worksheet-name', default='Products',
                       help='Worksheet name (default: Products)')
    
    parser.add_argument('--db-host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('--db-port', default='5432', help='PostgreSQL port (default: 5432)')
    parser.add_argument('--db-name', default='fashion_data', help='PostgreSQL database name (default: fashion_data)')
    parser.add_argument('--db-user', default='postgres', help='PostgreSQL username (default: postgres)')
    parser.add_argument('--db-pass', default='postgres', help='PostgreSQL password (default: postgres)')
    
    parser.add_argument('--dry-run', action='store_true', 
                       help='Validate data for loading but do not save to repositories (for testing)')
    
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose error messages with stack traces')
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    run_pipeline(args)