"""
Load module for Fashion Studio ETL Pipeline.

This module is responsible for loading transformed data into various repositories:
- CSV files
- Google Sheets
- PostgreSQL database

Dependencies:
- pandas: For data handling
- gspread & oauth2client: For Google Sheets integration
- psycopg2 & sqlalchemy: For PostgreSQL integration

Usage:
- This module should be used after data extraction and transformation
- Expects a clean DataFrame as input from the transform module
"""

import pandas as pd
import os
import time
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple
from colorama import Fore, Back, Style, init
import gspread
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text as sqlalchemy_text
from oauth2client.service_account import ServiceAccountCredentials
import sys
import argparse

# Initialize colorama
init(autoreset=True)

# ASCII Art Banner
banner = """
╔═══════════════════════════════════════════════════════════════════════════════════╗
║                                                                                   ║
║  ██╗      ██████╗  █████╗ ██████╗     ██████╗  █████╗ ████████╗ █████╗            ║
║  ██║     ██╔═══██╗██╔══██╗██╔══██╗    ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗           ║
║  ██║     ██║   ██║███████║██║  ██║    ██║  ██║███████║   ██║   ███████║           ║
║  ██║     ██║   ██║██╔══██║██║  ██║    ██║  ██║██╔══██║   ██║   ██╔══██║           ║
║  ███████╗╚██████╔╝██║  ██║██████╔╝    ██████╔╝██║  ██║   ██║   ██║  ██║           ║
║  ╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝           ║
║                                                                                   ║
║  ███████╗ █████╗ ███████╗██╗  ██╗██╗ ██████╗ ███╗   ██╗                           ║
║  ██╔════╝██╔══██╗██╔════╝██║  ██║██║██╔═══██╗████╗  ██║                           ║
║  █████╗  ███████║███████╗███████║██║██║   ██║██╔██╗ ██║                           ║
║  ██╔══╝  ██╔══██║╚════██║██╔══██║██║██║   ██║██║╚██╗██║                           ║
║  ██║     ██║  ██║███████║██║  ██║██║╚██████╔╝██║ ╚████║                           ║
║  ╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝                           ║
║                                                                                   ║
║  ██████╗ ███████╗██████╗  ██████╗ ███████╗██╗████████╗ ██████╗ ██████╗ ██╗   ██╗  ║
║  ██╔══██╗██╔════╝██╔══██╗██╔═══██╗██╔════╝██║╚══██╔══╝██╔═══██╗██╔══██╗╚██╗ ██╔╝  ║
║  ██████╔╝█████╗  ██████╔╝██║   ██║███████╗██║   ██║   ██║   ██║██████╔╝ ╚████╔╝   ║
║  ██╔══██╗██╔══╝  ██╔═══╝ ██║   ██║╚════██║██║   ██║   ██║   ██║██╔══██╗  ╚██╔╝    ║
║  ██║  ██║███████╗██║     ╚██████╔╝███████║██║   ██║   ╚██████╔╝██║  ██║   ██║     ║
║  ╚═╝  ╚═╝╚══════╝╚═╝      ╚═════╝ ╚══════╝╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝     ║
║                                                                                   ║
╚═══════════════════════════════════════════════════════════════════════════════════╝
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

# Function to show a spinner effect
def show_spinner(seconds, message):
    """
    Display an animated spinner with message for the specified duration.
    
    Args:
        seconds: Duration to show the spinner in seconds
        message: Message to display alongside the spinner
    """
    spinner = ['⣾', '⣽', '⣻', '⢿', '⡿', '⣟', '⣯', '⣷']
    for _ in range(int(seconds * 5)):
        for char in spinner:
            print(f"\r{Fore.CYAN}{message} {char}{Style.RESET_ALL}", end='', flush=True)
            time.sleep(0.2)
    print()

# Function to display progress bar
def show_progress_bar(current, total, prefix="", suffix="", length=50):
    """
    Generate a text-based progress bar.
    
    Args:
        current: Current progress value
        total: Total value
        prefix: Text to display before the progress bar
        suffix: Text to display after the progress bar
        length: Length of the progress bar in characters
        
    Returns:
        Formatted progress bar string
    """
    percent = (current / total) * 100 if total > 0 else 0
    filled_length = int(length * current // total) if total > 0 else 0
    bar = Fore.GREEN + '█' * filled_length + Fore.WHITE + '░' * (length - filled_length)
    return f"{prefix} [{bar}{Style.RESET_ALL}] {current}/{total} {suffix} ({percent:.1f}%)"

def load_to_csv(df: pd.DataFrame, output_path: str = "products.csv") -> bool:
    """
    Save transformed data to a CSV file.
    
    Args:
        df: DataFrame to save
        output_path: Path where the CSV file will be saved
        
    Returns:
        Boolean indicating success or failure
    """
    try:
        log_message(f"Saving data to CSV file: '{output_path}'", "PROCESSING", "💾")
        
        # Create directory if it doesn't exist
        directory = os.path.dirname(output_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            log_message(f"Created directory: '{directory}'", "INFO", "📁")
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        
        # Verify the file was created and contains data
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            log_message(f"Successfully saved {len(df)} records to '{output_path}'", "SUCCESS", "✅")
            return True
        else:
            log_message(f"File was created but may be empty: '{output_path}'", "WARNING", "⚠️")
            return False
            
    except Exception as e:
        log_message(f"Error saving to CSV: {e}", "ERROR", "❌")
        return False

def load_to_google_sheets(df: pd.DataFrame, 
                          credentials_path: str = "google-sheets-api.json",
                          sheet_name: str = "Fashion Products Data",
                          worksheet_name: str = "Products",
                          sheet_id: str = None) -> bool: 
    """
    Save transformed data to Google Sheets.
    
    Args:
        df: DataFrame to save
        credentials_path: Path to Google Sheets API credentials file
        sheet_name: Name of the Google Sheet to use/create
        worksheet_name: Name of the worksheet within the sheet
        
    Returns:
        Boolean indicating success or failure
    """
    try:
        log_message(f"Preparing to save data to Google Sheets: '{sheet_name}'", "PROCESSING", "📊")
        
        # Check if credentials file exists
        if not os.path.exists(credentials_path):
            log_message(f"Google Sheets API credentials file not found: '{credentials_path}'", "ERROR", "❌")
            return False
        
        # Set up credentials
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        try:
            log_message("Authenticating with Google Sheets API", "PROCESSING", "🔐")
            credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
            client = gspread.authorize(credentials)
            log_message("Successfully authenticated with Google Sheets API", "SUCCESS", "✅")
        except Exception as auth_error:
            log_message(f"Authentication with Google Sheets API failed: {auth_error}", "ERROR", "❌")
            return False
        
        # Open sheet by ID or name
        if sheet_id:
            # Method 1: Open by specific Sheet ID
            try:
                sheet = client.open_by_key(sheet_id)
                log_message(f"Opened Google Sheet by ID: {sheet_id}", "SUCCESS", "🎯")
            except Exception as e:
                log_message(f"Failed to open sheet with ID {sheet_id}: {e}", "ERROR", "❌")
                return False
        else:
            # Method 2: Open by name (original behavior)
            try:
                sheet = client.open(sheet_name)
                log_message(f"Found existing Google Sheet: '{sheet_name}'", "INFO", "📝")
            except gspread.exceptions.SpreadsheetNotFound:
                sheet = client.create(sheet_name)
                sheet.share('anyone', perm_type='anyone', role='writer')
                log_message(f"Created new Google Sheet: '{sheet_name}'", "SUCCESS", "✅")
                log_message(f"Sheet ID: {sheet.id}", "INFO", "🆔")
        
        # Try to open existing worksheet or create a new one
        try:
            worksheet = sheet.worksheet(worksheet_name)
            # Clear existing content
            worksheet.clear()
            log_message(f"Cleared existing worksheet: '{worksheet_name}'", "INFO", "🧹")
        except gspread.exceptions.WorksheetNotFound:
            # Add a new worksheet if it doesn't exist
            worksheet = sheet.add_worksheet(title=worksheet_name, rows=len(df) + 1, cols=len(df.columns))
            log_message(f"Created new worksheet: '{worksheet_name}'", "SUCCESS", "✅")
        
        # Convert DataFrame to list of lists for Google Sheets
        header = df.columns.tolist()
        values = df.values.tolist()
        all_values = [header] + values
        
        # Update the sheet in batches to avoid API limits
        batch_size = 1000
        total_batches = (len(all_values) + batch_size - 1) // batch_size
        
        log_message(f"Updating Google Sheet with {len(df)} records in {total_batches} batches", "PROCESSING", "🔄")
        
        for i in range(total_batches):
            start_idx = i * batch_size
            end_idx = min(start_idx + batch_size, len(all_values))
            batch = all_values[start_idx:end_idx]
            
            # Update the cells
            cell_range = f"A{start_idx + 1}:{chr(65 + len(df.columns) - 1)}{end_idx}"
            worksheet.update(cell_range, batch)
            
            print(show_progress_bar(
                i + 1, 
                total_batches, 
                prefix=f"{Fore.CYAN}Google Sheets Upload Progress:", 
                suffix=f"batches"
            ))
            
            # Add a small delay to avoid API rate limits
            if i < total_batches - 1:
                time.sleep(1)
        
        # Format the header row (bold, freeze)
        try:
            # Format header
            header_format = {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            }
            
            # Apply formatting to header row
            format_range = f"A1:{chr(65 + len(df.columns) - 1)}1"
            worksheet.format(format_range, {"textFormat": {"bold": True}})
            
            # Freeze the header row
            worksheet.freeze(rows=1)
            
            log_message("Applied formatting to Google Sheet header", "SUCCESS", "✨")
        except Exception as format_error:
            log_message(f"Warning: Could not format header: {format_error}", "WARNING", "⚠️")
        
        # Get the sheet URL
        sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet.id}"
        log_message(f"Successfully uploaded data to Google Sheets", "SUCCESS", "🎉")
        log_message(f"Sheet URL: {sheet_url}", "INFO", "🔗")
        
        return True
        
    except Exception as e:
        log_message(f"Error saving to Google Sheets: {e}", "ERROR", "❌")
        return False

def load_to_postgresql(df: pd.DataFrame, 
                      db_params: Dict[str, str] = {
                          "dbname": "fashion_data",
                          "user": "postgres",
                          "password": "postgres",
                          "host": "localhost",
                          "port": "5432"
                      },
                      table_name: str = "fashion_products") -> bool:
    """
    Save transformed data to PostgreSQL database.
    
    Args:
        df: DataFrame to save
        db_params: Database connection parameters
        table_name: Name of the table to save the data
        
    Returns:
        Boolean indicating success or failure
    """
    log_message(f"Preparing to save data to PostgreSQL: '{table_name}'", "PROCESSING", "🐘")
    
    # Create database if it doesn't exist
    temp_db_params = db_params.copy()
    temp_db_params["dbname"] = "postgres"
    
    try:
        conn = psycopg2.connect(**temp_db_params, connect_timeout=10)  # Add timeout
        conn.autocommit = True
        cursor = conn.cursor()
    except psycopg2.OperationalError as e:
        if "timeout" in str(e).lower():
            log_message(f"Database connection timeout. Please check if PostgreSQL server is running at {db_params['host']}:{db_params['port']}", "ERROR", "⏱️")
        else:
            log_message(f"Database connection error: {e}", "ERROR", "❌")
        return False
    
    try:
        # Check if the target database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_params['dbname']}'")
        exists = cursor.fetchone()
        
        if not exists:
            log_message(f"Creating database '{db_params['dbname']}'", "PROCESSING", "🗄️")
            # Create the database
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(db_params['dbname'])
            ))
            log_message(f"Database '{db_params['dbname']}' created successfully", "SUCCESS", "✅")
    except Exception as db_error:
        log_message(f"Could not create database: {db_error}", "ERROR", "❌")
        cursor.close()
        conn.close()
        return False
    
    cursor.close()
    conn.close()
    
    # Now connect to the target database
    log_message(f"Connecting to PostgreSQL database: '{db_params['dbname']}'", "PROCESSING", "🔌")
    
    # Create SQLAlchemy engine
    try:
        engine_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        engine = create_engine(engine_url)
        log_message("Successfully connected to PostgreSQL database", "SUCCESS", "✅")
    except Exception as engine_error:
        log_message(f"Error creating database engine: {engine_error}", "ERROR", "❌")
        return False
    
    # Save DataFrame to PostgreSQL
    log_message(f"Saving {len(df)} records to table '{table_name}'", "PROCESSING", "📥")
    
    try:
        # Use SQLAlchemy to handle the DataFrame insertion
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  # Replace the table if it already exists
            index=False,
            chunksize=1000  # Process in chunks to handle large datasets
        )
        
        log_message(f"Successfully saved data to PostgreSQL table '{table_name}'", "SUCCESS", "🎉")
        
        # Verify the data was inserted correctly
        try:
            with engine.connect() as connection:
                result = connection.execute(sqlalchemy_text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.fetchone()[0]
                
                if count == len(df):
                    log_message(f"Data verification successful. {count} records in database.", "SUCCESS", "✓")
                else:
                    log_message(f"Data count mismatch. Expected {len(df)}, found {count}.", "WARNING", "⚠️")
        except Exception as verify_error:
            log_message(f"Could not verify data: {verify_error}", "WARNING", "⚠️")
        
        return True
    except Exception as insert_error:
        log_message(f"Error inserting data into PostgreSQL: {insert_error}", "ERROR", "❌")
        return False

def main(df: Optional[pd.DataFrame] = None, 
         input_file: Optional[str] = None,
         csv_output: str = "products.csv",
         load_to_csv_flag: bool = True,
         load_to_sheets_flag: bool = False,
         load_to_postgres_flag: bool = False,
         google_sheets_credentials: str = "google-sheets-api.json",
         google_sheet_id: Optional[str] = None,
         google_sheet_name: str = "Fashion Products Data",
         google_worksheet_name: str = "Products",
         db_params: Optional[Dict] = None,
         dry_run: bool = False) -> bool:
    """
    Main function to handle loading data to different repositories.
    
    Args:
        df: DataFrame to save (optional, will load from input_file if not provided)
        input_file: Path to input CSV file (optional if df is provided)
        csv_output: Path to save the CSV output
        load_to_csv_flag: Whether to load data to CSV
        load_to_sheets_flag: Whether to load data to Google Sheets
        load_to_postgres_flag: Whether to load data to PostgreSQL
        google_sheets_credentials: Path to Google Sheets API credentials file
        google_sheet_id: Google Sheets ID (optional)
        google_sheet_name: Name of the Google Sheet to use/create
        google_worksheet_name: Name of the worksheet within the sheet
        db_params: PostgreSQL database parameters
        dry_run: If True, validate data but do not save to repositories
        
    Returns:
        Boolean indicating overall success or failure
    """
    try:
        # Clear screen and show banner
        os.system('cls' if os.name == 'nt' else 'clear')
        print(Fore.GREEN + banner + Style.RESET_ALL)
        
        # Display header info
        print(f"{Fore.YELLOW}{'═' * 70}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  Process: {Fore.WHITE}Data Loading{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  Repositories: {Style.RESET_ALL}")
        if load_to_csv_flag:
            print(f"{Fore.YELLOW}    - CSV: {Fore.WHITE}{csv_output}{Style.RESET_ALL}")
        if load_to_sheets_flag:
            print(f"{Fore.YELLOW}    - Google Sheets: {Fore.WHITE}Using credentials from {google_sheets_credentials}{Style.RESET_ALL}")
        if load_to_postgres_flag:
            db_info = db_params or {}
            print(f"{Fore.YELLOW}    - PostgreSQL: {Fore.WHITE}{db_info.get('dbname', 'fashion_data')} @ {db_info.get('host', 'localhost')}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  Start Time: {Fore.WHITE}{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  [👤] Code brewed by: {Fore.GREEN}notsuperganang 🔥{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}{'═' * 70}{Style.RESET_ALL}\n")
        
        start_time = time.time()
        
        # Load data if DataFrame not provided
        if df is None:
            if input_file:
                log_message(f"Loading data from '{input_file}'", "PROCESSING", "📂")
                try:
                    df = pd.read_csv(input_file)
                    log_message(f"Successfully loaded {len(df)} records from '{input_file}'", "SUCCESS", "✅")
                except Exception as e:
                    log_message(f"Error loading file '{input_file}': {e}", "ERROR", "❌")
                    return False
            else:
                log_message("No data provided. Either DataFrame or input_file must be specified.", "ERROR", "❌")
                return False
        
        # Validate data before loading
        log_message("Validating data before loading", "PROCESSING", "🔍")
        required_columns = ["Title", "Price", "Rating", "Colors", "Size", "Gender"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            log_message(f"Data is missing required columns: {', '.join(missing_columns)}", "ERROR", "❌")
            log_message("Available columns: " + ", ".join(df.columns), "INFO", "ℹ️")
            return False
        
        # Check for null values in required columns
        null_counts = df[required_columns].isnull().sum()
        has_nulls = null_counts.sum() > 0
        
        if has_nulls:
            log_message("Data contains null values in required columns:", "WARNING", "⚠️")
            for col, count in null_counts.items():
                if count > 0:
                    log_message(f"  - {col}: {count} null values", "WARNING", "⚠️")
        
        # Show a sample of the data
        log_message("Sample of data to be loaded (first 5 rows):", "INFO", "👀")
        print(f"\n{Fore.CYAN}Data Sample:{Style.RESET_ALL}")
        print(df.head().to_string())
        print()
        
        # Check if this is a dry run
        if dry_run:
            log_message("DRY RUN MODE: Data will not be saved to any repository", "INFO", "🔍")
            log_message("Data validation successful. Would save to repositories:", "SUCCESS", "✓")
            if load_to_csv_flag:
                log_message(f"  - CSV: {csv_output}", "INFO", "📄")
            if load_to_sheets_flag:
                log_message(f"  - Google Sheets: Using credentials from {google_sheets_credentials}", "INFO", "📊")
            if load_to_postgres_flag:
                db_info = db_params or {}
                log_message(f"  - PostgreSQL: {db_info.get('dbname', 'fashion_data')} @ {db_info.get('host', 'localhost')}", "INFO", "🐘")
            return True
        
        # Perform loading tasks
        success_count = 0
        tasks_count = sum([load_to_csv_flag, load_to_sheets_flag, load_to_postgres_flag])
        
        if tasks_count == 0:
            log_message("No loading tasks specified. Please enable at least one repository.", "ERROR", "❌")
            return False
        
        # 1. Load to CSV
        if load_to_csv_flag:
            log_message("STEP 1/3: CSV Loading", "PROCESSING", "📄")
            csv_success = load_to_csv(df, csv_output)
            if csv_success:
                success_count += 1
        
        # 2. Load to Google Sheets
        if load_to_sheets_flag:
            log_message("STEP 2/3: Google Sheets Loading", "PROCESSING", "📊")
            sheets_success = load_to_google_sheets(
                df, 
                credentials_path=google_sheets_credentials,
                sheet_name=google_sheet_name,
                worksheet_name=google_worksheet_name,
                sheet_id=google_sheet_id  
            )
            if sheets_success:
                success_count += 1
        
        # 3. Load to PostgreSQL
        if load_to_postgres_flag:
            log_message("STEP 3/3: PostgreSQL Loading", "PROCESSING", "🐘")
            postgres_params = db_params or {
                "dbname": "fashion_data",
                "user": "postgres",
                "password": "postgres",
                "host": "localhost",
                "port": "5432"
            }
            postgres_success = load_to_postgresql(df, postgres_params)
            if postgres_success:
                success_count += 1
        
        # Display completion message
        total_time = time.time() - start_time
        print(f"\n{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}  LOADING SUMMARY{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
        print(f"  📊 {Fore.WHITE}Total records: {Fore.YELLOW}{len(df)}{Style.RESET_ALL}")
        print(f"  🎯 {Fore.WHITE}Tasks completed: {Fore.GREEN}{success_count}/{tasks_count}{Style.RESET_ALL}")
        print(f"  ⏱️ {Fore.WHITE}Processing time: {Fore.CYAN}{total_time:.2f} seconds{Style.RESET_ALL}")
        
        # Print status for each repository
        if load_to_csv_flag:
            csv_status = f"{Fore.GREEN}✓ SUCCESS" if csv_success else f"{Fore.RED}✗ FAILED"
            print(f"  💾 {Fore.WHITE}CSV: {csv_status}{Style.RESET_ALL}")
        
        if load_to_sheets_flag:
            sheets_status = f"{Fore.GREEN}✓ SUCCESS" if sheets_success else f"{Fore.RED}✗ FAILED"
            print(f"  📊 {Fore.WHITE}Google Sheets: {sheets_status}{Style.RESET_ALL}")
        
        if load_to_postgres_flag:
            postgres_status = f"{Fore.GREEN}✓ SUCCESS" if postgres_success else f"{Fore.RED}✗ FAILED"
            print(f"  🐘 {Fore.WHITE}PostgreSQL: {postgres_status}{Style.RESET_ALL}")
            
        print(f"{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
        
        # Final status
        overall_success = success_count == tasks_count
        
        if overall_success:
            log_message("LOADING COMPLETE: All repositories successfully updated!", "SUCCESS", "✓")
            print(f"\n{Fore.GREEN}★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★{Style.RESET_ALL}")
            print(f"{Fore.GREEN}★  ETL pipeline execution completed successfully!            {Style.RESET_ALL}")
            print(f"{Fore.GREEN}★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★{Style.RESET_ALL}")
        else:
            log_message(f"LOADING PARTIAL: {success_count}/{tasks_count} repositories updated.", "WARNING", "⚠️")
            print(f"\n{Fore.YELLOW}⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}⚠️  Please check the logs for error details.                {Style.RESET_ALL}")
            print(f"{Fore.YELLOW}⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️{Style.RESET_ALL}")
        
        return overall_success
        
    except Exception as e:
        log_message(f"Critical error in loading process: {e}", "ERROR", "💥")
        import traceback
        traceback.print_exc()
        return False
    
def parse_args():
    parser = argparse.ArgumentParser(description='Load transformed fashion product data to repositories.')
    parser.add_argument('--input', '-i', help='Input CSV file path')
    parser.add_argument('--csv-output', '-o', default='products.csv', help='Output CSV file path')
    parser.add_argument('--repositories', '-r', choices=['csv', 'sheets', 'postgres', 'all'], 
                        default='csv', help='Target repositories to load data (default: csv)')
    parser.add_argument('--google-creds', '-g', default='google-sheets-api.json', 
                        help='Google Sheets API credentials file')
    parser.add_argument('--google-sheet-id', '--sheet-id', 
                        help='Google Sheets ID (optional, will create/search by name if not provided)')
    parser.add_argument('--sheet-name', default='Fashion Products Data',
                        help='Google Sheet name (default: Fashion Products Data)')
    parser.add_argument('--worksheet-name', default='Products',
                        help='Worksheet name within the sheet (default: Products)')
    parser.add_argument('--db-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--db-port', default='5432', help='PostgreSQL port')
    parser.add_argument('--db-name', default='fashion_data', help='PostgreSQL database name')
    parser.add_argument('--db-user', default='postgres', help='PostgreSQL username')
    parser.add_argument('--db-pass', default='postgres', help='PostgreSQL password')
    parser.add_argument('--dry-run', action='store_true', 
                      help='Validate data but do not save to repositories (for testing)')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    # Determine which repositories to use
    load_to_csv = args.repositories in ['csv', 'all']
    load_to_sheets = args.repositories in ['sheets', 'all']
    load_to_postgres = args.repositories in ['postgres', 'all']
    
    # Set up database parameters
    db_params = {
        "dbname": args.db_name,
        "user": args.db_user,
        "password": args.db_pass,
        "host": args.db_host,
        "port": args.db_port
    }
    
    main(
        input_file=args.input,
        csv_output=args.csv_output,
        load_to_csv_flag=load_to_csv,
        load_to_sheets_flag=load_to_sheets,
        load_to_postgres_flag=load_to_postgres,
        google_sheets_credentials=args.google_creds,
        google_sheet_id=args.google_sheet_id,
        google_sheet_name=args.sheet_name,
        google_worksheet_name=args.worksheet_name,
        db_params=db_params,
        dry_run=args.dry_run
    )