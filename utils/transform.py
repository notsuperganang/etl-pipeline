import pandas as pd
import numpy as np
import re
import os
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple
from colorama import Fore, Back, Style, init

# Initialize colorama
init(autoreset=True)

# ASCII Art Banner
banner = """
╔═══════════════════════════════════════════════════════════════════════════════════╗
║                                                                                   ║
║  ████████╗██████╗  █████╗ ███╗   ██╗███████╗███████╗ ██████╗ ██████╗ ███╗   ███╗  ║
║  ╚══██╔══╝██╔══██╗██╔══██╗████╗  ██║██╔════╝██╔════╝██╔═══██╗██╔══██╗████╗ ████║  ║
║     ██║   ██████╔╝███████║██╔██╗ ██║███████╗█████╗  ██║   ██║██████╔╝██╔████╔██║  ║
║     ██║   ██╔══██╗██╔══██║██║╚██╗██║╚════██║██╔══╝  ██║   ██║██╔══██╗██║╚██╔╝██║  ║
║     ██║   ██║  ██║██║  ██║██║ ╚████║███████║██║     ╚██████╔╝██║  ██║██║ ╚═╝ ██║  ║
║     ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝╚══════╝╚═╝      ╚═════╝ ╚═╝  ╚═╝╚═╝     ╚═╝  ║
║                                                                                   ║
║  ███████╗ █████╗ ███████╗██╗  ██╗██╗ ██████╗ ███╗   ██╗                           ║
║  ██╔════╝██╔══██╗██╔════╝██║  ██║██║██╔═══██╗████╗  ██║                           ║
║  █████╗  ███████║███████╗███████║██║██║   ██║██╔██╗ ██║                           ║
║  ██╔══╝  ██╔══██║╚════██║██╔══██║██║██║   ██║██║╚██╗██║                           ║
║  ██║     ██║  ██║███████║██║  ██║██║╚██████╔╝██║ ╚████║                           ║
║  ╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝                           ║
║                                                                                   ║
║  ██████╗  █████╗ ████████╗ █████╗                                                 ║
║  ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗                                                ║
║  ██║  ██║███████║   ██║   ███████║                                                ║
║  ██║  ██║██╔══██║   ██║   ██╔══██║                                                ║
║  ██████╔╝██║  ██║   ██║   ██║  ██║                                                ║
║  ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝                                                ║
║                                                                                   ║
╚═══════════════════════════════════════════════════════════════════════════════════╝
"""

# Define dirty patterns to identify problematic values
dirty_patterns = {
    "Title": ["Unknown Product"],
    "Rating": ["Invalid Rating", "Not Rated"],
    "Price": ["Price Unavailable", None]  # None for missing values
}

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
    filled_length = int(length * current // total)
    bar = Fore.GREEN + '█' * filled_length + Fore.WHITE + '░' * (length - filled_length)
    return f"{prefix} [{bar}{Style.RESET_ALL}] {current}/{total} {suffix} ({percent:.1f}%)"

def transform_price(price_value: Optional[str], exchange_rate: float = 16000.0) -> Optional[float]:
    """
    Transform price data from USD to IDR.
    
    Args:
        price_value: The price value as a string (e.g., "$25.99", "Price Unavailable")
        exchange_rate: USD to IDR exchange rate (default: 16000.0)
        
    Returns:
        Converted price in IDR as float or None if invalid
    """
    try:
        if not price_value or price_value in dirty_patterns["Price"] or "Price Unavailable" in str(price_value):
            return None
        
        # Extract numeric value using regex
        match = re.search(r'(\d+\.?\d*)', str(price_value))
        if match:
            # Convert to float and multiply by exchange rate
            usd_price = float(match.group(1))
            idr_price = usd_price * exchange_rate
            return float(idr_price)
        else:
            log_message(f"Could not extract price from: {price_value}", "WARNING", "⚠️")
            return None
            
    except Exception as e:
        log_message(f"Error transforming price '{price_value}': {e}", "ERROR", "❌")
        return None

def transform_title(title_value: Optional[str]) -> Optional[str]:
    """
    Transform product title.
    
    Args:
        title_value: The title as a string
        
    Returns:
        Cleaned title or None if invalid
    """
    try:
        if not title_value or title_value in dirty_patterns["Title"]:
            return None
        
        # Clean and return the title
        return title_value.strip()
        
    except Exception as e:
        log_message(f"Error transforming title '{title_value}': {e}", "ERROR", "❌")
        return None

def transform_rating(rating_value: Optional[str]) -> Optional[float]:
    """
    Transform rating data to extract numeric value.
    
    Args:
        rating_value: The rating value as a string (e.g., "⭐ 4.8 / 5", "Invalid Rating")
        
    Returns:
        Rating as float or None if invalid
    """
    try:
        if not rating_value:
            return None
            
        # Check if the rating contains any of the dirty patterns
        if any(pattern in str(rating_value) for pattern in dirty_patterns["Rating"]):
            return None
        
        # Extract numeric value using regex
        match = re.search(r'(\d+\.?\d*)', str(rating_value))
        if match:
            return float(match.group(1))
        else:
            log_message(f"Could not extract rating from: {rating_value}", "WARNING", "⚠️")
            return None
            
    except Exception as e:
        log_message(f"Error transforming rating '{rating_value}': {e}", "ERROR", "❌")
        return None

def transform_colors(colors_value: Optional[str]) -> Optional[int]:
    """
    Transform colors data to extract number of colors.
    
    Args:
        colors_value: The colors value as a string (e.g., "3 Colors")
        
    Returns:
        Number of colors as int or None if invalid
    """
    try:
        if not colors_value:
            return None
        
        # Extract numeric value using regex
        match = re.search(r'(\d+)', str(colors_value))
        if match:
            return int(match.group(1))
        else:
            log_message(f"Could not extract number of colors from: {colors_value}", "WARNING", "⚠️")
            return None
            
    except Exception as e:
        log_message(f"Error transforming colors '{colors_value}': {e}", "ERROR", "❌")
        return None

def transform_size(size_value: Optional[str]) -> Optional[str]:
    """
    Transform size data to remove prefix.
    
    Args:
        size_value: The size value as a string (e.g., "Size: M")
        
    Returns:
        Size as string without prefix or None if invalid
    """
    try:
        if not size_value:
            return None
        
        # Extract size part after "Size: " prefix
        match = re.search(r'Size:\s*(.+)', str(size_value))
        if match:
            return match.group(1).strip()
        else:
            # If no "Size: " prefix, return as is
            return str(size_value).strip()
            
    except Exception as e:
        log_message(f"Error transforming size '{size_value}': {e}", "ERROR", "❌")
        return None

def transform_gender(gender_value: Optional[str]) -> Optional[str]:
    """
    Transform gender data to remove prefix.
    
    Args:
        gender_value: The gender value as a string (e.g., "Gender: Male")
        
    Returns:
        Gender as string without prefix or None if invalid
    """
    try:
        if not gender_value:
            return None
        
        # Extract gender part after "Gender: " prefix
        match = re.search(r'Gender:\s*(.+)', str(gender_value))
        if match:
            return match.group(1).strip()
        else:
            # If no "Gender: " prefix, return as is
            return str(gender_value).strip()
            
    except Exception as e:
        log_message(f"Error transforming gender '{gender_value}': {e}", "ERROR", "❌")
        return None

def check_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check for missing values and log results.
    
    Args:
        df: DataFrame to check
        
    Returns:
        The same DataFrame (for chaining)
    """
    missing_counts = df.isna().sum()
    total_rows = len(df)
    
    log_message("Missing value analysis:", "INFO", "📊")
    for column, count in missing_counts.items():
        if count > 0:
            percentage = (count / total_rows) * 100
            log_message(f"  - {column}: {count} missing values ({percentage:.2f}%)", 
                     "WARNING" if percentage > 5 else "INFO", 
                     "⚠️" if percentage > 5 else "ℹ️")
    
    return df

def check_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check data types and log results.
    
    Args:
        df: DataFrame to check
        
    Returns:
        The same DataFrame (for chaining)
    """
    log_message("Data type analysis:", "INFO", "🔍")
    for column, dtype in df.dtypes.items():
        log_message(f"  - {column}: {dtype}", "INFO", "ℹ️")
    
    return df

def validate_and_clean_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Validate and clean the data by removing invalid rows and fixing data issues.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        Tuple containing:
        - Cleaned DataFrame
        - Dictionary with counts of issues found and fixed
    """
    # Create a copy to avoid modifying the original
    df_clean = df.copy()
    
    # Initialize counters for issues
    issue_counts = {
        "missing_title": 0,
        "missing_price": 0,
        "duplicate_rows": 0,
        "invalid_rating": 0,
        "invalid_colors": 0,
        "rows_before": len(df_clean),
        "rows_after": 0
    }
    
    # Track progress
    log_message("Starting data validation and cleaning process", "PROCESSING", "🧹")
    
    # Check for and remove duplicate rows
    duplicates = df_clean.duplicated()
    duplicate_count = duplicates.sum()
    if duplicate_count > 0:
        df_clean = df_clean[~duplicates]
        issue_counts["duplicate_rows"] = duplicate_count
        log_message(f"Removed {duplicate_count} duplicate rows", "INFO", "🔄")
    
    # Remove rows with missing essential data (Title or Price)
    missing_title = df_clean["Title"].isna()
    missing_title_count = missing_title.sum()
    
    missing_price = df_clean["Price"].isna()
    missing_price_count = missing_price.sum()
    
    if missing_title_count > 0 or missing_price_count > 0:
        df_clean = df_clean[~(missing_title | missing_price)]
        issue_counts["missing_title"] = missing_title_count
        issue_counts["missing_price"] = missing_price_count
        log_message(f"Removed {missing_title_count} rows with missing Title", "INFO", "📝")
        log_message(f"Removed {missing_price_count} rows with missing Price", "INFO", "💰")
    
    # Set rows_after count
    issue_counts["rows_after"] = len(df_clean)
    
    return df_clean, issue_counts

def transform_data(df: pd.DataFrame, exchange_rate: float = 16000.0) -> pd.DataFrame:
    """
    Apply all transformations to the dataset.
    
    Args:
        df: DataFrame with raw data
        exchange_rate: USD to IDR exchange rate (default: 16000.0)
        
    Returns:
        Transformed DataFrame
    """
    # Create a copy to avoid modifying the original
    df_transformed = df.copy()
    
    # Get total number of rows for progress tracking
    total_rows = len(df_transformed)
    
    log_message("Starting data transformation process", "PROCESSING", "🔄")
    
    # Display initial data info
    log_message(f"Input DataFrame has {total_rows} rows and {len(df.columns)} columns", "INFO", "📋")
    
    # Transform each column with progress display
    columns_to_transform = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender']
    
    for i, column in enumerate(columns_to_transform):
        log_message(f"Transforming '{column}' column", "PROCESSING", "🔄")
        
        # Show progress bar
        progress = int(((i) / len(columns_to_transform)) * 100)
        print(show_progress_bar(i, len(columns_to_transform), 
                               prefix=f"{Fore.CYAN}Column Transformation Progress:", 
                               suffix=f"columns"))
        
        # Apply appropriate transformation function
        if column == 'Title':
            df_transformed[column] = df_transformed[column].apply(transform_title)
        elif column == 'Price':
            df_transformed[column] = df_transformed[column].apply(lambda x: transform_price(x, exchange_rate))
        elif column == 'Rating':
            df_transformed[column] = df_transformed[column].apply(transform_rating)
        elif column == 'Colors':
            df_transformed[column] = df_transformed[column].apply(transform_colors)
        elif column == 'Size':
            df_transformed[column] = df_transformed[column].apply(transform_size)
        elif column == 'Gender':
            df_transformed[column] = df_transformed[column].apply(transform_gender)
    
    # Show final progress
    print(show_progress_bar(len(columns_to_transform), len(columns_to_transform), 
                           prefix=f"{Fore.CYAN}Column Transformation Progress:", 
                           suffix=f"columns"))
    
    # Validate and clean the data
    df_transformed, issue_counts = validate_and_clean_data(df_transformed)
    
    # Check for remaining missing values
    check_missing_values(df_transformed)
    
    # Ensure correct data types
    df_transformed['Price'] = pd.to_numeric(df_transformed['Price'], errors='coerce')
    df_transformed['Rating'] = pd.to_numeric(df_transformed['Rating'], errors='coerce')
    df_transformed['Colors'] = pd.to_numeric(df_transformed['Colors'], errors='coerce')
    
    # Handle timestamp column - keep it as string for compatibility with Google Sheets
    if 'timestamp' in df_transformed.columns:
        # If timestamp is already a string and in valid format, keep it as is
        if df_transformed['timestamp'].dtype == 'object':
            # Validate that it's a proper datetime string format
            try:
                # Test parse the first non-null value to ensure it's valid
                sample_timestamp = df_transformed['timestamp'].dropna().iloc[0] if len(df_transformed['timestamp'].dropna()) > 0 else None
                if sample_timestamp:
                    pd.to_datetime(sample_timestamp)
                    log_message("Timestamp column is already in string format - keeping for Google Sheets compatibility", "INFO", "📅")
            except Exception as e:
                log_message(f"Warning: Timestamp might not be in valid format: {e}", "WARNING", "⚠️")
        else:
            # If it's not a string, convert it to pandas datetime first, then back to string
            try:
                # Convert to datetime first (if it's not already)
                df_transformed['timestamp'] = pd.to_datetime(df_transformed['timestamp'])
                # Then convert back to ISO string format for compatibility with all repositories
                df_transformed['timestamp'] = df_transformed['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
                log_message("Converted timestamp to string format for Google Sheets compatibility", "SUCCESS", "✅")
            except Exception as e:
                log_message(f"Could not handle timestamp column: {e}", "WARNING", "⚠️")
    
    # Check data types after conversion
    check_data_types(df_transformed)
    
    # Final success message
    log_message(f"Data transformation completed successfully! {len(df_transformed)} clean records produced.", 
                "SUCCESS", "✅")
    
    # Report on issues fixed
    rows_removed = issue_counts["rows_before"] - issue_counts["rows_after"]
    if rows_removed > 0:
        log_message(f"Removed {rows_removed} problematic rows during cleaning", 
                   "INFO" if rows_removed < issue_counts["rows_before"] * 0.1 else "WARNING",
                   "🧹")
    
    return df_transformed

def find_latest_csv(directory: str = '.', prefix: str = 'fashion_products_') -> Optional[str]:
    """
    Find the most recent CSV file with the given prefix in the specified directory.
    
    Args:
        directory: Directory to search in (default: current directory)
        prefix: Prefix for CSV files to search for
        
    Returns:
        Path to the most recent CSV file or None if not found
    """
    try:
        # Get all matching CSV files in the directory
        csv_files = [os.path.join(directory, f) for f in os.listdir(directory) 
                    if f.startswith(prefix) and f.endswith('.csv')]
        
        if not csv_files:
            return None
            
        # Find the most recent file
        latest_csv = max(csv_files, key=os.path.getmtime)
        
        # Normalisasi path untuk konsistensi cross-platform
        # 1. Gunakan os.path.normpath untuk normalisasi umum
        latest_csv = os.path.normpath(latest_csv)
        # 2. Ubah backslash ke forward slash untuk konsistensi di semua platform
        latest_csv = latest_csv.replace('\\', '/')
        
        # Pastikan format path konsisten dengan prefix ./ jika diperlukan
        if directory == '.' and not latest_csv.startswith('./'):
            latest_csv = './' + latest_csv
            
        return latest_csv
        
    except Exception as e:
        log_message(f"Error finding latest CSV file: {e}", "ERROR", "❌")
        return None

def main(input_file: Optional[str] = None, exchange_rate: float = 16000.0, 
        dataset_dir: str = '') -> pd.DataFrame:
    """
    Main function to run the transformation process.
    
    Args:
        input_file: Path to the input CSV file (optional)
        exchange_rate: USD to IDR exchange rate (default: 16000.0)
        dataset_dir: Directory containing dataset files (default: current directory)
        
    Returns:
        Transformed DataFrame
    """
    try:
        # Clear screen and show banner
        os.system('cls' if os.name == 'nt' else 'clear')
        print(Fore.GREEN + banner + Style.RESET_ALL)
        
        # Display header info
        print(f"{Fore.YELLOW}{'═' * 70}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  Process: {Fore.WHITE}Data Transformation{Style.RESET_ALL}")
        if input_file:
            print(f"{Fore.YELLOW}  Input File: {Fore.WHITE}{input_file}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  USD to IDR Exchange Rate: {Fore.WHITE}Rp{exchange_rate:,.0f}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  Start Time: {Fore.WHITE}{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  [👤] Code brewed by: {Fore.GREEN}notsuperganang 🔥{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}{'═' * 70}{Style.RESET_ALL}\n")
        
        # Load data
        start_time = time.time()
        
        if input_file:
            log_message(f"Loading data from '{input_file}'", "PROCESSING", "📂")
            try:
                df = pd.read_csv(input_file)
                log_message(f"Successfully loaded {len(df)} records from '{input_file}'", "SUCCESS", "✅")
            except Exception as e:
                log_message(f"Error loading file '{input_file}': {e}", "ERROR", "❌")
                return pd.DataFrame()
        else:
            log_message("No input file specified, searching for most recent CSV file", "INFO", "🔍")
            
            # Find the most recent fashion product CSV
            search_dir = dataset_dir if dataset_dir else '.'
            latest_csv = find_latest_csv(search_dir)
            
            if not latest_csv:
                alt_path = os.path.join(search_dir, 'dataset') if not dataset_dir else None
                if alt_path and os.path.exists(alt_path):
                    latest_csv = find_latest_csv(alt_path)
                    
            if not latest_csv:
                log_message("No fashion product CSV files found!", "ERROR", "❌")
                return pd.DataFrame()
                
            log_message(f"Found latest CSV file: {latest_csv}", "SUCCESS", "🎯")
            try:
                df = pd.read_csv(latest_csv)
                log_message(f"Successfully loaded {len(df)} records from '{latest_csv}'", "SUCCESS", "✅")
            except Exception as e:
                log_message(f"Error loading file '{latest_csv}': {e}", "ERROR", "❌")
                return pd.DataFrame()
        
        # Show a sample of the data
        log_message("Sample of raw data (first 5 rows):", "INFO", "👀")
        print(f"\n{Fore.CYAN}Raw Data Sample:{Style.RESET_ALL}")
        print(df.head().to_string())
        print()
        
        # Transform data
        show_spinner(1.5, "Preparing transformation process")
        df_transformed = transform_data(df, exchange_rate)
        
        # Show a sample of the transformed data
        log_message("Sample of transformed data (first 5 rows):", "INFO", "👀")
        print(f"\n{Fore.GREEN}Transformed Data Sample:{Style.RESET_ALL}")
        print(df_transformed.head().to_string())
        print()        

        # Display completion message
        total_time = time.time() - start_time
        print(f"\n{Fore.GREEN}{'═' * 70}{Style.RESET_ALL}")
        log_message(f"Transformation complete! Processed {len(df)} records in {total_time:.2f} seconds", 
                   "SUCCESS", "🏆")
        
        # Print summary stats
        print(f"\n{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}  TRANSFORMATION SUMMARY{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
        print(f"  📊 {Fore.WHITE}Input records: {Fore.YELLOW}{len(df)}{Style.RESET_ALL}")
        print(f"  📈 {Fore.WHITE}Output records: {Fore.GREEN}{len(df_transformed)}{Style.RESET_ALL}")
        print(f"  🔄 {Fore.WHITE}Records removed: {Fore.RED}{len(df) - len(df_transformed)}{Style.RESET_ALL}")
        print(f"  ⏱️ {Fore.WHITE}Processing time: {Fore.CYAN}{total_time:.2f} seconds{Style.RESET_ALL}")
        print(f"  🚀 {Fore.WHITE}Records per second: {Fore.GREEN}{len(df) / total_time:.2f}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
        
        return df_transformed
        
    except Exception as e:
        log_message(f"Critical error in transformation process: {e}", "ERROR", "💥")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()  # Return empty DataFrame in case of error

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Transform fashion product data.')
    parser.add_argument('--input', '-i', help='Input CSV file path')
    parser.add_argument('--exchange-rate', '-e', type=float, default=16000.0, 
                       help='USD to IDR exchange rate (default: 16000.0)')
    parser.add_argument('--dataset-dir', '-d', default='', 
                       help='Directory containing dataset files')
    
    args = parser.parse_args()
    
    main(args.input, args.exchange_rate, args.dataset_dir)