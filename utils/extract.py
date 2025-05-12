import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import re
import os
from typing import List, Dict, Any, Optional, Tuple
from colorama import Fore, Back, Style, init
import random
import sys

# Initialize colorama
init(autoreset=True)

# ASCII Art Banner
banner = """
╔═══════════════════════════════════════════════════════════════════╗
║                                                                   ║
║  ███████╗ █████╗ ███████╗██╗  ██╗██╗ ██████╗ ███╗   ██╗           ║
║  ██╔════╝██╔══██╗██╔════╝██║  ██║██║██╔═══██╗████╗  ██║           ║
║  █████╗  ███████║███████╗███████║██║██║   ██║██╔██╗ ██║           ║
║  ██╔══╝  ██╔══██║╚════██║██╔══██║██║██║   ██║██║╚██╗██║           ║
║  ██║     ██║  ██║███████║██║  ██║██║╚██████╔╝██║ ╚████║           ║
║  ╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝           ║
║                                                                   ║
║  ███████╗████████╗██╗   ██╗██████╗ ██╗ ██████╗                    ║
║  ██╔════╝╚══██╔══╝██║   ██║██╔══██╗██║██╔═══██╗                   ║
║  ███████╗   ██║   ██║   ██║██║  ██║██║██║   ██║                   ║
║  ╚════██║   ██║   ██║   ██║██║  ██║██║██║   ██║                   ║
║  ███████║   ██║   ╚██████╔╝██████╔╝██║╚██████╔╝                   ║
║  ╚══════╝   ╚═╝    ╚═════╝ ╚═════╝ ╚═╝ ╚═════╝                    ║
║                                                                   ║
║  ███████╗ ██████╗██████╗  █████╗ ██████╗ ███████╗██████╗          ║
║  ██╔════╝██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗         ║
║  ███████╗██║     ██████╔╝███████║██████╔╝█████╗  ██████╔╝         ║
║  ╚════██║██║     ██╔══██╗██╔══██║██╔═══╝ ██╔══╝  ██╔══██╗         ║
║  ███████║╚██████╗██║  ██║██║  ██║██║     ███████╗██║  ██║         ║
║  ╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚══════╝╚═╝  ╚═╝         ║
║                                                                   ║
╚═══════════════════════════════════════════════════════════════════╝
"""

# Function to display fancy log messages
def log_message(message, level="INFO", emoji=""):
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
    spinner = ['⣾', '⣽', '⣻', '⢿', '⡿', '⣟', '⣯', '⣷']
    for _ in range(int(seconds * 5)):
        for char in spinner:
            print(f"\r{Fore.CYAN}{message} {char}{Style.RESET_ALL}", end='', flush=True)
            time.sleep(0.2)
    print()

# Function to display progress bar
def show_progress_bar(current, total, prefix="", suffix="", length=50):
    percent = (current / total) * 100 if total > 0 else 0
    filled_length = int(length * current // total)
    bar = Fore.GREEN + '█' * filled_length + Fore.WHITE + '░' * (length - filled_length)
    return f"{prefix} [{bar}{Style.RESET_ALL}] {current}/{total} {suffix} ({percent:.1f}%)"

def get_page_content(url: str, max_retries: int = 3, retry_delay: int = 2) -> Optional[BeautifulSoup]:
    """
    Fetch and parse a web page with retry mechanism.
    
    Args:
        url: The URL to fetch
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Delay between retries in seconds (default: 2)
        
    Returns:
        BeautifulSoup object with the parsed HTML content or None if failed
    """
    retries = 0
    
    while retries < max_retries:
        try:
            log_message(f"Fetching page: {url}", "PROCESSING", "🌐")
            
            # Show spinner while waiting for response
            print(f"\r{Fore.CYAN}Connecting to server... ⏳{Style.RESET_ALL}", end='', flush=True)
            
            response = requests.get(url, timeout=10)
            print()  # Clear the spinner line
            
            if response.status_code == 200:
                log_message(f"Successfully fetched page: {url}", "SUCCESS", "✅")
                return BeautifulSoup(response.content, 'html.parser')
            else:
                log_message(f"Failed to fetch {url}. Status code: {response.status_code}", "WARNING", "⚠️")
                
        except requests.RequestException as e:
            log_message(f"Error fetching {url}: {e}", "ERROR", "❌")
        
        retries += 1
        log_message(f"Retrying ({retries}/{max_retries}) after {retry_delay} seconds...", "INFO", "🔄")
        
        # Show spinner during retry delay
        show_spinner(retry_delay, "Waiting before retry")
    
    log_message(f"Max retries reached for {url}. Giving up.", "ERROR", "🛑")
    return None

def extract_product_details(product_div: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract all required details from a product div.
    
    Args:
        product_div: BeautifulSoup object representing a product div
        
    Returns:
        Dictionary with extracted product details
    """
    product_data = {
        "Title": "Unknown Product",
        "Price": None,
        "Rating": None,
        "Colors": None,
        "Size": None,
        "Gender": None
    }
    
    try:
        # Extract title
        title_elem = product_div.select_one('.product-title')
        if title_elem:
            product_data["Title"] = title_elem.text.strip()
        
        # Extract price
        price_elem = product_div.select_one('.price')
        if price_elem:
            # Remove any currency symbols and convert to float
            price_text = price_elem.text.strip()
            # Just store the raw price text for now, transformation will happen later
            product_data["Price"] = price_text
        
        # Extract rating
        rating_elem = product_div.find('p', string=lambda t: t and 'Rating:' in t)
        if rating_elem:
            product_data["Rating"] = rating_elem.text.strip()
        
        # Extract colors
        colors_elem = product_div.find('p', string=lambda t: t and 'Colors' in t)
        if colors_elem:
            product_data["Colors"] = colors_elem.text.strip()
        
        # Extract size
        size_elem = product_div.find('p', string=lambda t: t and 'Size:' in t)
        if size_elem:
            product_data["Size"] = size_elem.text.strip()
        
        # Extract gender
        gender_elem = product_div.find('p', string=lambda t: t and 'Gender:' in t)
        if gender_elem:
            product_data["Gender"] = gender_elem.text.strip()
            
    except Exception as e:
        log_message(f"Error extracting product details: {e}", "ERROR", "❌")
    
    return product_data

def get_total_pages(soup: BeautifulSoup) -> int:
    """
    Extract the total number of pages from the pagination information. IN CASE THERE ARE MORE THAN 50 PAGES, IT WILL DEFAULT TO 50. 
    
    Args:
        soup: BeautifulSoup object of the page containing pagination
        
    Returns:
        Total number of pages as an integer
    """
    try:
        pagination_info = soup.select_one('.page-item.current .page-link')
        if pagination_info:
            # Extract "X of Y" format
            match = re.search(r'(\d+) of (\d+)', pagination_info.text)
            if match:
                total_pages = int(match.group(2))
                log_message(f"Found {total_pages} total pages of products", "SUCCESS", "📚")
                return total_pages
    except Exception as e:
        log_message(f"Error getting total pages: {e}", "ERROR", "❌")
    
    # Default to 50 pages if we can't determine
    log_message("Could not determine total pages, defaulting to 50", "WARNING", "⚠️")
    return 50

def extract_products_from_page(page_url: str) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Extract all products from a single page.
    
    Args:
        page_url: URL of the page to scrape
        
    Returns:
        Tuple containing:
        - List of dictionaries, each containing product details
        - Total number of pages (on first page only, otherwise None)
    """
    soup = get_page_content(page_url)
    if not soup:
        return [], None
    
    products = []
    total_pages = None
    
    # Get total pages (only needed from first page)
    if '/page' not in page_url:
        total_pages = get_total_pages(soup)
    
    # Display extraction start message
    page_number = page_url.split('page')[-1] if '/page' in page_url else "1"
    log_message(f"Extracting products from page {page_number}", "PROCESSING", "🔍")
    
    # Extract all product cards
    product_details_divs = soup.select('.product-details')
    
    # Show progress for extraction
    total_products = len(product_details_divs)
    log_message(f"Found {total_products} products on page {page_number}", "INFO", "📋")
    
    for i, product_div in enumerate(product_details_divs):
        # Display progress periodically
        if total_products > 10 and i % 5 == 0:
            progress = int((i / total_products) * 100)
            sys.stdout.write(f"\r{Fore.CYAN}Extracting product data... {progress}% complete {Fore.GREEN}{'█' * (progress//5)}{Style.RESET_ALL}")
            sys.stdout.flush()
            
        product_data = extract_product_details(product_div)
        products.append(product_data)
    
    # Clear progress line if we printed one
    if total_products > 10:
        sys.stdout.write("\r" + " " * 80 + "\r")
        sys.stdout.flush()
    
    # Success message with random emoji
    emoji_options = ["📦", "🛍️", "🎁", "📝", "💼"]
    log_message(f"Successfully extracted {len(products)} products from page {page_number}", 
               "SUCCESS", random.choice(emoji_options))
    
    return products, total_pages

def scrape_all_products(base_url: str = 'https://fashion-studio.dicoding.dev', 
                       max_pages: int = 50) -> pd.DataFrame:
    """
    Scrape all products from all pages.
    
    Args:
        base_url: Base URL of the website
        max_pages: Maximum number of pages to scrape
        
    Returns:
        DataFrame containing all scraped products
    """
    all_products = []
    start_time = time.time()
    
    # Clear screen and show banner
    os.system('cls' if os.name == 'nt' else 'clear')
    print(Fore.GREEN + banner + Style.RESET_ALL)
    
    # Display header info
    print(f"{Fore.YELLOW}{'═' * 70}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  Target Website: {Fore.WHITE}{base_url}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  Max Pages: {Fore.WHITE}{max_pages}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  Start Time: {Fore.WHITE}{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  [👤] Code brewed by: {Fore.GREEN}notsuperganang 🔥{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}{'═' * 70}{Style.RESET_ALL}\n")
    
    try:
        # Start with the first page
        log_message("Starting extraction process", "INFO", "🚀")
        first_page_url = f"{base_url}"
        products, total_pages = extract_products_from_page(first_page_url)
        all_products.extend(products)
        
        # Determine how many pages to scrape
        if total_pages:
            pages_to_scrape = min(total_pages, max_pages)
        else:
            pages_to_scrape = max_pages
            
        log_message(f"Planning to scrape {pages_to_scrape} total pages", "INFO", "📊")
        
        # Print divider
        print(f"{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
        
        # Scrape remaining pages
        for page_num in range(2, pages_to_scrape + 1):
            page_url = f"{base_url}/page{page_num}"
            products, _ = extract_products_from_page(page_url)
            all_products.extend(products)
            
            # Show overall progress
            elapsed = time.time() - start_time
            progress_percent = (page_num / pages_to_scrape) * 100
            rate = page_num / elapsed if elapsed > 0 else 0
            remaining = (pages_to_scrape - page_num) / rate if rate > 0 else 0
            
            print(show_progress_bar(
                page_num, 
                pages_to_scrape, 
                prefix=f"{Fore.CYAN}Overall Progress:", 
                suffix=f"pages {Fore.YELLOW}(Est. {remaining:.1f}s remaining)"
            ))
            
            # Add a small delay to be respectful to the server
            if page_num < pages_to_scrape:
                delay = random.uniform(0.8, 1.5)
                show_spinner(delay, "Respecting server limits")
            
        # Print final divider    
        print(f"{Fore.CYAN}{'─' * 70}{Style.RESET_ALL}")
            
    except Exception as e:
        log_message(f"Error during scraping: {e}", "ERROR", "❌")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_products)
    
    # Add timestamp
    df['timestamp'] = datetime.now().isoformat()
    
    # Display final stats
    total_time = time.time() - start_time
    products_per_second = len(df) / total_time if total_time > 0 else 0
    
    print(f"\n{Fore.GREEN}{'═' * 70}{Style.RESET_ALL}")
    log_message(f"Scraping completed! Total products collected: {len(df)}", "SUCCESS", "🏆")
    log_message(f"Time taken: {total_time:.2f} seconds ({products_per_second:.2f} products/sec)", "INFO", "⏱️")
    
    return df

def main():
    """Main function to run the extraction process."""
    try:
        df = scrape_all_products()
        
        # Save raw data to CSV for debugging/backup
        output_file = f'fashion_products_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(output_file, index=False)
        log_message(f"Raw data saved to '{output_file}'", "SUCCESS", "💾")
        
        # Display completion message
        print(f"\n{Fore.GREEN}★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★{Style.RESET_ALL}")
        print(f"{Fore.GREEN}★  EXTRACTION COMPLETE: Collected {len(df)} products!        {Style.RESET_ALL}")
        print(f"{Fore.GREEN}★  Data is ready for transformation & loading stages!       {Style.RESET_ALL}")
        print(f"{Fore.GREEN}★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★{Style.RESET_ALL}")
        
        return df
    
    except Exception as e:
        log_message(f"Critical error in main extraction process: {e}", "ERROR", "💥")
        return pd.DataFrame()  # Return empty DataFrame in case of error

if __name__ == "__main__":
    main()