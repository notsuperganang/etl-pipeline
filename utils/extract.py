import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import time
from datetime import datetime
import re
from typing import List, Dict, Any, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
            logger.info(f"Fetching page: {url}")
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                return BeautifulSoup(response.content, 'html.parser')
            else:
                logger.warning(f"Failed to fetch {url}. Status code: {response.status_code}")
                
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
        
        retries += 1
        logger.info(f"Retrying ({retries}/{max_retries}) after {retry_delay} seconds...")
        time.sleep(retry_delay)
    
    logger.error(f"Max retries reached for {url}. Giving up.")
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
        logger.error(f"Error extracting product details: {e}")
    
    return product_data

def get_total_pages(soup: BeautifulSoup) -> int:
    """
    Extract the total number of pages from the pagination information.
    
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
                return int(match.group(2))
    except Exception as e:
        logger.error(f"Error getting total pages: {e}")
    
    # Default to 50 pages if we can't determine
    logger.warning("Could not determine total pages, defaulting to 50")
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
    
    # Extract all product cards
    product_details_divs = soup.select('.product-details')
    
    for product_div in product_details_divs:
        product_data = extract_product_details(product_div)
        products.append(product_data)
    
    logger.info(f"Extracted {len(products)} products from {page_url}")
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
    
    try:
        # Start with the first page
        first_page_url = f"{base_url}"
        products, total_pages = extract_products_from_page(first_page_url)
        all_products.extend(products)
        
        # Determine how many pages to scrape
        if total_pages:
            pages_to_scrape = min(total_pages, max_pages)
        else:
            pages_to_scrape = max_pages
            
        logger.info(f"Found {pages_to_scrape} total pages to scrape")
        
        # Scrape remaining pages
        for page_num in range(2, pages_to_scrape + 1):
            page_url = f"{base_url}/page{page_num}"
            products, _ = extract_products_from_page(page_url)
            all_products.extend(products)
            
            # Add a small delay to be respectful to the server
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_products)
    
    # Add timestamp
    df['timestamp'] = datetime.now().isoformat()
    
    logger.info(f"Total products scraped: {len(df)}")
    return df

def main():
    """Main function to run the extraction process."""
    df = scrape_all_products()
    
    # Save raw data to CSV for debugging/backup
    df.to_csv('raw_products.csv', index=False)
    logger.info("Raw data saved to 'raw_products.csv'")
    
    return df

if __name__ == "__main__":
    main()