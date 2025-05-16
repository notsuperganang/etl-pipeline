"""
Unit tests for Fashion Studio ETL Pipeline - Extract Module

This module contains comprehensive tests for the extract.py module with 100% coverage.
Tests cover all functions including error handling and edge cases.

Dependencies:
- pytest: Testing framework
- pytest-mock: For mocking
- pytest-cov: For coverage reporting
- requests_mock: For mocking HTTP requests
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, call
from io import StringIO
import sys
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup

# Import modules to test
sys.path.insert(0, '.')
from utils.extract import (
    log_message,
    show_spinner,
    show_progress_bar,
    get_page_content,
    extract_product_details,
    get_total_pages,
    extract_products_from_page,
    scrape_all_products,
    main
)


class TestLogMessage:
    """Test cases for log_message function"""
    
    @patch('utils.extract.datetime')
    @patch('builtins.print')
    def test_log_message_info(self, mock_print, mock_datetime):
        """Test log_message with INFO level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Test message", "INFO", "🔍")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "2025-01-01 12:00:00" in printed_text
        assert "[INFO]" in printed_text
        assert "Test message" in printed_text
        assert "🔍" in printed_text
    
    @patch('utils.extract.datetime')
    @patch('builtins.print')
    def test_log_message_success(self, mock_print, mock_datetime):
        """Test log_message with SUCCESS level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Success message", "SUCCESS", "✅")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[SUCCESS]" in printed_text
    
    @patch('utils.extract.datetime')
    @patch('builtins.print')
    def test_log_message_warning(self, mock_print, mock_datetime):
        """Test log_message with WARNING level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Warning message", "WARNING", "⚠️")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[WARNING]" in printed_text
    
    @patch('utils.extract.datetime')
    @patch('builtins.print')
    def test_log_message_error(self, mock_print, mock_datetime):
        """Test log_message with ERROR level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Error message", "ERROR", "❌")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[ERROR]" in printed_text
    
    @patch('utils.extract.datetime')
    @patch('builtins.print')
    def test_log_message_processing(self, mock_print, mock_datetime):
        """Test log_message with PROCESSING level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Processing message", "PROCESSING", "🔄")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[PROCESSING]" in printed_text
    
    @patch('utils.extract.datetime')
    @patch('builtins.print')
    def test_log_message_custom_level(self, mock_print, mock_datetime):
        """Test log_message with custom level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Custom message", "CUSTOM", "🎯")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[CUSTOM]" in printed_text
    
    @patch('utils.extract.datetime')
    @patch('builtins.print')
    def test_log_message_no_emoji(self, mock_print, mock_datetime):
        """Test log_message without emoji"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("No emoji message", "INFO")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "No emoji message" in printed_text


class TestShowSpinner:
    """Test cases for show_spinner function"""
    
    @patch('utils.extract.time.sleep')
    @patch('builtins.print')
    def test_show_spinner(self, mock_print, mock_sleep):
        """Test show_spinner functionality"""
        # Mock time.sleep to avoid actual waiting
        show_spinner(0.5, "Loading")
        
        # Should have called print multiple times for spinner animation
        assert mock_print.call_count > 0
        
        # Check that sleep was called
        assert mock_sleep.call_count > 0
        
        # Verify final print call (empty line)
        final_call = mock_print.call_args_list[-1]
        assert final_call == call()


class TestShowProgressBar:
    """Test cases for show_progress_bar function"""
    
    def test_show_progress_bar_complete(self):
        """Test progress bar at 100%"""
        result = show_progress_bar(100, 100, "Progress:", "items")
        
        assert "Progress:" in result
        assert "100/100" in result
        assert "items" in result
        assert "(100.0%)" in result
        assert "█" in result  # Progress bar filled
    
    def test_show_progress_bar_half(self):
        """Test progress bar at 50%"""
        result = show_progress_bar(50, 100, "Progress:", "items")
        
        assert "50/100" in result
        assert "(50.0%)" in result
        assert "█" in result  # Some progress
        assert "░" in result  # Some empty
    
    def test_show_progress_bar_zero(self):
        """Test progress bar at 0%"""
        result = show_progress_bar(0, 100, "Progress:", "items")
        
        assert "0/100" in result
        assert "(0.0%)" in result
        assert "░" in result  # All empty
    
    def test_show_progress_bar_zero_total(self):
        """Test progress bar with zero total"""
        result = show_progress_bar(0, 0, "Progress:", "items")
        
        assert "0/0" in result
        assert "(0.0%)" in result
    
    def test_show_progress_bar_no_prefix_suffix(self):
        """Test progress bar without prefix and suffix"""
        result = show_progress_bar(25, 100)
        
        assert "25/100" in result
        assert "(25.0%)" in result


class TestGetPageContent:
    """Test cases for get_page_content function"""
    
    @patch('utils.extract.requests.get')
    @patch('utils.extract.log_message')
    @patch('builtins.print')
    def test_get_page_content_success(self, mock_print, mock_log, mock_get):
        """Test successful page content retrieval"""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'<html><body>Test</body></html>'
        mock_get.return_value = mock_response
        
        result = get_page_content("http://test.com")
        
        assert result is not None
        assert isinstance(result, BeautifulSoup)
        mock_get.assert_called_once_with("http://test.com", timeout=10)
    
    @patch('utils.extract.requests.get')
    @patch('utils.extract.log_message')
    @patch('utils.extract.show_spinner')
    @patch('builtins.print')
    def test_get_page_content_retry_then_success(self, mock_print, mock_spinner, mock_log, mock_get):
        """Test retry mechanism with eventual success"""
        # First call fails, second succeeds
        mock_response_fail = Mock()
        mock_response_fail.status_code = 500
        
        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.content = b'<html><body>Test</body></html>'
        
        mock_get.side_effect = [
            requests.RequestException("Connection error"),
            mock_response_success
        ]
        
        result = get_page_content("http://test.com", max_retries=2)
        
        assert result is not None
        assert mock_get.call_count == 2
        mock_spinner.assert_called()
    
    @patch('utils.extract.requests.get')
    @patch('utils.extract.log_message')
    @patch('utils.extract.show_spinner')
    @patch('builtins.print')
    def test_get_page_content_max_retries_reached(self, mock_print, mock_spinner, mock_log, mock_get):
        """Test max retries reached"""
        mock_get.side_effect = requests.RequestException("Connection error")
        
        result = get_page_content("http://test.com", max_retries=2)
        
        assert result is None
        assert mock_get.call_count == 2
        mock_spinner.assert_called()
    
    @patch('utils.extract.requests.get')
    @patch('utils.extract.log_message')
    @patch('builtins.print')
    def test_get_page_content_bad_status_code(self, mock_print, mock_log, mock_get):
        """Test handling of bad status codes"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        result = get_page_content("http://test.com", max_retries=1)
        
        assert result is None
        mock_log.assert_any_call("Failed to fetch http://test.com. Status code: 404", "WARNING", "⚠️")


class TestExtractProductDetails:
    """Test cases for extract_product_details function"""
    
    def create_mock_product_div(self, title=None, price=None, rating=None, 
                               colors=None, size=None, gender=None):
        """Helper to create mock product div"""
        mock_div = Mock()

        # Mock title - use select_one
        if title:
            mock_title = Mock()
            mock_title.text = f"  {title}  "  # Add spaces to test strip()
            mock_div.select_one.return_value = mock_title
        else:
            mock_div.select_one.return_value = None

        # Mock price - override select_one for '.price'
        def mock_select_one(selector):
            if selector == '.price' and price:
                mock_price = Mock()
                mock_price.text = f"  {price}  "
                return mock_price
            elif selector == '.product-title' and title:
                mock_title = Mock()
                mock_title.text = f"  {title}  "
                return mock_title
            return None

        mock_div.select_one.side_effect = mock_select_one

        # Mock other elements using find with lambda
        find_responses = []

        # Setup responses in specific order for each find call
        if rating:
            rating_mock = Mock()
            rating_mock.text = f"  {rating}  "
            find_responses.append(rating_mock)
        else:
            find_responses.append(None)

        if colors:
            colors_mock = Mock()
            colors_mock.text = f"  {colors}  "
            find_responses.append(colors_mock)
        else:
            find_responses.append(None)

        if size:
            size_mock = Mock()
            size_mock.text = f"  {size}  "
            find_responses.append(size_mock)
        else:
            find_responses.append(None)

        if gender:
            gender_mock = Mock()
            gender_mock.text = f"  {gender}  "
            find_responses.append(gender_mock)
        else:
            find_responses.append(None)

        # Mock find method to return values in order
        mock_div.find.side_effect = find_responses

        return mock_div
    
    def test_extract_product_details_complete(self):
        """Test extracting complete product details"""
        mock_div = self.create_mock_product_div(
            title="Test Product",
            price="$25.99",
            rating="Rating: 4.5 / 5",
            colors="3 Colors",
            size="Size: M",
            gender="Gender: Unisex"
        )
        
        result = extract_product_details(mock_div)
        
        assert result["Title"] == "Test Product"
        assert result["Price"] == "$25.99"
        assert result["Rating"] == "Rating: 4.5 / 5"
        assert result["Colors"] == "3 Colors"
        assert result["Size"] == "Size: M"
        assert result["Gender"] == "Gender: Unisex"
    
    def test_extract_product_details_partial(self):
        """Test extracting partial product details"""
        mock_div = self.create_mock_product_div(
            title="Partial Product",
            price="$15.50"
        )
        
        result = extract_product_details(mock_div)
        
        assert result["Title"] == "Partial Product"
        assert result["Price"] == "$15.50"
        assert result["Rating"] is None
        assert result["Colors"] is None
        assert result["Size"] is None
        assert result["Gender"] is None
    
    def test_extract_product_details_empty(self):
        """Test extracting from empty div"""
        mock_div = self.create_mock_product_div()
        
        result = extract_product_details(mock_div)
        
        assert result["Title"] == "Unknown Product"
        assert result["Price"] is None
        assert result["Rating"] is None
        assert result["Colors"] is None
        assert result["Size"] is None
        assert result["Gender"] is None
    
    @patch('utils.extract.log_message')
    def test_extract_product_details_exception(self, mock_log):
        """Test exception handling in extract_product_details"""
        mock_div = Mock()
        mock_div.select_one.side_effect = Exception("Test error")
        
        result = extract_product_details(mock_div)
        
        # Should return default values
        assert result["Title"] == "Unknown Product"
        mock_log.assert_called_with("Error extracting product details: Test error", "ERROR", "❌")


class TestGetTotalPages:
    """Test cases for get_total_pages function"""
    
    def create_mock_soup_with_pagination(self, page_text):
        """Helper to create mock soup with pagination"""
        mock_soup = Mock()
        mock_pagination = Mock()
        mock_pagination.text = page_text
        mock_soup.select_one.return_value = mock_pagination
        return mock_soup
    
    @patch('utils.extract.log_message')
    def test_get_total_pages_success(self, mock_log):
        """Test successful extraction of total pages"""
        mock_soup = self.create_mock_soup_with_pagination("Page 1 of 25")
        
        result = get_total_pages(mock_soup)
        
        assert result == 25
        mock_log.assert_called_with("Found 25 total pages of products", "SUCCESS", "📚")
    
    @patch('utils.extract.log_message')
    def test_get_total_pages_no_pagination(self, mock_log):
        """Test when no pagination element found"""
        mock_soup = Mock()
        mock_soup.select_one.return_value = None
        
        result = get_total_pages(mock_soup)
        
        assert result == 50  # Default value
        mock_log.assert_called_with("Could not determine total pages, defaulting to 50", "WARNING", "⚠️")
    
    @patch('utils.extract.log_message')
    def test_get_total_pages_invalid_format(self, mock_log):
        """Test when pagination text is in invalid format"""
        mock_soup = self.create_mock_soup_with_pagination("Invalid format")
        
        result = get_total_pages(mock_soup)
        
        assert result == 50  # Default value
        mock_log.assert_called_with("Could not determine total pages, defaulting to 50", "WARNING", "⚠️")
    
    @patch('utils.extract.log_message')
    def test_get_total_pages_exception(self, mock_log):
        """Test exception handling"""
        mock_soup = Mock()
        mock_soup.select_one.side_effect = Exception("Test error")
        
        result = get_total_pages(mock_soup)
        
        assert result == 50  # Default value
        mock_log.assert_any_call("Error getting total pages: Test error", "ERROR", "❌")


class TestExtractProductsFromPage:
    """Test cases for extract_products_from_page function"""
    
    @patch('utils.extract.get_page_content')
    @patch('utils.extract.get_total_pages')
    @patch('utils.extract.extract_product_details')
    @patch('utils.extract.log_message')
    @patch('sys.stdout')
    def test_extract_products_from_page_first_page(self, mock_stdout, mock_log, 
                                                   mock_extract_details, mock_get_total,
                                                   mock_get_content):
        """Test extracting products from first page"""
        # Mock soup
        mock_soup = Mock()
        mock_get_content.return_value = mock_soup
        mock_get_total.return_value = 50
        
        # Mock product divs
        mock_divs = [Mock() for _ in range(5)]
        mock_soup.select.return_value = mock_divs
        
        # Mock extracted details
        mock_extract_details.side_effect = [
            {"Title": f"Product {i}", "Price": f"${i*10}"}
            for i in range(5)
        ]
        
        result = extract_products_from_page("http://test.com")
        
        products, total_pages = result
        assert len(products) == 5
        assert total_pages == 50
        assert mock_get_total.called  # Should get total pages for first page
    
    @patch('utils.extract.get_page_content')
    @patch('utils.extract.get_total_pages')
    @patch('utils.extract.extract_product_details')
    @patch('utils.extract.log_message')
    @patch('sys.stdout')
    def test_extract_products_from_page_subsequent_page(self, mock_stdout, mock_log,
                                                        mock_extract_details, mock_get_total,
                                                        mock_get_content):
        """Test extracting products from subsequent page"""
        # Mock soup
        mock_soup = Mock()
        mock_get_content.return_value = mock_soup
        
        # Mock product divs
        mock_divs = [Mock() for _ in range(3)]
        mock_soup.select.return_value = mock_divs
        
        # Mock extracted details
        mock_extract_details.side_effect = [
            {"Title": f"Product {i}", "Price": f"${i*10}"}
            for i in range(3)
        ]
        
        result = extract_products_from_page("http://test.com/page2")
        
        products, total_pages = result
        assert len(products) == 3
        assert total_pages is None  # Should not get total pages for subsequent pages
        assert not mock_get_total.called
    
    @patch('utils.extract.get_page_content')
    @patch('utils.extract.log_message')
    def test_extract_products_from_page_no_content(self, mock_log, mock_get_content):
        """Test when page content cannot be retrieved"""
        mock_get_content.return_value = None
        
        result = extract_products_from_page("http://test.com")
        
        products, total_pages = result
        assert products == []
        assert total_pages is None
    
    @patch('utils.extract.get_page_content')
    @patch('utils.extract.extract_product_details')
    @patch('utils.extract.log_message')
    @patch('sys.stdout')
    def test_extract_products_from_page_many_products(self, mock_stdout, mock_log,
                                                      mock_extract_details, mock_get_content):
        """Test extracting many products (triggers progress display)"""
        # Mock soup
        mock_soup = Mock()
        mock_get_content.return_value = mock_soup
        
        # Mock 15 product divs (more than 10 to trigger progress)
        mock_divs = [Mock() for _ in range(15)]
        mock_soup.select.return_value = mock_divs
        
        # Mock extracted details
        mock_extract_details.side_effect = [
            {"Title": f"Product {i}", "Price": f"${i*10}"}
            for i in range(15)
        ]
        
        result = extract_products_from_page("http://test.com/page3")
        
        products, total_pages = result
        assert len(products) == 15
        
        # Verify progress was displayed
        assert mock_stdout.write.called


class TestScrapeAllProducts:
    """Test cases for scrape_all_products function"""
    
    @patch('utils.extract.extract_products_from_page')
    @patch('utils.extract.log_message')
    @patch('utils.extract.show_progress_bar')
    @patch('utils.extract.show_spinner')
    @patch('utils.extract.datetime')
    @patch('utils.extract.time')
    @patch('builtins.print')
    @patch('os.system')
    def test_scrape_all_products_success(self, mock_system, mock_print, mock_time_module,
                                         mock_datetime, mock_spinner, mock_progress,
                                         mock_log, mock_extract_page):
        """Test successful scraping of multiple pages"""
        # Mock time using a simple counter closure
        counter = [0]
        def mock_time():
            counter[0] += 1
            return counter[0]
        
        mock_time_module.time.side_effect = mock_time
        
        # Mock datetime for timestamp
        mock_now = Mock()
        mock_now.isoformat.return_value = "2025-01-01T12:00:00"
        mock_now.strftime.return_value = "2025-01-01 12:00:00"  # Add strftime mock
        mock_datetime.now.return_value = mock_now
        
        # Mock extract_products_from_page
        def mock_extract_side_effect(url):
            if 'page' not in url:
                # First page returns total pages
                return ([{"Title": "Product 1", "Price": "$10"}], 3)
            else:
                # Other pages
                return ([{"Title": "Product X", "Price": "$20"}], None)
        
        mock_extract_page.side_effect = mock_extract_side_effect
        
        result = scrape_all_products(max_pages=3)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3  # 3 pages of 1 product each
        assert 'timestamp' in result.columns
        
        # Verify calls
        assert mock_extract_page.call_count == 3
        mock_spinner.assert_called()  # Delay between pages
    
    @patch('utils.extract.extract_products_from_page')
    @patch('utils.extract.log_message')
    @patch('utils.extract.datetime')
    @patch('utils.extract.time')
    @patch('builtins.print')
    @patch('os.system')
    def test_scrape_all_products_exception(self, mock_system, mock_print, mock_time_module,
                                           mock_datetime, mock_log, mock_extract_page):
        """Test scraping with exception"""
        # Mock time.time()
        mock_time_module.time.side_effect = [0, 5]
        
        # Mock datetime
        mock_now = Mock()
        mock_now.isoformat.return_value = "2025-01-01T12:00:00"
        mock_datetime.now.return_value = mock_now
        
        # Mock exception
        mock_extract_page.side_effect = Exception("Network error")
        
        result = scrape_all_products(max_pages=1)
        
        assert isinstance(result, pd.DataFrame)
        # Should still return DataFrame with timestamp even after exception
        assert 'timestamp' in result.columns
        mock_log.assert_any_call("Error during scraping: Network error", "ERROR", "❌")
    
    @patch('utils.extract.extract_products_from_page')
    @patch('utils.extract.log_message')
    @patch('utils.extract.datetime')
    @patch('utils.extract.time')
    @patch('builtins.print')
    @patch('os.system')
    def test_scrape_all_products_no_total_pages(self, mock_system, mock_print,
                                                mock_time_module, mock_datetime,
                                                mock_log, mock_extract_page):
        """Test scraping when total pages is not found"""
        # Mock time using a simple counter closure
        counter = [0]
        def mock_time():
            counter[0] += 1
            return counter[0]
        
        mock_time_module.time.side_effect = mock_time
        
        # Mock datetime
        mock_now = Mock()
        mock_now.isoformat.return_value = "2025-01-01T12:00:00"
        mock_now.strftime.return_value = "2025-01-01 12:00:00"  # Add strftime mock
        mock_datetime.now.return_value = mock_now
        
        # Mock first page returning no total pages
        mock_extract_page.return_value = ([{"Title": "Product 1"}], None)
        
        result = scrape_all_products(max_pages=2)
        
        assert isinstance(result, pd.DataFrame)
        # Should use max_pages when total_pages is None
        assert mock_extract_page.call_count == 2


class TestMain:
    """Test cases for main function"""
    
    @patch('utils.extract.scrape_all_products')
    @patch('utils.extract.log_message')
    @patch('builtins.print')
    def test_main_success(self, mock_print, mock_log, mock_scrape):
        """Test successful main execution"""
        # Mock DataFrame result
        mock_df = pd.DataFrame([{"Title": "Test Product", "Price": "$10"}])
        mock_scrape.return_value = mock_df
        
        result = main()
        
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        mock_scrape.assert_called_once()
    
    @patch('utils.extract.scrape_all_products')
    @patch('utils.extract.log_message')
    @patch('builtins.print')
    def test_main_exception(self, mock_print, mock_log, mock_scrape):
        """Test main with exception"""
        # Mock exception
        mock_scrape.side_effect = Exception("Critical error")
        
        result = main()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0  # Empty DataFrame
        mock_log.assert_any_call("Critical error in main extraction process: Critical error", "ERROR", "💥")


# Fixtures for common test data
@pytest.fixture
def sample_html():
    """Sample HTML for testing"""
    return """
    <html>
        <body>
            <div class="product-details">
                <h3 class="product-title">Test Product</h3>
                <span class="price">$25.99</span>
                <p>Rating: 4.5 / 5</p>
                <p>3 Colors</p>
                <p>Size: M</p>
                <p>Gender: Unisex</p>
            </div>
        </body>
    </html>
    """


@pytest.fixture
def sample_pagination_html():
    """Sample pagination HTML for testing"""
    return """
    <html>
        <body>
            <div class="page-item current">
                <span class="page-link">Page 5 of 25</span>
            </div>
        </body>
    </html>
    """


# Integration test
class TestExtractIntegration:
    """Integration tests for extract module"""
    
    @patch('utils.extract.requests.get')
    @patch('utils.extract.log_message')
    @patch('builtins.print')
    @patch('os.system')
    def test_full_extraction_flow(self, mock_system, mock_print, mock_log, mock_get):
        """Test complete extraction flow with mocked HTTP"""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'''
        <html>
            <body>
                <div class="page-item current">
                    <span class="page-link">Page 1 of 2</span>
                </div>
                <div class="product-details">
                    <h3 class="product-title">Product 1</h3>
                    <span class="price">$10.99</span>
                    <p>Rating: 4.0 / 5</p>
                    <p>2 Colors</p>
                    <p>Size: L</p>
                    <p>Gender: Men</p>
                </div>
            </body>
        </html>
        '''
        mock_get.return_value = mock_response
        
        # Run scraping with limited pages
        with patch('utils.extract.time.time', return_value=0):
            with patch('utils.extract.time.sleep'):
                with patch('utils.extract.random.uniform', return_value=0.1):
                    result = scrape_all_products(max_pages=1)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'Title' in result.columns
        assert 'Price' in result.columns
        assert 'timestamp' in result.columns
        
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=utils.extract", "--cov-report=html"])