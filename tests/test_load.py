"""
Unit tests for Fashion Studio ETL Pipeline - Load Module

This module contains comprehensive tests for the load.py module with high coverage.
Tests cover all functions including error handling and edge cases.

Dependencies:
- pytest: Testing framework
- pytest-mock: For mocking
- pytest-cov: For coverage reporting
- pandas: For DataFrame operations
"""

import gspread
import psycopg2
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock, call, mock_open
import sys
from contextlib import contextmanager

# Import modules to test
sys.path.insert(0, '.')
from utils.load import (
    log_message,
    show_spinner,
    show_progress_bar,
    load_to_csv,
    load_to_google_sheets,
    load_to_postgresql,
    main
)


class TestLogMessage:
    """Test cases for log_message function"""
    
    @patch('utils.load.datetime')
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
    
    @patch('utils.load.datetime')
    @patch('builtins.print')
    def test_log_message_success(self, mock_print, mock_datetime):
        """Test log_message with SUCCESS level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Success message", "SUCCESS", "✅")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[SUCCESS]" in printed_text
    
    @patch('utils.load.datetime')
    @patch('builtins.print')
    def test_log_message_warning(self, mock_print, mock_datetime):
        """Test log_message with WARNING level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Warning message", "WARNING", "⚠️")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[WARNING]" in printed_text
    
    @patch('utils.load.datetime')
    @patch('builtins.print')
    def test_log_message_error(self, mock_print, mock_datetime):
        """Test log_message with ERROR level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Error message", "ERROR", "❌")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[ERROR]" in printed_text
    
    @patch('utils.load.datetime')
    @patch('builtins.print')
    def test_log_message_processing(self, mock_print, mock_datetime):
        """Test log_message with PROCESSING level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Processing message", "PROCESSING", "🔄")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[PROCESSING]" in printed_text
    
    @patch('utils.load.datetime')
    @patch('builtins.print')
    def test_log_message_custom_level(self, mock_print, mock_datetime):
        """Test log_message with custom level"""
        mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"
        
        log_message("Custom message", "CUSTOM", "🎯")
        
        mock_print.assert_called_once()
        printed_text = mock_print.call_args[0][0]
        assert "[CUSTOM]" in printed_text


class TestShowSpinner:
    """Test cases for show_spinner function"""
    
    @patch('utils.load.time.sleep')
    @patch('builtins.print')
    def test_show_spinner(self, mock_print, mock_sleep):
        """Test show_spinner functionality"""
        show_spinner(0.5, "Loading")
        
        assert mock_print.call_count > 0
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
        assert "█" in result
    
    def test_show_progress_bar_half(self):
        """Test progress bar at 50%"""
        result = show_progress_bar(50, 100, "Progress:", "items")
        
        assert "50/100" in result
        assert "(50.0%)" in result
        assert "█" in result
        assert "░" in result
    
    def test_show_progress_bar_zero(self):
        """Test progress bar at 0%"""
        result = show_progress_bar(0, 100, "Progress:", "items")
        
        assert "0/100" in result
        assert "(0.0%)" in result
        assert "░" in result
    
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


class TestLoadToCsv:
    """Test cases for load_to_csv function"""
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('os.path.getsize')
    @patch('os.makedirs')
    @patch('pandas.DataFrame.to_csv')
    def test_load_to_csv_success(self, mock_to_csv, mock_makedirs, mock_getsize, mock_exists, mock_log):
        """Test successful CSV saving"""
        # Setup test data
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock file operations
        mock_exists.return_value = True
        mock_getsize.return_value = 100
        
        result = load_to_csv(df, "test.csv")
        
        assert result is True
        mock_to_csv.assert_called_once_with("test.csv", index=False)
        mock_log.assert_any_call("Saving data to CSV file: 'test.csv'", "PROCESSING", "💾")
        mock_log.assert_any_call("Successfully saved 2 records to 'test.csv'", "SUCCESS", "✅")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('os.makedirs')
    @patch('os.path.dirname')
    @patch('pandas.DataFrame.to_csv')
    def test_load_to_csv_create_directory(self, mock_to_csv, mock_dirname, mock_makedirs, mock_exists, mock_log):
        """Test CSV saving with directory creation"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock directory operations
        mock_dirname.return_value = "test_dir"
        mock_exists.side_effect = [False, True]  # Directory doesn't exist, file exists after creation
        
        with patch('os.path.getsize', return_value=100):
            result = load_to_csv(df, "test_dir/test.csv")
        
        assert result is True
        mock_makedirs.assert_called_once_with("test_dir")
        mock_log.assert_any_call("Created directory: 'test_dir'", "INFO", "📁")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('os.path.getsize')
    @patch('pandas.DataFrame.to_csv')
    def test_load_to_csv_empty_file_warning(self, mock_to_csv, mock_getsize, mock_exists, mock_log):
        """Test CSV saving with empty file warning"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock file operations
        mock_exists.return_value = True
        mock_getsize.return_value = 0  # Empty file
        
        result = load_to_csv(df, "test.csv")
        
        assert result is False
        mock_log.assert_any_call("File was created but may be empty: 'test.csv'", "WARNING", "⚠️")
    
    @patch('utils.load.log_message')
    @patch('pandas.DataFrame.to_csv')
    def test_load_to_csv_exception(self, mock_to_csv, mock_log):
        """Test CSV saving with exception"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock exception during save
        mock_to_csv.side_effect = Exception("Write error")
        
        result = load_to_csv(df, "test.csv")
        
        assert result is False
        mock_log.assert_any_call("Error saving to CSV: Write error", "ERROR", "❌")


class TestLoadToGoogleSheets:
    """Test cases for load_to_google_sheets function"""

    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    @patch('utils.load.gspread.authorize')
    @patch('utils.load.time.sleep')
    @patch('builtins.print')
    def test_load_to_google_sheets_invalid_sheet_id(self, mock_print, mock_sleep, mock_authorize, mock_credentials, mock_exists, mock_log):
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_exists.return_value = True
        mock_client = Mock()
        mock_authorize.return_value = mock_client
        mock_client.open_by_key.side_effect = Exception("Invalid sheet ID")
        
        result = load_to_google_sheets(df, sheet_id="invalid_id")
        
        assert result is False
        mock_log.assert_any_call("Failed to open sheet with ID invalid_id: Invalid sheet ID", "ERROR", "❌")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    def test_load_to_google_sheets_no_credentials(self, mock_exists, mock_log):
        """Test Google Sheets loading without credentials file"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        mock_exists.return_value = False
        
        result = load_to_google_sheets(df)
        
        assert result is False
        mock_log.assert_any_call("Google Sheets API credentials file not found: 'google-sheets-api.json'", "ERROR", "❌")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    @patch('utils.load.gspread.authorize')
    def test_load_to_google_sheets_auth_failure(self, mock_authorize, mock_credentials, mock_exists, mock_log):
        """Test Google Sheets authentication failure"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        mock_exists.return_value = True
        mock_credentials.side_effect = Exception("Auth error")
        
        result = load_to_google_sheets(df)
        
        assert result is False
        mock_log.assert_any_call("Authentication with Google Sheets API failed: Auth error", "ERROR", "❌")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    @patch('utils.load.gspread.authorize')
    @patch('utils.load.time.sleep')
    @patch('builtins.print')
    def test_load_to_google_sheets_success_by_id(self, mock_print, mock_sleep, mock_authorize, mock_credentials, mock_exists, mock_log):
        """Test successful Google Sheets loading by sheet ID"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock file and auth
        mock_exists.return_value = True
        mock_client = Mock()
        mock_authorize.return_value = mock_client
        
        # Mock sheet operations
        mock_sheet = Mock()
        mock_sheet.id = "test_sheet_id"
        mock_worksheet = Mock()
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_client.open_by_key.return_value = mock_sheet
        
        result = load_to_google_sheets(df, sheet_id="test_sheet_id")
        
        assert result is True
        mock_client.open_by_key.assert_called_once_with("test_sheet_id")
        mock_worksheet.clear.assert_called_once()
        mock_worksheet.update.assert_called()
        mock_log.assert_any_call("Opened Google Sheet by ID: test_sheet_id", "SUCCESS", "🎯")
        mock_log.assert_any_call("Successfully uploaded data to Google Sheets", "SUCCESS", "🎉")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    @patch('utils.load.gspread.authorize')
    @patch('utils.load.time.sleep')
    @patch('builtins.print')
    def test_load_to_google_sheets_success_by_name(self, mock_print, mock_sleep, mock_authorize, mock_credentials, mock_exists, mock_log):
        """Test successful Google Sheets loading by sheet name"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock file and auth
        mock_exists.return_value = True
        mock_client = Mock()
        mock_authorize.return_value = mock_client
        
        # Mock sheet operations
        mock_sheet = Mock()
        mock_sheet.id = "auto_generated_id"
        mock_worksheet = Mock()
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_client.open.return_value = mock_sheet
        
        result = load_to_google_sheets(df, sheet_name="Test Sheet")
        
        assert result is True
        mock_client.open.assert_called_once_with("Test Sheet")
        mock_worksheet.clear.assert_called_once()
        mock_worksheet.update.assert_called()
        mock_log.assert_any_call("Found existing Google Sheet: 'Test Sheet'", "INFO", "📝")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    @patch('utils.load.gspread.authorize')
    @patch('utils.load.time.sleep')
    @patch('builtins.print')
    def test_load_to_google_sheets_create_new_sheet(self, mock_print, mock_sleep, mock_authorize, mock_credentials, mock_exists, mock_log):
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_exists.return_value = True
        mock_client = Mock()
        mock_authorize.return_value = mock_client
        mock_not_found = gspread.exceptions.SpreadsheetNotFound
        mock_client.open.side_effect = mock_not_found
        mock_sheet = Mock()
        mock_sheet.id = "new_sheet_id"
        mock_worksheet = Mock()
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_client.create.return_value = mock_sheet
        
        result = load_to_google_sheets(df, sheet_name="New Sheet")
        
        assert result is True
        mock_client.create.assert_called_once_with("New Sheet")
        mock_sheet.share.assert_called_once_with('anyone', perm_type='anyone', role='writer')
        mock_log.assert_any_call("Created new Google Sheet: 'New Sheet'", "SUCCESS", "✅")
        mock_log.assert_any_call("Sheet ID: new_sheet_id", "INFO", "🆔")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    @patch('utils.load.gspread.authorize')
    @patch('utils.load.time.sleep')
    @patch('builtins.print')
    def test_load_to_google_sheets_create_new_worksheet(self, mock_print, mock_sleep, mock_authorize, mock_credentials, mock_exists, mock_log):
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_exists.return_value = True
        mock_client = Mock()
        mock_authorize.return_value = mock_client
        mock_sheet = Mock()
        mock_sheet.id = "existing_sheet_id"
        mock_worksheet_not_found = gspread.exceptions.WorksheetNotFound
        mock_sheet.worksheet.side_effect = mock_worksheet_not_found
        mock_client.open.return_value = mock_sheet
        mock_new_worksheet = Mock()
        mock_sheet.add_worksheet.return_value = mock_new_worksheet
        
        result = load_to_google_sheets(df, worksheet_name="New Worksheet")
        
        assert result is True
        mock_sheet.add_worksheet.assert_called_once_with(title="New Worksheet", rows=3, cols=2)
        mock_log.assert_any_call("Created new worksheet: 'New Worksheet'", "SUCCESS", "✅")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    @patch('utils.load.gspread.authorize')
    @patch('utils.load.time.sleep')
    @patch('builtins.print')
    def test_load_to_google_sheets_large_data_batches(self, mock_print, mock_sleep, mock_authorize, mock_credentials, mock_exists, mock_log):
        """Test Google Sheets loading with large data requiring batches"""
        # Create large DataFrame to trigger batch processing
        df = pd.DataFrame({'A': range(2500), 'B': range(2500, 5000)})
        
        # Mock file and auth
        mock_exists.return_value = True
        mock_client = Mock()
        mock_authorize.return_value = mock_client
        
        # Mock sheet operations
        mock_sheet = Mock()
        mock_sheet.id = "test_sheet_id"
        mock_worksheet = Mock()
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_client.open.return_value = mock_sheet
        
        result = load_to_google_sheets(df)
        
        assert result is True
        # Should be called multiple times due to batching
        assert mock_worksheet.update.call_count > 1
        mock_log.assert_any_call("Updating Google Sheet with 2500 records in 3 batches", "PROCESSING", "🔄")
    
    @patch('utils.load.log_message')
    @patch('os.path.exists')
    @patch('utils.load.ServiceAccountCredentials.from_json_keyfile_name')
    @patch('utils.load.gspread.authorize')
    @patch('utils.load.time.sleep')
    @patch('builtins.print')
    def test_load_to_google_sheets_formatting_error(self, mock_print, mock_sleep, mock_authorize, mock_credentials, mock_exists, mock_log):
        """Test Google Sheets loading with formatting error"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock file and auth
        mock_exists.return_value = True
        mock_client = Mock()
        mock_authorize.return_value = mock_client
        
        # Mock sheet operations
        mock_sheet = Mock()
        mock_worksheet = Mock()
        mock_worksheet.format.side_effect = Exception("Format error")
        mock_worksheet.freeze.side_effect = Exception("Freeze error")
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_client.open.return_value = mock_sheet
        
        result = load_to_google_sheets(df)
        
        assert result is True  # Should still succeed despite formatting errors
        mock_log.assert_any_call("Warning: Could not format header: Format error", "WARNING", "⚠️")
    
    @patch('utils.load.log_message')
    def test_load_to_google_sheets_general_exception(self, mock_log):
        """Test Google Sheets loading with general exception"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock general exception during process
        with patch('os.path.exists', side_effect=Exception("Unexpected error")):
            result = load_to_google_sheets(df)
        
        assert result is False
        mock_log.assert_any_call("Error saving to Google Sheets: Unexpected error", "ERROR", "❌")


class TestLoadToPostgreSQL:
    """Test cases for load_to_postgresql function"""
    
    @patch('utils.load.log_message')
    @patch('utils.load.psycopg2.connect')
    @patch('utils.load.create_engine')
    def test_load_to_postgresql_success(self, mock_create_engine, mock_connect, mock_log):
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        @contextmanager
        def mock_context_manager():
            yield mock_connection
        
        with patch.object(df, 'to_sql'):
            mock_connection = Mock()
            mock_result = Mock()
            mock_result.fetchone.return_value = [2]
            mock_connection.execute.return_value = mock_result
            mock_engine.connect.return_value = mock_context_manager()
            
            result = load_to_postgresql(df)
        
        assert result is True
        mock_log.assert_any_call("Successfully saved data to PostgreSQL table 'fashion_products'", "SUCCESS", "🎉")
        mock_log.assert_any_call("Data verification successful. 2 records in database.", "SUCCESS", "✓")
    
    @patch('utils.load.log_message')
    @patch('utils.load.psycopg2.connect')
    def test_load_to_postgresql_connection_timeout(self, mock_connect, mock_log):
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_connect.side_effect = psycopg2.OperationalError("timeout")
        result = load_to_postgresql(df)
        assert result is False
        mock_log.assert_any_call("Database connection timeout. Please check if PostgreSQL server is running at localhost:5432", "ERROR", "⏱️")
    
    @patch('utils.load.log_message')
    @patch('utils.load.psycopg2.connect')
    def test_load_to_postgresql_create_database(self, mock_connect, mock_log):
        """Test PostgreSQL database creation"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock database operations - database doesn't exist
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None  # Database doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock engine creation failure (database still doesn't exist)
        with patch('utils.load.create_engine', side_effect=Exception("Database error")):
            result = load_to_postgresql(df)
        
        assert result is False
        mock_log.assert_any_call("Creating database 'fashion_data'", "PROCESSING", "🗄️")
        mock_log.assert_any_call("Error creating database engine: Database error", "ERROR", "❌")
    
    @patch('utils.load.log_message')
    @patch('utils.load.psycopg2.connect')
    @patch('utils.load.create_engine')
    def test_load_to_postgresql_insert_error(self, mock_create_engine, mock_connect, mock_log):
        """Test PostgreSQL insert error"""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        # Mock database operations
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [1]  # Database exists
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock SQLAlchemy engine
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        # Mock to_sql error
        with patch.object(df, 'to_sql', side_effect=Exception("Insert error")):
            result = load_to_postgresql(df)
        
        assert result is False
        mock_log.assert_any_call("Error inserting data into PostgreSQL: Insert error", "ERROR", "❌")
    
    @patch('utils.load.log_message')
    @patch('utils.load.psycopg2.connect')
    def test_load_to_postgresql_connection_error(self, mock_connect, mock_log):
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_connect.side_effect = psycopg2.OperationalError("Connection refused")
        result = load_to_postgresql(df)
        assert result is False
        mock_log.assert_any_call("Database connection error: Connection refused", "ERROR", "❌")
    
    @patch('utils.load.log_message')
    @patch('utils.load.psycopg2.connect')
    @patch('utils.load.create_engine')
    def test_load_to_postgresql_verification_error(self, mock_create_engine, mock_connect, mock_log):
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        @contextmanager
        def mock_context_manager():
            yield mock_connection
        
        with patch.object(df, 'to_sql'):
            mock_connection = Mock()
            mock_connection.execute.side_effect = Exception("Verification error")
            mock_engine.connect.return_value = mock_context_manager()
            result = load_to_postgresql(df)
        
        assert result is True
        mock_log.assert_any_call("Could not verify data: Verification error", "WARNING", "⚠️")
    
    @patch('utils.load.log_message')
    @patch('utils.load.psycopg2.connect')
    @patch('utils.load.create_engine')
    def test_load_to_postgresql_count_mismatch(self, mock_create_engine, mock_connect, mock_log):
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        @contextmanager
        def mock_context_manager():
            yield mock_connection
        
        with patch.object(df, 'to_sql'):
            mock_connection = Mock()
            mock_result = Mock()
            mock_result.fetchone.return_value = [1]
            mock_connection.execute.return_value = mock_result
            mock_engine.connect.return_value = mock_context_manager()
            result = load_to_postgresql(df)
        
        assert result is True
        mock_log.assert_any_call("Data count mismatch. Expected 2, found 1.", "WARNING", "⚠️")

    @patch('utils.load.log_message')
    @patch('utils.load.psycopg2.connect')
    def test_load_to_postgresql_database_creation_error(self, mock_connect, mock_log):
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database creation error")
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        result = load_to_postgresql(df)

        assert result is False
        mock_log.assert_any_call("Could not create database: Database creation error", "ERROR", "❌")


class TestMain:
    """Test cases for main function"""
    
    def create_sample_dataframe(self):
        """Helper to create sample DataFrame"""
        return pd.DataFrame({
            'Title': ['Product A', 'Product B'],
            'Price': [100, 200],
            'Rating': [4.5, 3.8],
            'Colors': [3, 2],
            'Size': ['M', 'L'],
            'Gender': ['Male', 'Female']
        })
    
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('utils.load.load_to_csv')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_with_dataframe_csv_only(self, mock_system, mock_print, mock_load_csv, mock_log, mock_datetime, mock_time):
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        df = self.create_sample_dataframe()
        mock_load_csv.return_value = True
        result = main(df=df, load_to_csv_flag=True)
        assert result is True
        mock_load_csv.assert_called_once()
        mock_log.assert_any_call("LOADING COMPLETE: All repositories successfully updated!", "SUCCESS", "✓")
    
    @patch('utils.load.pd.read_csv')
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('utils.load.load_to_csv')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_with_input_file(self, mock_system, mock_print, mock_load_csv, mock_log, mock_datetime, mock_time, mock_read_csv):
        """Test main function with input file"""
        # Mock time and datetime
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        
        df = self.create_sample_dataframe()
        mock_read_csv.return_value = df
        mock_load_csv.return_value = True
        
        result = main(input_file="test.csv", load_to_csv_flag=True)
        
        assert result is True
        mock_read_csv.assert_called_once_with("test.csv")
        mock_load_csv.assert_called_once()
    
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_no_data_no_file(self, mock_system, mock_print, mock_log, mock_datetime, mock_time):
        """Test main function with no data and no file"""
        # Mock time and datetime
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        
        result = main()
        
        assert result is False
        mock_log.assert_any_call("No data provided. Either DataFrame or input_file must be specified.", "ERROR", "❌")
    
    @patch('utils.load.pd.read_csv')
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_file_load_error(self, mock_system, mock_print, mock_log, mock_datetime, mock_time, mock_read_csv):
        """Test main function with file loading error"""
        # Mock time and datetime
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        
        mock_read_csv.side_effect = Exception("File not found")
        
        result = main(input_file="nonexistent.csv")
        
        assert result is False
        mock_log.assert_any_call("Error loading file 'nonexistent.csv': File not found", "ERROR", "❌")
    
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_google_sheets')
    @patch('utils.load.load_to_postgresql')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_all_repositories(self, mock_system, mock_print, mock_load_postgres, mock_load_sheets, mock_load_csv, mock_log, mock_datetime, mock_time):
        """Test main function with all repositories"""
        # Mock time and datetime
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        
        df = self.create_sample_dataframe()
        mock_load_csv.return_value = True
        mock_load_sheets.return_value = True
        mock_load_postgres.return_value = True
        
        result = main(df=df, load_to_csv_flag=True, load_to_sheets_flag=True, load_to_postgres_flag=True)
        
        assert result is True
        mock_load_csv.assert_called_once()
        mock_load_sheets.assert_called_once()
        mock_load_postgres.assert_called_once()
    
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_no_tasks_specified(self, mock_system, mock_print, mock_log, mock_datetime, mock_time):
        """Test main function with no loading tasks specified"""
        # Mock time and datetime
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        
        df = self.create_sample_dataframe()
        
        result = main(df=df, load_to_csv_flag=False, load_to_sheets_flag=False, load_to_postgres_flag=False)
        
        assert result is False
        mock_log.assert_any_call("No loading tasks specified. Please enable at least one repository.", "ERROR", "❌")
    
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_google_sheets')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_partial_success(self, mock_system, mock_print, mock_load_sheets, mock_load_csv, mock_log, mock_datetime, mock_time):
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        df = self.create_sample_dataframe()
        mock_load_csv.return_value = True
        mock_load_sheets.return_value = False
        result = main(df=df, load_to_csv_flag=True, load_to_sheets_flag=True)
        assert result is False
        mock_log.assert_any_call("LOADING PARTIAL: 1/2 repositories updated.", "WARNING", "⚠️")
    
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_dry_run_mode(self, mock_system, mock_print, mock_log, mock_datetime, mock_time):
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        df = self.create_sample_dataframe()
        result = main(df=df, load_to_csv_flag=True, load_to_sheets_flag=True, load_to_postgres_flag=True, dry_run=True)
        assert result is True
        mock_log.assert_any_call("DRY RUN MODE: Data will not be saved to any repository", "INFO", "🔍")
        mock_log.assert_any_call("Data validation successful. Would save to repositories:", "SUCCESS", "✓")
        mock_log.assert_any_call("  - Google Sheets: Using credentials from google-sheets-api.json", "INFO", "📊")
        mock_log.assert_any_call("  - PostgreSQL: fashion_data @ localhost", "INFO", "🐘")
    
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_validation_missing_columns(self, mock_system, mock_print, mock_log, mock_datetime, mock_time):
        """Test main function with missing required columns"""
        # Mock time and datetime
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        
        # DataFrame missing required columns
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        
        result = main(df=df, load_to_csv_flag=True)
        
        assert result is False
        mock_log.assert_any_call("Data is missing required columns: Title, Price, Rating, Colors, Size, Gender", "ERROR", "❌")
    
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_data_with_nulls(self, mock_system, mock_print, mock_log, mock_datetime, mock_time):
        """Test main function with null values in data"""
        # Mock time and datetime
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        
        # DataFrame with null values
        df = pd.DataFrame({
            'Title': ['Product A', None],
            'Price': [100, 200],
            'Rating': [4.5, None],
            'Colors': [3, 2],
            'Size': ['M', 'L'],
            'Gender': ['Male', 'Female']
        })
        
        with patch('utils.load.load_to_csv', return_value=True):
            result = main(df=df, load_to_csv_flag=True)
        
        assert result is True
        mock_log.assert_any_call("Data contains null values in required columns:", "WARNING", "⚠️")
        mock_log.assert_any_call("  - Title: 1 null values", "WARNING", "⚠️")
        mock_log.assert_any_call("  - Rating: 1 null values", "WARNING", "⚠️")
    
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('utils.load.log_message')
    @patch('utils.load.load_to_csv')
    @patch('builtins.print')
    @patch('os.system')
    def test_main_critical_exception(self, mock_system, mock_print, mock_load_csv, mock_log, mock_datetime, mock_time):
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        df = self.create_sample_dataframe()

        mock_load_csv.side_effect = Exception("Critical error")

        result = main(df=df, load_to_csv_flag=True)
        assert result is False
        mock_log.assert_any_call("Critical error in loading process: Critical error", "ERROR", "💥")


class TestCommandLineInterface:
    """Test cases for command line interface"""
    
    @patch('utils.load.main')
    @patch('sys.argv', ['load.py', '--input', 'test.csv', '--repositories', 'all'])
    def test_main_cli_execution(self, mock_main):
        from utils.load import parse_args, main

        # Simulate CLI argument parsing
        args = parse_args()

        # Call main with parsed arguments
        main(
            input_file=args.input,
            load_to_csv_flag='csv' in args.repositories or args.repositories == 'all',
            load_to_sheets_flag='sheets' in args.repositories or args.repositories == 'all',
            load_to_postgres_flag='postgres' in args.repositories or args.repositories == 'all',
            csv_output=args.csv_output,
            google_sheets_credentials=args.google_creds,
            sheet_id=args.google_sheet_id,
            sheet_name=args.sheet_name,
            worksheet_name=args.worksheet_name,
            db_params={
                'host': args.db_host,
                'port': args.db_port,
                'dbname': args.db_name,
                'user': args.db_user,
                'password': args.db_pass
            },
            dry_run=args.dry_run
        )

        mock_main.assert_called_once()


# Fixtures for common test data
@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing"""
    return pd.DataFrame({
        'Title': ['Product A', 'Product B'],
        'Price': [100, 200],
        'Rating': [4.5, 3.8],
        'Colors': [3, 2],
        'Size': ['M', 'L'],
        'Gender': ['Male', 'Female']
    })


# Integration tests
class TestLoadIntegration:
    """Integration tests for load module"""
    
    @patch('utils.load.log_message')
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_google_sheets')
    @patch('utils.load.load_to_postgresql')
    @patch('utils.load.time')
    @patch('utils.load.datetime')
    @patch('builtins.print')
    @patch('os.system')
    def test_full_loading_pipeline(self, mock_system, mock_print, mock_datetime, mock_time, 
                                  mock_load_postgres, mock_load_sheets, mock_load_csv, mock_log):
        """Test complete loading pipeline with all repositories"""
        # Mock time and datetime
        mock_time.time.side_effect = [0, 5]
        mock_now = Mock()
        mock_now.strftime.return_value = "2025-01-01 12:00:00"
        mock_datetime.now.return_value = mock_now
        
        df = pd.DataFrame({
            'Title': ['Product A', 'Product B'],
            'Price': [100, 200],
            'Rating': [4.5, 3.8],
            'Colors': [3, 2],
            'Size': ['M', 'L'],
            'Gender': ['Male', 'Female']
        })
        
        # Mock all loaders to succeed
        mock_load_csv.return_value = True
        mock_load_sheets.return_value = True
        mock_load_postgres.return_value = True
        
        result = main(
            df=df,
            load_to_csv_flag=True,
            load_to_sheets_flag=True,
            load_to_postgres_flag=True
        )
        
        assert result is True
        mock_load_csv.assert_called_once()
        mock_load_sheets.assert_called_once()
        mock_load_postgres.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=utils.load", "--cov-report=html"])