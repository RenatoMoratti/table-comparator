"""
Configuration file for Database Table Comparator
Modify these values according to your environment
"""

import json
import os
import re
from typing import List, Tuple

# DEV Environment Defaults
DEV_DEFAULTS = {
    # Do NOT hardcode environment details in git. Configure via environment variables.
    'host': os.getenv('DATABRICKS_DEV_HOST', ''),
    'port': os.getenv('DATABRICKS_DEV_WAREHOUSE_ID', ''),
    'database': os.getenv('DATABRICKS_DEV_DATABASE', ''),
    'token': os.getenv('DATABRICKS_DEV_TOKEN', ''),
}

# PROD Environment Defaults
PROD_DEFAULTS = {
    # Do NOT hardcode environment details in git. Configure via environment variables.
    'host': os.getenv('DATABRICKS_PROD_HOST', ''),
    'port': os.getenv('DATABRICKS_PROD_WAREHOUSE_ID', ''),
    'database': os.getenv('DATABRICKS_PROD_DATABASE', ''),
    'token': os.getenv('DATABRICKS_PROD_TOKEN', ''),
}

# Default Available Tables (fallback if no custom tables file exists)
DEFAULT_AVAILABLE_TABLES = [
    ('schema.table1', 'Dimension Example 1', 'PK_Table1', 'PK_Table1', '__processing_timestamp_utc | __year | __month | __day'),
    ('schema.table2', 'Dimension Example 2', 'PK_Table2', 'PK_Table2', '__processing_timestamp_utc | __year | __month | __day'),
]

# File path for storing custom tables
CUSTOM_TABLES_FILE = 'data/custom_tables.json'

def ensure_data_directory():
    """Ensure the data directory exists."""
    os.makedirs(os.path.dirname(CUSTOM_TABLES_FILE), exist_ok=True)

def load_available_tables() -> List[Tuple[str, str, str, str, str]]:
    """Load available tables from custom file or return defaults."""
    ensure_data_directory()
    
    if os.path.exists(CUSTOM_TABLES_FILE):
        try:
            with open(CUSTOM_TABLES_FILE, 'r', encoding='utf-8') as f:
                custom_tables = json.load(f)
                # Convert list of dicts back to tuples - ATUALIZAR PARA 5 ELEMENTOS
                return [(table['table_name'], table['display_name'], 
                        table.get('prod_primary_keys', table.get('primary_keys', '')), 
                        table.get('dev_primary_keys', table.get('primary_keys', '')),
                        re.sub(r"\s*\|\s*", " | ", table['ignored_columns'].replace('\n',' | '))) 
                       for table in custom_tables]
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            # If file is corrupted or has wrong format, fall back to defaults
            save_available_tables(DEFAULT_AVAILABLE_TABLES)
            return DEFAULT_AVAILABLE_TABLES
    else:
        # Create default file
        save_available_tables(DEFAULT_AVAILABLE_TABLES)
        return DEFAULT_AVAILABLE_TABLES

def save_available_tables(tables: List[Tuple[str, str, str, str, str]]):
    """Save available tables to custom file."""
    ensure_data_directory()
    
    # Convert tuples to list of dicts for JSON serialization - ATUALIZAR PARA 5 ELEMENTOS
    def _normalize_pipe(s: str) -> str:
        return re.sub(r"\s*\|\s*", " | ", (s or '').replace('\n', ' | '))

    tables_data = [
        {
            'table_name': table[0],
            'display_name': table[1],
            'prod_primary_keys': table[2],
            'dev_primary_keys': table[3],
            'ignored_columns': _normalize_pipe(table[4])
        }
        for table in tables
    ]
    
    try:
        with open(CUSTOM_TABLES_FILE, 'w', encoding='utf-8') as f:
            json.dump(tables_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving tables: {e}")

def add_table(table_name: str, display_name: str, prod_primary_keys: str, dev_primary_keys: str, ignored_columns: str) -> bool:
    """Add a new table to the available tables list."""
    tables = load_available_tables()
    
    # Check if table already exists
    if any(table[0] == table_name for table in tables):
        return False
    
    # Normalize ignored columns to spaced pipe format
    ignored_norm = re.sub(r"\s*\|\s*", " | ", (ignored_columns or '').replace('\n', ' | '))
    tables.append((table_name, display_name, prod_primary_keys, dev_primary_keys, ignored_norm))
    save_available_tables(tables)
    return True

def update_table(old_table_name: str, table_name: str, display_name: str, 
                prod_primary_keys: str, dev_primary_keys: str, ignored_columns: str) -> bool:
    """Update an existing table in the available tables list."""
    tables = load_available_tables()
    
    for i, table in enumerate(tables):
        if table[0] == old_table_name:
            ignored_norm = re.sub(r"\s*\|\s*", " | ", (ignored_columns or '').replace('\n', ' | '))
            tables[i] = (table_name, display_name, prod_primary_keys, dev_primary_keys, ignored_norm)
            save_available_tables(tables)
            return True
    
    return False

def remove_table(table_name: str) -> bool:
    """Remove a table from the available tables list."""
    tables = load_available_tables()
    original_length = len(tables)
    
    tables = [table for table in tables if table[0] != table_name]
    
    if len(tables) < original_length:
        save_available_tables(tables)
        return True
    
    return False

# Load available tables at module import
AVAILABLE_TABLES = load_available_tables()

# Comparison Settings Defaults
COMPARISON_DEFAULTS = {
    'prod_primary_keys': 'PK_Account',
    'dev_primary_keys': 'PK_Account',
    'float_tolerance': 1e-9,
    'ignored_columns': '__processing_timestamp_utc | __year | __month | __day'
}

# Application Settings
APP_CONFIG = {
    'debug': True,
    'host': '0.0.0.0',
    'port': 5000
}

# Sampling Configuration for Large Tables
SAMPLING_CONFIG = {
    'max_rows_for_comparison': 20000,  # Default maximum rows to compare
    'enable_row_limit': True,  # Enable row limiting
    'sampling_method': 'LAST_N',  # Options: 'TOP_N', 'LAST_N', 'RANDOM'
    'order_direction': 'DESC',  # DESC for last N rows, ASC for first N rows
    'allow_user_override': True  # ADICIONAR ESTA LINHA - Allow user to override max rows
}

# Batch Comparison Settings
BATCH_CONFIG = {
    'enable_parallel_processing': False,  # Set to True for parallel processing (experimental)
    'max_concurrent_comparisons': 3,  # Maximum number of concurrent comparisons
    'continue_on_error': True,  # Continue with other tables if one fails
    'detailed_logging': True  # Enable detailed logging for batch operations
}