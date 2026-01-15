#!/usr/bin/env python3
"""
Database Table Comparator Module - Enhanced for Multiple Table Pairs
"""

import sys
import logging
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from databricks import sql
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


@dataclass
class DatabaseConfig:
    """Configuration for database connection."""
    host: str
    port: str
    database_name: str
    token: str
    environment: str


@dataclass
class TablePairConfig:
    """Configuration for a table pair comparison."""
    prod_table: str
    dev_table: str
    display_name: str
    prod_primary_keys: List[str]
    dev_primary_keys: List[str]
    ignored_columns: List[str]
    float_tolerance: float
    ignore_prod_pks: bool = False  # ADICIONAR
    ignore_dev_pks: bool = False   # ADICIONAR
    # Optional per-environment row filters: { column_name: [values...] }
    prod_row_filters: Dict[str, List[str]] = None
    dev_row_filters: Dict[str, List[str]] = None


@dataclass
class ComparisonResult:
    """Results of table comparison."""
    prod_table: str
    dev_table: str
    display_name: str
    tables_identical: bool
    dev_row_count: int
    prod_row_count: int
    dev_compared_rows: int
    prod_compared_rows: int
    missing_from_dev: List[str]
    missing_from_prod: List[str]
    differing_rows: List[Dict[str, Any]]
    schema_differences: List[str]
    ignored_columns: List[str]
    compared_columns: List[str]
    was_limited: bool
    max_rows_setting: int
    sampling_method: str
    comparison_duration: float
    prod_primary_key_columns: List[str]
    dev_primary_key_columns: List[str]
    executed_queries: Dict[str, List[Dict[str, str]]] = None  # {'DEV': [{'query': '', 'description': '', 'environment': ''}], 'PROD': [...]}
    error_message: Optional[str] = None


@dataclass
class BatchComparisonResult:
    """Results of batch table comparison."""
    total_pairs: int
    successful_comparisons: int
    failed_comparisons: int
    identical_tables: int
    different_tables: int
    results: List[ComparisonResult]
    total_duration: float
    summary: Dict[str, Any]


class DatabaseTableComparator:
    """Main class for comparing database tables between environments."""
    
    def __init__(self, dev_config: DatabaseConfig, prod_config: DatabaseConfig, 
                 float_tolerance: float = 1e-9, user_max_rows: int = None):  # ADICIONAR user_max_rows
        """Initialize the comparator."""
        self.dev_config = dev_config
        self.prod_config = prod_config
        self.float_tolerance = float_tolerance
        self.user_max_rows = user_max_rows  # ADICIONAR ESTA LINHA
        self.logger = logging.getLogger(__name__)
        self._connections = {}
        self._connection_lock = threading.Lock()
        # Query tracking
        self.executed_queries = {'DEV': [], 'PROD': []}
        
    def get_connection(self, config: DatabaseConfig):
        """Get or create a database connection with thread safety."""
        connection_key = f"{config.environment}_{threading.current_thread().ident}"
        
        with self._connection_lock:
            if connection_key not in self._connections:
                try:
                    connection = sql.connect(
                        server_hostname=config.host,
                        http_path=f"/sql/1.0/warehouses/{config.port}",
                        access_token=config.token
                    )
                    self._connections[connection_key] = connection
                    self.logger.info(f"Created new connection for {config.environment}")
                except Exception as e:
                    self.logger.error(f"Failed to connect to {config.environment} database: {str(e)}")
                    raise
            
            return self._connections[connection_key]
    
    def close_connections(self):
        """Close all database connections."""
        with self._connection_lock:
            for connection_key, connection in self._connections.items():
                try:
                    connection.close()
                    self.logger.info(f"Closed connection: {connection_key}")
                except Exception as e:
                    self.logger.warning(f"Error closing connection {connection_key}: {str(e)}")
            self._connections.clear()
    
    def execute_and_track_query(self, cursor, query: str, environment: str, description: str = ""):
        """Execute a query and track it for later reference."""
        try:
            # Log query execution for debugging
            self.logger.info(f">>> EXECUTING QUERY IN {environment}: {description}")
            self.logger.info(f">>> Current executed_queries keys: {list(self.executed_queries.keys())}")
            
            # Ensure the environment key exists in the tracking dictionary
            if environment not in self.executed_queries:
                self.executed_queries[environment] = []
                self.logger.info(f">>> Created new environment key: {environment}")
            
            # Clean up the query for better readability
            clean_query = ' '.join(query.strip().split())
            
            # Add to tracking with description
            query_info = {
                'query': query.strip(),
                'description': description,
                'environment': environment
            }
            self.executed_queries[environment].append(query_info)
            self.logger.info(f">>> Query tracked for {environment}. Total queries now: {len(self.executed_queries[environment])}")
            
            # Execute the query
            cursor.execute(query)
            return cursor
        except Exception as e:
            self.logger.error(f"Error in execute_and_track_query: {str(e)}")
            raise
        return cursor
    
    def fetch_table_schema(self, connection, config: DatabaseConfig, table_name: str) -> pd.DataFrame:
        """Fetch table schema information."""
        try:
            query = f"DESCRIBE TABLE {config.database_name}.{table_name}"
            
            with connection.cursor() as cursor:
                self.execute_and_track_query(cursor, query, config.environment, f"Get schema for table {table_name}")
                schema_data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
            schema_df = pd.DataFrame(schema_data, columns=columns)
            self.logger.info(f"Retrieved schema for {config.environment}.{table_name}: {len(schema_df)} columns")
            return schema_df
            
        except Exception as e:
            self.logger.error(f"Failed to fetch schema from {config.environment}.{table_name}: {str(e)}")
            raise
    
    def fetch_table_data(self, connection, config: DatabaseConfig, table_name: str, 
                        primary_keys: List[str], max_rows: int = None,
                        row_filters: Optional[Dict[str, List[str]]] = None) -> pd.DataFrame:
        """Fetch data from the specified table with optional row limiting."""
        try:
            from config import SAMPLING_CONFIG
            
            # SEMPRE usar as primary keys do formulário para ordenação
            order_clause = ", ".join(primary_keys)

            # Build optional exclusion WHERE clause
            filter_clause = self._build_where_exclusion_clause(row_filters)
            
            # Use user-defined max_rows if available, otherwise use config default
            effective_max_rows = max_rows
            if self.user_max_rows is not None:
                effective_max_rows = self.user_max_rows if self.user_max_rows > 0 else None
            
            if effective_max_rows and effective_max_rows > 0:
                sampling_method = SAMPLING_CONFIG.get('sampling_method', 'TOP_N')
                
                if sampling_method == 'LAST_N':
                    query = f"""
                    WITH generate_pk AS (
                        SELECT *,
                            ROW_NUMBER() OVER (ORDER BY {order_clause}) as pk_comparison_app
                        FROM {config.database_name}.{table_name}
                        {filter_clause}
                    )
                    ,ranked_data AS (
                        SELECT *,
                            ROW_NUMBER() OVER (ORDER BY pk_comparison_app DESC) as rn
                        FROM generate_pk
                    )
                    SELECT * EXCEPT(rn)
                    FROM ranked_data
                    WHERE rn <= {effective_max_rows}
                    ORDER BY {order_clause} ASC
                    """
                    self.logger.info(f"Fetching LAST {effective_max_rows:,} rows from {config.environment}.{table_name} ordered by {order_clause}")
                    
                elif sampling_method == 'RANDOM':
                    query = f"""
                    SELECT * FROM {config.database_name}.{table_name}
                    {filter_clause}
                    ORDER BY RAND(12345)
                    LIMIT {effective_max_rows}
                    """
                    self.logger.info(f"Fetching RANDOM {effective_max_rows:,} rows from {config.environment}.{table_name}")
                    
                else:  # TOP_N
                    query = f"""
                    SELECT * FROM {config.database_name}.{table_name}
                    {filter_clause}
                    ORDER BY {order_clause} ASC
                    LIMIT {effective_max_rows}
                    """
                    self.logger.info(f"Fetching FIRST {effective_max_rows:,} rows from {config.environment}.{table_name} ordered by {order_clause}")
            else:
                query = f"""
                SELECT * FROM {config.database_name}.{table_name}
                {filter_clause}
                ORDER BY {order_clause} ASC
                """
                self.logger.info(f"Fetching ALL data from {config.environment}.{table_name} ordered by {order_clause}")
            
            with connection.cursor() as cursor:
                # Determine the query description based on the sampling method and limits
                if effective_max_rows:
                    sampling_method = SAMPLING_CONFIG.get('sampling_method', 'TOP_N')
                    description = f"Fetch {sampling_method} {effective_max_rows:,} rows from table {table_name}"
                else:
                    description = f"Fetch ALL data from table {table_name}"
                
                self.execute_and_track_query(cursor, query, config.environment, description)
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
            df = pd.DataFrame(data, columns=columns)
            self.logger.info(f"Retrieved {len(df)} rows from {config.environment}.{table_name}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to fetch data from {config.environment}.{table_name}: {str(e)}")
            raise

    def _build_where_exclusion_clause(self, row_filters: Optional[Dict[str, List[str]]]) -> str:
        """Given a mapping of column -> list of values, build a WHERE clause to EXCLUDE matching rows.

        The form is: WHERE NOT (col1 IN (...) AND col2 IN (...) ...)
        Returns an empty string if no filters provided.
        """
        if not row_filters:
            return ""

        def is_number(value: str) -> bool:
            try:
                float(value)
                return True
            except Exception:
                return False

        parts: List[str] = []
        for column_name, values in row_filters.items():
            safe_values: List[str] = []
            for raw in values:
                val = str(raw).strip()
                if val == "":
                    continue
                if is_number(val):
                    safe_values.append(val)
                else:
                    escaped = val.replace("'", "''")
                    safe_values.append(f"'{escaped}'")
            if not safe_values:
                continue
            parts.append(f"{column_name} IN ({', '.join(safe_values)})")

        if not parts:
            return ""

        return "WHERE NOT (" + " AND ".join(parts) + ")"
    
    def get_row_count(self, connection, config: DatabaseConfig, table_name: str,
                      row_filters: Optional[Dict[str, List[str]]] = None) -> int:
        """Get total row count for the table after applying optional exclusion filters."""
        try:
            filter_clause = self._build_where_exclusion_clause(row_filters)
            query = f"SELECT COUNT(*) as row_count FROM {config.database_name}.{table_name} {filter_clause}"
            
            with connection.cursor() as cursor:
                self.execute_and_track_query(cursor, query, config.environment, f"Get row count for table {table_name}")
                result = cursor.fetchone()
                
            row_count = result[0] if result else 0
            self.logger.info(f"{config.environment}.{table_name} row count: {row_count}")
            return row_count
            
        except Exception as e:
            self.logger.error(f"Failed to get row count from {config.environment}.{table_name}: {str(e)}")
            raise
    
    def compare_schemas(self, dev_schema: pd.DataFrame, prod_schema: pd.DataFrame, ignored_columns: List[str]) -> List[str]:
        """Compare schemas between DEV and PROD tables, ignoring specified columns."""
        differences = []
        
        # Convert ignored columns to lowercase for case-insensitive comparison
        ignored_columns_lower = [col.lower() for col in ignored_columns]
        
        # Filter out ignored columns from both schemas
        dev_columns = set([col for col in dev_schema['col_name'].tolist() 
                        if col.lower() not in ignored_columns_lower])
        prod_columns = set([col for col in prod_schema['col_name'].tolist() 
                        if col.lower() not in ignored_columns_lower])
        
        missing_in_prod = dev_columns - prod_columns
        missing_in_dev = prod_columns - dev_columns
        
        if missing_in_prod:
            differences.append(f"Columns missing in PROD: {', '.join(missing_in_prod)}")
        
        if missing_in_dev:
            differences.append(f"Columns missing in DEV: {', '.join(missing_in_dev)}")
        
        # Check data types for common columns (excluding ignored ones)
        common_columns = dev_columns & prod_columns
        for col in common_columns:
            # Use query method to avoid boolean array issues
            dev_matches = dev_schema.query(f"col_name == '{col}'")
            prod_matches = prod_schema.query(f"col_name == '{col}'")
            
            if len(dev_matches) > 0 and len(prod_matches) > 0:
                dev_type = dev_matches['data_type'].iloc[0]
                prod_type = prod_matches['data_type'].iloc[0]
                
                if dev_type != prod_type:
                    differences.append(f"Column '{col}' type mismatch: DEV({dev_type}) vs PROD({prod_type})")
        
        return differences
    
    def validate_primary_keys(self, dev_df: pd.DataFrame, prod_df: pd.DataFrame, 
                            prod_primary_keys: List[str], dev_primary_keys: List[str],
                            ignore_prod_pks: bool = False, ignore_dev_pks: bool = False) -> bool:
        """Validate that primary key columns exist in both tables."""
        dev_columns = set(dev_df.columns)
        prod_columns = set(prod_df.columns)
        
        # Se ambas as PKs estão sendo ignoradas, não precisamos validá-las para comparação
        # mas ainda precisamos validá-las para ordenação/busca
        if ignore_prod_pks and ignore_dev_pks:
            self.logger.info("Both primary keys are being ignored for comparison, skipping PK validation")
            return True
        
        prod_pk_columns = set(prod_primary_keys)
        dev_pk_columns = set(dev_primary_keys)
        
        # Validar apenas as PKs que NÃO estão sendo ignoradas
        if not ignore_dev_pks:
            missing_in_dev = dev_pk_columns - dev_columns
            if missing_in_dev:
                self.logger.error(f"DEV primary key columns missing in DEV: {', '.join(missing_in_dev)}")
                return False
        
        if not ignore_prod_pks:
            missing_in_prod = prod_pk_columns - prod_columns
            if missing_in_prod:
                self.logger.error(f"PROD primary key columns missing in PROD: {', '.join(missing_in_prod)}")
                return False
        
        return True
    
    def create_primary_key(self, row: pd.Series, primary_keys: List[str], use_row_number: bool = False, row_number: int = None) -> str:
        """Create a composite primary key string from row data."""
        if use_row_number and row_number is not None:
            return str(row_number)
        else:
            pk_values = [str(row[col]) for col in primary_keys]
            return "|".join(pk_values)
    
    def compare_values(self, val1: Any, val2: Any) -> bool:
        """Compare two values with appropriate handling for different data types."""
        if pd.isna(val1) and pd.isna(val2):
            return True
        if pd.isna(val1) or pd.isna(val2):
            return False
        
        if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
            return abs(float(val1) - float(val2)) <= self.float_tolerance
        
        if isinstance(val1, datetime) and isinstance(val2, datetime):
            return val1 == val2
        
        return str(val1) == str(val2)
    
    def get_comparison_columns(self, dev_df: pd.DataFrame, prod_df: pd.DataFrame, 
                            ignored_columns: List[str], ignore_prod_pks: bool = False, 
                            ignore_dev_pks: bool = False, prod_primary_keys: List[str] = None, 
                            dev_primary_keys: List[str] = None) -> Tuple[List[str], List[str]]:
        """Get columns to compare and columns to ignore."""
        common_columns = list(set(dev_df.columns) & set(prod_df.columns))
        ignored_columns_lower = [col.lower() for col in ignored_columns]
        
        # Add primary keys to ignored columns if requested
        if ignore_prod_pks and prod_primary_keys:
            ignored_columns_lower.extend([pk.lower() for pk in prod_primary_keys])
        
        if ignore_dev_pks and dev_primary_keys:
            ignored_columns_lower.extend([pk.lower() for pk in dev_primary_keys])
        
        # Remove duplicates
        ignored_columns_lower = list(set(ignored_columns_lower))
        
        ignored_columns_found = []
        columns_to_compare = []
        
        for col in common_columns:
            if col.lower() in ignored_columns_lower:
                ignored_columns_found.append(col)
            else:
                columns_to_compare.append(col)
        
        return columns_to_compare, ignored_columns_found
    
    def compare_single_pair(self, table_config: TablePairConfig) -> ComparisonResult:
        """Compare a single table pair."""
        start_time = datetime.now()
        
        # Reset query tracking for this comparison
        self.executed_queries = {'DEV': [], 'PROD': []}
        self.logger.info(">>> Query tracking reset. Initial state: %s", self.executed_queries)
        
        try:
            self.logger.info(f"Starting comparison: {table_config.display_name}")
            self.logger.info(f"DEV config environment: {self.dev_config.environment}")
            self.logger.info(f"PROD config environment: {self.prod_config.environment}")
            
            # Get connections
            dev_conn = self.get_connection(self.dev_config)
            prod_conn = self.get_connection(self.prod_config)
            
            # Use user-defined max_rows if available, otherwise use config default
            from config import SAMPLING_CONFIG
            config_max_rows = SAMPLING_CONFIG['max_rows_for_comparison'] if SAMPLING_CONFIG['enable_row_limit'] else None
            effective_max_rows = config_max_rows
            
            if self.user_max_rows is not None:
                effective_max_rows = self.user_max_rows if self.user_max_rows > 0 else None
            
            # Step 1: Compare row counts
            dev_total_count = self.get_row_count(
                dev_conn, self.dev_config, table_config.dev_table,
                row_filters=table_config.dev_row_filters or {}
            )
            prod_total_count = self.get_row_count(
                prod_conn, self.prod_config, table_config.prod_table,
                row_filters=table_config.prod_row_filters or {}
            )
            
            # Step 2: Compare schemas
            dev_schema = self.fetch_table_schema(dev_conn, self.dev_config, table_config.dev_table)
            prod_schema = self.fetch_table_schema(prod_conn, self.prod_config, table_config.prod_table)
            
            # Create combined ignored columns list including PKs if requested
            combined_ignored_columns = list(table_config.ignored_columns)
            if table_config.ignore_prod_pks:
                combined_ignored_columns.extend(table_config.prod_primary_keys)
            if table_config.ignore_dev_pks:
                combined_ignored_columns.extend(table_config.dev_primary_keys)
            
            schema_differences = self.compare_schemas(dev_schema, prod_schema, combined_ignored_columns)
            
            # Step 3: Fetch and compare data - USAR CHAVES PRIMÁRIAS ESPECÍFICAS
            dev_df = self.fetch_table_data(
                dev_conn, self.dev_config, table_config.dev_table,
                table_config.dev_primary_keys, effective_max_rows,
                row_filters=table_config.dev_row_filters or {}
            )
            prod_df = self.fetch_table_data(
                prod_conn, self.prod_config, table_config.prod_table,
                table_config.prod_primary_keys, effective_max_rows,
                row_filters=table_config.prod_row_filters or {}
            )
            
            # Validate primary keys - PASSAR OS PARÂMETROS DE IGNORE
            if not self.validate_primary_keys(dev_df, prod_df, table_config.prod_primary_keys, 
                                            table_config.dev_primary_keys, table_config.ignore_prod_pks, 
                                            table_config.ignore_dev_pks):
                raise ValueError("Primary key validation failed")
            
            # Step 4: Detailed data comparison
            result = self.compare_data(dev_df, prod_df, schema_differences, dev_total_count, 
                                    prod_total_count, effective_max_rows, table_config)
            
            duration = (datetime.now() - start_time).total_seconds()
            result.comparison_duration = duration
            
            self.logger.info(f"Completed comparison: {table_config.display_name} in {duration:.2f}s")
            return result
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Failed comparison: {table_config.display_name} - {str(e)}")
            
            return ComparisonResult(
                prod_table=table_config.prod_table,
                dev_table=table_config.dev_table,
                display_name=table_config.display_name,
                tables_identical=False,
                dev_row_count=0,
                prod_row_count=0,
                dev_compared_rows=0,
                prod_compared_rows=0,
                missing_from_dev=[],
                missing_from_prod=[],
                differing_rows=[],
                schema_differences=[],
                ignored_columns=[],
                compared_columns=[],
                was_limited=False,
                max_rows_setting=0,
                sampling_method='',
                comparison_duration=duration,
                prod_primary_key_columns=[],
                dev_primary_key_columns=[],
                executed_queries=self.executed_queries.copy(),
                error_message=str(e)
            )
    
    def compare_data(self, dev_df: pd.DataFrame, prod_df: pd.DataFrame, 
                    schema_differences: List[str], dev_total_rows: int, prod_total_rows: int,
                    max_rows: int, table_config: TablePairConfig) -> ComparisonResult:
        """Perform detailed data comparison between DEV and PROD tables."""
        columns_to_compare, ignored_columns_found = self.get_comparison_columns(
            dev_df, prod_df, table_config.ignored_columns, 
            table_config.ignore_prod_pks, table_config.ignore_dev_pks,
            table_config.prod_primary_keys, table_config.dev_primary_keys)
        
        # Se ambas as PKs estão sendo ignoradas, usar comparação baseada em posição/conteúdo
        if table_config.ignore_prod_pks and table_config.ignore_dev_pks:
            self.logger.info("Both primary keys ignored - using position-based comparison")
            
            # IMPORTANTE: Os DataFrames já vêm ordenados pelas PKs do formulário
            # então podemos comparar linha por linha baseado na posição
            min_rows = min(len(dev_df), len(prod_df))
            
            missing_from_dev = []
            missing_from_prod = []
            differing_rows = []
            
            # Se há diferença no número de linhas, reportar as extras como "missing"
            if len(prod_df) > len(dev_df):
                missing_from_dev = [f"row_{i+len(dev_df)+1}" for i in range(len(prod_df) - len(dev_df))]
            elif len(dev_df) > len(prod_df):
                missing_from_prod = [f"row_{i+len(prod_df)+1}" for i in range(len(dev_df) - len(prod_df))]
            
            # Comparar linhas na mesma posição (já ordenadas pelas PKs)
            for i in range(min_rows):
                dev_row = dev_df.iloc[i]
                prod_row = prod_df.iloc[i]
                
                differing_columns = []
                
                for col in columns_to_compare:
                    try:
                        # Safely check if column exists and get values
                        if col in dev_row.index.tolist() and col in prod_row.index.tolist():
                            dev_val = dev_row[col]
                            prod_val = prod_row[col]
                            if not self.compare_values(dev_val, prod_val):
                                differing_columns.append({
                                    'column': col,
                                    'dev_value': str(dev_val),
                                    'prod_value': str(prod_val)
                                })
                    except (KeyError, IndexError, ValueError):
                        # Skip column if there's any issue accessing it
                        continue
                
                if differing_columns:
                    # Mostrar as PKs originais para referência, mesmo que ignoradas na comparação
                    dev_pk_display = self.create_primary_key(dev_row, table_config.dev_primary_keys)
                    prod_pk_display = self.create_primary_key(prod_row, table_config.prod_primary_keys)
                    
                    differing_rows.append({
                        'primary_key': f"Position {i+1} [DEV: {dev_pk_display}, PROD: {prod_pk_display}]",
                        'differences': differing_columns
                    })
        
        else:
            # Lógica com PKs - os DataFrames já vêm ordenados pelas PKs corretas
            # Verificar se DEV usa ROW_NUMBER como PK (PK_Account, PK_Customer, etc.)
            dev_uses_row_number = (len(table_config.dev_primary_keys) == 1 and 
                                table_config.dev_primary_keys[0].startswith('PK_'))
            
            # Verificar se PROD usa ROW_NUMBER como PK
            prod_uses_row_number = (len(table_config.prod_primary_keys) == 1 and 
                                table_config.prod_primary_keys[0].startswith('PK_'))
            
            if dev_uses_row_number and not prod_uses_row_number and not table_config.ignore_dev_pks:
                # DEV usa ROW_NUMBER, PROD usa colunas reais
                # Os dados já vêm ordenados, então podemos usar a posição diretamente
                dev_pk_map = {}
                prod_pk_map = {}
                
                for idx, row in dev_df.iterrows():
                    row_number = idx + 1  # ROW_NUMBER começa em 1
                    dev_pk_map[str(row_number)] = idx
                
                for idx, row in prod_df.iterrows():
                    row_number = idx + 1  # ROW_NUMBER começa em 1
                    prod_pk_map[str(row_number)] = idx
                
                dev_pks = set(dev_pk_map.keys())
                prod_pks = set(prod_pk_map.keys())
                
            elif prod_uses_row_number and not dev_uses_row_number and not table_config.ignore_prod_pks:
                # PROD usa ROW_NUMBER, DEV usa colunas reais
                # Os dados já vêm ordenados, então podemos usar a posição diretamente
                dev_pk_map = {}
                prod_pk_map = {}
                
                for idx, row in dev_df.iterrows():
                    row_number = idx + 1  # ROW_NUMBER começa em 1
                    dev_pk_map[str(row_number)] = idx
                
                for idx, row in prod_df.iterrows():
                    row_number = idx + 1  # ROW_NUMBER começa em 1
                    prod_pk_map[str(row_number)] = idx
                
                dev_pks = set(dev_pk_map.keys())
                prod_pks = set(prod_pk_map.keys())
                
            else:
                # Lógica original - usar as PKs diretamente (dados já ordenados)
                dev_pk_map = {self.create_primary_key(row, table_config.dev_primary_keys): idx 
                            for idx, row in dev_df.iterrows()}
                prod_pk_map = {self.create_primary_key(row, table_config.prod_primary_keys): idx 
                            for idx, row in prod_df.iterrows()}
                
                dev_pks = set(dev_pk_map.keys())
                prod_pks = set(prod_pk_map.keys())
            
            # Resto da lógica permanece igual
            missing_from_dev = list(prod_pks - dev_pks)
            missing_from_prod = list(dev_pks - prod_pks)
            
            common_pks = dev_pks & prod_pks
            differing_rows = []
            
            for pk in common_pks:
                dev_idx = dev_pk_map[pk]
                prod_idx = prod_pk_map[pk]
                
                dev_row = dev_df.iloc[dev_idx]
                prod_row = prod_df.iloc[prod_idx]
                
                differing_columns = []
                
                for col in columns_to_compare:
                    try:
                        # Safely check if column exists and get values
                        if col in dev_row.index.tolist() and col in prod_row.index.tolist():
                            dev_val = dev_row[col]
                            prod_val = prod_row[col]
                            if not self.compare_values(dev_val, prod_val):
                                differing_columns.append({
                                    'column': col,
                                    'dev_value': str(dev_val),
                                    'prod_value': str(prod_val)
                                })
                    except (KeyError, IndexError, ValueError):
                        # Skip column if there's any issue accessing it
                        continue
                
                if differing_columns:
                    differing_rows.append({
                        'primary_key': pk,
                        'differences': differing_columns
                    })
        
        was_limited = max_rows and (dev_total_rows > max_rows or prod_total_rows > max_rows)
        
        tables_identical = (
            len(missing_from_dev) == 0 and
            len(missing_from_prod) == 0 and
            len(differing_rows) == 0 and
            len(schema_differences) == 0 and
            dev_total_rows == prod_total_rows
        )
        
        from config import SAMPLING_CONFIG
        sampling_method = SAMPLING_CONFIG.get('sampling_method', 'TOP_N')
        
        # Log final query state
        self.logger.info(">>> FINAL QUERY STATE before return:")
        for env, queries in self.executed_queries.items():
            self.logger.info(f">>> {env}: {len(queries)} queries")
            for i, q in enumerate(queries):
                self.logger.info(f">>>   {i+1}. {q['description']}")
        
        return ComparisonResult(
            prod_table=table_config.prod_table,
            dev_table=table_config.dev_table,
            display_name=table_config.display_name,
            tables_identical=tables_identical,
            dev_row_count=dev_total_rows,
            prod_row_count=prod_total_rows,
            dev_compared_rows=len(dev_df),
            prod_compared_rows=len(prod_df),
            missing_from_dev=missing_from_dev,
            missing_from_prod=missing_from_prod,
            differing_rows=differing_rows,
            schema_differences=schema_differences,
            ignored_columns=ignored_columns_found,
            compared_columns=columns_to_compare,
            was_limited=was_limited,
            max_rows_setting=max_rows or 0,
            sampling_method=sampling_method,
            comparison_duration=0.0,
            prod_primary_key_columns=table_config.prod_primary_keys,
            dev_primary_key_columns=table_config.dev_primary_keys,
            executed_queries=self.executed_queries.copy()
        )
    
    def run_batch_comparison(self, table_pairs: List[TablePairConfig]) -> BatchComparisonResult:
        """Execute batch comparison of multiple table pairs."""
        start_time = datetime.now()
        self.logger.info(f"Starting batch comparison of {len(table_pairs)} table pairs")
        
        from config import BATCH_CONFIG
        
        results = []
        successful_comparisons = 0
        failed_comparisons = 0
        identical_tables = 0
        different_tables = 0
        
        try:
            if BATCH_CONFIG.get('enable_parallel_processing', False):
                # Parallel processing (experimental)
                max_workers = min(BATCH_CONFIG.get('max_concurrent_comparisons', 3), len(table_pairs))
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_config = {executor.submit(self.compare_single_pair, config): config 
                                      for config in table_pairs}
                    
                    for future in as_completed(future_to_config):
                        config = future_to_config[future]
                        try:
                            result = future.result()
                            results.append(result)
                            
                            if result.error_message:
                                failed_comparisons += 1
                            else:
                                successful_comparisons += 1
                                if result.tables_identical:
                                    identical_tables += 1
                                else:
                                    different_tables += 1
                                    
                        except Exception as e:
                            self.logger.error(f"Parallel comparison failed for {config.display_name}: {str(e)}")
                            failed_comparisons += 1
                            if BATCH_CONFIG.get('continue_on_error', True):
                                continue
                            else:
                                break
            else:
                # Sequential processing
                for config in table_pairs:
                    try:
                        result = self.compare_single_pair(config)
                        results.append(result)
                        
                        if result.error_message:
                            failed_comparisons += 1
                        else:
                            successful_comparisons += 1
                            if result.tables_identical:
                                identical_tables += 1
                            else:
                                different_tables += 1
                                
                    except Exception as e:
                        self.logger.error(f"Sequential comparison failed for {config.display_name}: {str(e)}")
                        failed_comparisons += 1
                        if BATCH_CONFIG.get('continue_on_error', True):
                            continue
                        else:
                            break
            
            total_duration = (datetime.now() - start_time).total_seconds()
            
            # Create summary
            summary = {
                'total_pairs': len(table_pairs),
                'successful_comparisons': successful_comparisons,
                'failed_comparisons': failed_comparisons,
                'identical_tables': identical_tables,
                'different_tables': different_tables,
                'success_rate': (successful_comparisons / len(table_pairs)) * 100 if table_pairs else 0,
                'identical_rate': (identical_tables / successful_comparisons) * 100 if successful_comparisons else 0
            }
            
            self.logger.info(f"Batch comparison completed in {total_duration:.2f}s")
            self.logger.info(f"Summary: {successful_comparisons}/{len(table_pairs)} successful, "
                           f"{identical_tables} identical, {different_tables} different")
            
            return BatchComparisonResult(
                total_pairs=len(table_pairs),
                successful_comparisons=successful_comparisons,
                failed_comparisons=failed_comparisons,
                identical_tables=identical_tables,
                different_tables=different_tables,
                results=results,
                total_duration=total_duration,
                summary=summary
            )
            
        finally:
            self.close_connections()