#!/usr/bin/env python3
"""
Flask Web Application for Database Table Comparison - Enhanced with Table Management
"""

from flask import Flask, render_template, request, jsonify, flash, redirect, url_for, session
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, FloatField, TextAreaField, FieldList, FormField, HiddenField, IntegerField
from wtforms.validators import DataRequired, NumberRange, Optional
import json
import logging
from datetime import datetime
import threading
import uuid
import re
from comparator import DatabaseTableComparator, DatabaseConfig, TablePairConfig, BatchComparisonResult

# Import configuration
from config import (DEV_DEFAULTS, PROD_DEFAULTS, COMPARISON_DEFAULTS, APP_CONFIG, SAMPLING_CONFIG,
                   load_available_tables, save_available_tables, add_table, 
                   update_table, remove_table)

import os
import webbrowser
import secrets

from storage import load_connection_settings, save_connection_settings, clear_connection_settings

app = Flask(__name__)

_SECRET_KEY_FILE = os.path.join('data', 'flask_secret_key.txt')


def _load_or_create_flask_secret_key() -> str:
    """Return a stable secret key for local usage.

    Priority:
    1) Environment variable FLASK_SECRET_KEY (explicit override)
    2) Local file data/flask_secret_key.txt (auto-managed, git-ignored)
    3) Generated ephemeral key (fallback if file can't be written)
    """
    env_secret = os.getenv('FLASK_SECRET_KEY')
    if env_secret:
        return env_secret

    os.makedirs(os.path.dirname(_SECRET_KEY_FILE), exist_ok=True)

    try:
        with open(_SECRET_KEY_FILE, 'r', encoding='utf-8') as f:
            file_secret = f.read().strip()
            if file_secret:
                return file_secret
    except FileNotFoundError:
        pass

    new_secret = secrets.token_urlsafe(32)
    try:
        with open(_SECRET_KEY_FILE, 'w', encoding='utf-8') as f:
            f.write(new_secret)
        return new_secret
    except OSError:
        logging.getLogger(__name__).warning(
            'Could not persist Flask secret key to %s; using an ephemeral key for this run',
            _SECRET_KEY_FILE,
        )
        return new_secret


app.secret_key = _load_or_create_flask_secret_key()

# Per-process instance id. Used to detect app restarts so we can clear transient UI state
# (like last comparison table pairs) while keeping persisted Settings.
_APP_INSTANCE_ID = str(uuid.uuid4())

# Global storage for comparison results
comparison_results = {}
comparison_status = {}
cancellation_requests = {}

def _get_connection_settings() -> dict:
    # Persistent store (file + OS keyring). Safe to use across restarts.
    return load_connection_settings()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.before_request
def _clear_last_comparison_on_restart():
    """Ensure the Compare page starts empty after an app restart.

    Flask's default session is stored client-side (signed cookie), so values like
    `last_comparison` may survive app restarts. We treat them as transient and
    clear them when a new server instance starts.
    """
    previous_instance = session.get('_app_instance_id')
    if previous_instance != _APP_INSTANCE_ID:
        session['_app_instance_id'] = _APP_INSTANCE_ID
        session.pop('last_comparison', None)
        session.pop('last_comparison_id', None)


class TablePairForm(FlaskForm):
    """Form for individual table pair configuration."""
    class Meta:
        csrf = False  # Disable CSRF for subforms
    
    prod_table = SelectField('PROD Table', choices=[], validators=[Optional()])
    dev_table = SelectField('DEV Table', choices=[], validators=[Optional()])
    display_name = StringField('Display Name', validators=[Optional()])
    primary_keys = StringField('Primary Keys (comma-separated)', validators=[Optional()])
    ignored_columns = TextAreaField('Ignored Columns (one per line)', render_kw={"rows": 4}, validators=[Optional()])


class ComparisonForm(FlaskForm):
    """Enhanced form for database comparison configuration."""
    
    # Database Configuration
    dev_host = StringField('DEV Host', validators=[DataRequired()], 
                          default=DEV_DEFAULTS['host'])
    dev_port = StringField('DEV Warehouse ID', validators=[DataRequired()], 
                          default=DEV_DEFAULTS['port'])
    dev_database = StringField('DEV Database', validators=[DataRequired()], 
                              default=DEV_DEFAULTS['database'])
    dev_token = TextAreaField('DEV Access Token', validators=[DataRequired()], 
                             default=DEV_DEFAULTS['token'],
                             render_kw={"rows": 3, "placeholder": "Enter your DEV Databricks token (dapi...)"})
    
    prod_host = StringField('PROD Host', validators=[DataRequired()], 
                           default=PROD_DEFAULTS['host'])
    prod_port = StringField('PROD Warehouse ID', validators=[DataRequired()], 
                           default=PROD_DEFAULTS['port'])
    prod_database = StringField('PROD Database', validators=[DataRequired()], 
                               default=PROD_DEFAULTS['database'])
    prod_token = TextAreaField('PROD Access Token', validators=[DataRequired()], 
                              default=PROD_DEFAULTS['token'],
                              render_kw={"rows": 3, "placeholder": "Enter your PROD Databricks token (dapi...)"})
    
    # Global Settings
    float_tolerance = FloatField('Float Tolerance', validators=[NumberRange(min=0)], 
                                default=COMPARISON_DEFAULTS['float_tolerance'])
    
    max_rows_limit = IntegerField('Max Rows Limit', 
                                 validators=[Optional(), NumberRange(min=0)], 
                                 default=SAMPLING_CONFIG['max_rows_for_comparison'])
    
    # Hidden field to track number of table pairs
    table_pairs_count = HiddenField('Table Pairs Count', default='1')


def extract_table_pairs_from_request():
    """Extract table pairs data from request form data."""
    table_pairs = []
    
    # Get all form keys that start with 'table_pairs-'
    pair_indices = set()
    for key in request.form.keys():
        if key.startswith('table_pairs-') and '-' in key[12:]:  # Skip 'table_pairs-' prefix
            try:
                index = int(key.split('-')[1])
                pair_indices.add(index)
            except (IndexError, ValueError):
                continue
    
    # Extract data for each pair
    for index in sorted(pair_indices):
        prod_table = request.form.get(f'table_pairs-{index}-prod_table', '').strip()
        dev_table = request.form.get(f'table_pairs-{index}-dev_table', '').strip()
        prod_primary_keys = request.form.get(f'table_pairs-{index}-prod_primary_keys', '').strip()
        dev_primary_keys = request.form.get(f'table_pairs-{index}-dev_primary_keys', '').strip()
        ignored_columns = request.form.get(f'table_pairs-{index}-ignored_columns', '').strip()
        # Row filters: comma-separated values per column name for each environment
        prod_filter_columns = request.form.get(f'table_pairs-{index}-prod_filter_columns', '').strip()
        prod_filter_values = request.form.get(f'table_pairs-{index}-prod_filter_values', '').strip()
        dev_filter_columns = request.form.get(f'table_pairs-{index}-dev_filter_columns', '').strip()
        dev_filter_values = request.form.get(f'table_pairs-{index}-dev_filter_values', '').strip()
        ignore_prod_pks = request.form.get(f'table_pairs-{index}-ignore_prod_pks') == 'on'  # ADICIONAR
        ignore_dev_pks = request.form.get(f'table_pairs-{index}-ignore_dev_pks') == 'on'    # ADICIONAR
        
        # Only add pairs that have both tables selected
        if prod_table and dev_table:
            # Parse row filters into dicts {column: [values]}
            def parse_filters(columns_csv: str, values_multiline: str):
                filters = {}
                columns = [c.strip() for c in columns_csv.split(',') if c.strip()]
                # Values per column separated by lines; each line is comma-separated values for the corresponding column
                lines = [line.strip() for line in values_multiline.split('\n') if line.strip()]
                for i, col in enumerate(columns):
                    # Match line by index; if not enough lines, treat as empty
                    line_values = lines[i] if i < len(lines) else ''
                    vals = [v.strip() for v in line_values.split(',') if v.strip()]
                    if vals:
                        filters[col] = vals
                return filters

            prod_row_filters = parse_filters(prod_filter_columns, prod_filter_values)
            dev_row_filters = parse_filters(dev_filter_columns, dev_filter_values)

            # Build display name as "PROD x DEV"
            display_name = f"{prod_table} x {dev_table}"

            table_pairs.append({
                'prod_table': prod_table,
                'dev_table': dev_table,
                'display_name': display_name,
                'prod_primary_keys': prod_primary_keys,
                'dev_primary_keys': dev_primary_keys,
                'ignored_columns': ignored_columns,
                'ignore_prod_pks': ignore_prod_pks,  # ADICIONAR
                'ignore_dev_pks': ignore_dev_pks,     # ADICIONAR
                'prod_row_filters': prod_row_filters,
                'dev_row_filters': dev_row_filters
            })
    
    return table_pairs


def create_table_pair_configs(form_data, table_pairs_data) -> list[TablePairConfig]:
    """Create table pair configurations based on form data."""
    table_configs = []
    
    for pair_data in table_pairs_data:
        if pair_data['prod_table'] and pair_data['dev_table']:
            prod_primary_keys = [key.strip() for key in pair_data['prod_primary_keys'].split(',') if key.strip()]
            dev_primary_keys = [key.strip() for key in pair_data['dev_primary_keys'].split(',') if key.strip()]
            # Support both legacy newline-separated and new pipe-separated formats
            ignored_columns_raw = pair_data['ignored_columns'] or ''
            normalized = re.sub(r"\s*\|\s*", "|", ignored_columns_raw.replace('\n', '|'))
            ignored_columns = [col.strip() for col in normalized.split('|') if col.strip()]
            
            # Ensure at least one primary key exists for each environment
            if not prod_primary_keys:
                prod_primary_keys = ['id']  # Default fallback
            if not dev_primary_keys:
                dev_primary_keys = ['id']  # Default fallback
            
            config = TablePairConfig(
                prod_table=pair_data['prod_table'],
                dev_table=pair_data['dev_table'],
                display_name=pair_data['display_name'] or f"{pair_data['prod_table']} x {pair_data['dev_table']}",
                prod_primary_keys=prod_primary_keys,
                dev_primary_keys=dev_primary_keys,
                ignored_columns=ignored_columns,
                float_tolerance=form_data['float_tolerance'],
                ignore_prod_pks=pair_data.get('ignore_prod_pks', False),  # ADICIONAR
                ignore_dev_pks=pair_data.get('ignore_dev_pks', False),     # ADICIONAR
                prod_row_filters=pair_data.get('prod_row_filters', {}),
                dev_row_filters=pair_data.get('dev_row_filters', {})
            )
            table_configs.append(config)
    
    return table_configs

def run_comparison_async(comparison_id, form_data, table_pairs_data):
    """Run comparison in background thread with cancellation support."""
    start_time = datetime.now()
    
    try:
        # Initialize status with table list
        table_list = []
        for i, pair in enumerate(table_pairs_data):
            table_list.append({
                'index': i,
                'display_name': pair['display_name'],
                'prod_table': pair['prod_table'],
                'dev_table': pair['dev_table'],
                'status': 'pending',
                'start_time': None,
                'end_time': None,
                'duration': 0,
                'status_detail': None,
                'comparison_summary': None
            })
        
        comparison_status[comparison_id] = {
            'status': 'running', 
            'progress': 'Initializing...',
            'start_time': start_time.isoformat(),
            'table_list': table_list,
            'current_table_index': -1,
            'total_duration': 0,
            'can_cancel': True
        }
        
        # Create database configurations
        dev_config = DatabaseConfig(
            host=form_data['dev_host'],
            port=form_data['dev_port'],
            database_name=form_data['dev_database'],
            token=form_data['dev_token'],
            environment="DEV"
        )
        
        prod_config = DatabaseConfig(
            host=form_data['prod_host'],
            port=form_data['prod_port'],
            database_name=form_data['prod_database'],
            token=form_data['prod_token'],
            environment="PROD"
        )
        
        # Create table pair configurations
        table_configs = create_table_pair_configs(form_data, table_pairs_data)
        
        if not table_configs:
            raise ValueError("No valid table pairs configured")
        
        comparison_status[comparison_id]['progress'] = f'Starting comparison of {len(table_configs)} table pair(s)...'
        
        # Create and run comparator with user-defined max rows
        comparator = DatabaseTableComparator(
            dev_config=dev_config,
            prod_config=prod_config,
            float_tolerance=form_data['float_tolerance'],
            user_max_rows=form_data.get('max_rows_limit')
        )
        
        results = []
        successful_comparisons = 0
        failed_comparisons = 0
        identical_tables = 0
        different_tables = 0
        
        # Process each table individually to track progress
        for i, config in enumerate(table_configs):
            # Check for cancellation request ANTES de processar cada tabela
            if comparison_id in cancellation_requests:
                logger.info(f"Comparison {comparison_id} cancelled by user at table {i+1}/{len(table_configs)}")
                comparison_status[comparison_id]['status'] = 'cancelled'
                comparison_status[comparison_id]['progress'] = f'Cancelled after processing {i} of {len(table_configs)} tables'
                comparison_status[comparison_id]['can_cancel'] = False
                break
            
            table_start_time = datetime.now()
            
            # Update status for current table
            comparison_status[comparison_id]['current_table_index'] = i
            comparison_status[comparison_id]['table_list'][i]['status'] = 'running'
            comparison_status[comparison_id]['table_list'][i]['start_time'] = table_start_time.isoformat()
            comparison_status[comparison_id]['progress'] = f'Comparing table {i+1}/{len(table_configs)}: {config.display_name}'
            
            try:
                result = comparator.compare_single_pair(config)
                table_end_time = datetime.now()
                table_duration = (table_end_time - table_start_time).total_seconds()
                
                # Determine final status based on comparison result
                if result.error_message:
                    final_status = 'error'
                    status_detail = result.error_message
                    failed_comparisons += 1
                elif result.tables_identical:
                    final_status = 'identical'
                    status_detail = 'Tables are completely identical'
                    successful_comparisons += 1
                    identical_tables += 1
                else:
                    final_status = 'different'
                    # Create detailed status message
                    differences = []
                    if result.schema_differences:
                        differences.append(f"{len(result.schema_differences)} schema differences")
                    if result.dev_row_count != result.prod_row_count:
                        differences.append(f"Row count mismatch: DEV({result.dev_row_count:,}) vs PROD({result.prod_row_count:,})")
                    if result.differing_rows:
                        differences.append(f"{len(result.differing_rows)} differing rows")
                    if result.missing_from_dev:
                        differences.append(f"{len(result.missing_from_dev)} missing from DEV")
                    if result.missing_from_prod:
                        differences.append(f"{len(result.missing_from_prod)} missing from PROD")
                    
                    status_detail = "; ".join(differences) if differences else "Tables have differences"
                    successful_comparisons += 1
                    different_tables += 1
                
                # Update table status
                comparison_status[comparison_id]['table_list'][i]['status'] = final_status
                comparison_status[comparison_id]['table_list'][i]['end_time'] = table_end_time.isoformat()
                comparison_status[comparison_id]['table_list'][i]['duration'] = table_duration
                comparison_status[comparison_id]['table_list'][i]['status_detail'] = status_detail
                comparison_status[comparison_id]['table_list'][i]['comparison_summary'] = {
                    'dev_row_count': result.dev_row_count,
                    'prod_row_count': result.prod_row_count,
                    'schema_differences_count': len(result.schema_differences) if result.schema_differences else 0,
                    'differing_rows_count': len(result.differing_rows) if result.differing_rows else 0,
                    'missing_from_dev_count': len(result.missing_from_dev) if result.missing_from_dev else 0,
                    'missing_from_prod_count': len(result.missing_from_prod) if result.missing_from_prod else 0,
                    'was_limited': result.was_limited
                }
                
                results.append(result)
                        
            except Exception as e:
                table_end_time = datetime.now()
                table_duration = (table_end_time - table_start_time).total_seconds()
                
                # Update table status with error
                comparison_status[comparison_id]['table_list'][i]['status'] = 'error'
                comparison_status[comparison_id]['table_list'][i]['end_time'] = table_end_time.isoformat()
                comparison_status[comparison_id]['table_list'][i]['duration'] = table_duration
                comparison_status[comparison_id]['table_list'][i]['status_detail'] = str(e)
                
                failed_comparisons += 1
                logger.error(f"Comparison failed for {config.display_name}: {str(e)}")
        
        # Create batch result (mesmo se foi cancelado)
        total_duration = (datetime.now() - start_time).total_seconds()
        
        # Criar resultado apenas com as tabelas que foram processadas
        if results:
            if len(results) == 1 and len(table_configs) == 1:
                batch_result = BatchComparisonResult(
                    total_pairs=1,
                    successful_comparisons=successful_comparisons,
                    failed_comparisons=failed_comparisons,
                    identical_tables=identical_tables,
                    different_tables=different_tables,
                    results=results,
                    total_duration=total_duration,
                    summary={
                        'total_pairs': 1,
                        'successful_comparisons': successful_comparisons,
                        'failed_comparisons': failed_comparisons,
                        'identical_tables': identical_tables,
                        'different_tables': different_tables,
                        'success_rate': (successful_comparisons / 1) * 100,
                        'identical_rate': (identical_tables / successful_comparisons) * 100 if successful_comparisons > 0 else 0
                    }
                )
            else:
                # Para batch results, criar um resultado customizado
                batch_result = BatchComparisonResult(
                    total_pairs=len(results),
                    successful_comparisons=successful_comparisons,
                    failed_comparisons=failed_comparisons,
                    identical_tables=identical_tables,
                    different_tables=different_tables,
                    results=results,
                    total_duration=total_duration,
                    summary={
                        'total_pairs': len(results),
                        'successful_comparisons': successful_comparisons,
                        'failed_comparisons': failed_comparisons,
                        'identical_tables': identical_tables,
                        'different_tables': different_tables,
                        'success_rate': (successful_comparisons / len(results)) * 100 if results else 0,
                        'identical_rate': (identical_tables / successful_comparisons) * 100 if successful_comparisons > 0 else 0
                    }
                )
            
            # Store results
            comparison_results[comparison_id] = {
                'result': batch_result,
                'config': form_data,
                'table_pairs': table_pairs_data,
                'timestamp': datetime.now().isoformat(),
                'was_cancelled': comparison_id in cancellation_requests  # ADICIONAR ESTA LINHA
            }
        
        # Update final status
        if comparison_id in cancellation_requests:
            comparison_status[comparison_id]['status'] = 'cancelled'
            comparison_status[comparison_id]['progress'] = f'Cancelled - {len(results)} of {len(table_configs)} tables processed'
            # Remove from cancellation requests
            cancellation_requests.pop(comparison_id, None)
        else:
            comparison_status[comparison_id]['status'] = 'completed'
            comparison_status[comparison_id]['progress'] = 'Comparison completed!'
        
        comparison_status[comparison_id]['total_duration'] = total_duration
        comparison_status[comparison_id]['current_table_index'] = -1
        comparison_status[comparison_id]['can_cancel'] = False
        
    except Exception as e:
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Comparison failed: {str(e)}")
        comparison_status[comparison_id] = {
            'status': 'error', 
            'progress': f'Error: {str(e)}',
            'total_duration': total_duration,
            'table_list': comparison_status.get(comparison_id, {}).get('table_list', []),
            'can_cancel': False
        }
        # Remove from cancellation requests if exists
        cancellation_requests.pop(comparison_id, None)


@app.route('/')
def index():
    """Redirect root to compare page for better navigation."""
    return redirect(url_for('compare_page'))


@app.route('/compare', methods=['GET'])
def compare_page():
    """Comparison configuration page (GET)."""
    form = ComparisonForm()

    # Load values from session if available (avoid persisting credentials in client-side session)
    is_first_time = 'last_comparison' not in session
    last_data = session.get('last_comparison', {})
    connection_settings = _get_connection_settings()

    # Prefer settings saved on server; otherwise fall back to environment defaults
    form.dev_host.data = connection_settings.get('dev_host', DEV_DEFAULTS['host'])
    form.dev_port.data = connection_settings.get('dev_port', DEV_DEFAULTS['port'])
    form.dev_database.data = connection_settings.get('dev_database', DEV_DEFAULTS['database'])
    form.dev_token.data = connection_settings.get('dev_token', DEV_DEFAULTS['token'])

    form.prod_host.data = connection_settings.get('prod_host', PROD_DEFAULTS['host'])
    form.prod_port.data = connection_settings.get('prod_port', PROD_DEFAULTS['port'])
    form.prod_database.data = connection_settings.get('prod_database', PROD_DEFAULTS['database'])
    form.prod_token.data = connection_settings.get('prod_token', PROD_DEFAULTS['token'])

    # Apply user settings (float tolerance, max rows) from session if available
    user_settings = session.get('user_settings', {})
    form.float_tolerance.data = user_settings.get('float_tolerance', COMPARISON_DEFAULTS['float_tolerance'])
    form.max_rows_limit.data = user_settings.get('max_rows_limit', SAMPLING_CONFIG['max_rows_for_comparison'])

    # Get saved table pairs (if any)
    saved_table_pairs = last_data.get('table_pairs', [])
    available_tables = load_available_tables()

    return render_template(
        'compare.html',
        form=form,
        available_tables=available_tables,
        saved_table_pairs=saved_table_pairs,
        is_first_time=is_first_time,
        user_settings=user_settings
    )


@app.route('/api/tables')
def get_tables():
    """Get all available tables."""
    tables = load_available_tables()
    return jsonify([{
        'table_name': table[0],
        'display_name': table[1],
        'prod_primary_keys': table[2],
        'dev_primary_keys': table[3],
        'ignored_columns': table[4]
    } for table in tables])

@app.route('/api/tables', methods=['POST'])
def add_new_table():
    """Add a new table."""
    data = request.get_json()
    
    required_fields = ['table_name', 'display_name', 'prod_primary_keys', 'dev_primary_keys', 'ignored_columns']
    if not all(field in data for field in required_fields):
        return jsonify({'success': False, 'error': 'Missing required fields'}), 400
    
    success = add_table(
        data['table_name'],
        data['display_name'],
        data['prod_primary_keys'],
        data['dev_primary_keys'],
        data['ignored_columns']
    )
    
    if success:
        return jsonify({'success': True, 'message': 'Table added successfully'})
    else:
        return jsonify({'success': False, 'error': 'Table already exists'}), 400

@app.route('/api/tables/<table_name>', methods=['PUT'])
def update_existing_table(table_name):
    """Update an existing table."""
    data = request.get_json()
    
    required_fields = ['table_name', 'display_name', 'prod_primary_keys', 'dev_primary_keys', 'ignored_columns']
    if not all(field in data for field in required_fields):
        return jsonify({'success': False, 'error': 'Missing required fields'}), 400
    
    success = update_table(
        table_name,
        data['table_name'],
        data['display_name'],
        data['prod_primary_keys'],
        data['dev_primary_keys'],
        data['ignored_columns']
    )
    
    if success:
        return jsonify({'success': True, 'message': 'Table updated successfully'})
    else:
        return jsonify({'success': False, 'error': 'Table not found'}), 404


@app.route('/api/tables/<table_name>', methods=['DELETE'])
def delete_table(table_name):
    """Delete a table."""
    success = remove_table(table_name)
    
    if success:
        return jsonify({'success': True, 'message': 'Table deleted successfully'})
    else:
        return jsonify({'success': False, 'error': 'Table not found'}), 404


@app.route('/api/table-suggestions/<table_name>')
def get_table_suggestions(table_name):
    """Get suggestions for a specific table."""
    tables = load_available_tables()
    for table in tables:
        if table[0] == table_name:
            return jsonify({
                'display_name': table[1],
                'prod_primary_keys': table[2],
                'dev_primary_keys': table[3],
                'ignored_columns': table[4]
            })
    
    return jsonify({
        'display_name': table_name.split('.')[-1] if '.' in table_name else table_name,
        'prod_primary_keys': 'id',
        'dev_primary_keys': 'id',
        'ignored_columns': COMPARISON_DEFAULTS['ignored_columns']
    })


@app.route('/compare', methods=['POST'])
def compare():
    """Start comparison process."""
    form = ComparisonForm()

    # If the user filled Settings, but Compare fields are empty, use server-stored values.
    connection_settings = _get_connection_settings()
    
    # Extract table pairs from request
    table_pairs_data = extract_table_pairs_from_request()
    
    # Debug logging
    logger.info(f"Extracted table pairs: {table_pairs_data}")
    logger.info(f"Form validation: {form.validate()}")
    logger.info(f"Form errors: {form.errors}")
    
    # Validate main form (excluding table pairs for now)
    main_form_valid = True
    required_fields = ['dev_host', 'dev_port', 'dev_database', 'dev_token', 
                      'prod_host', 'prod_port', 'prod_database', 'prod_token', 'float_tolerance']
    
    for field_name in required_fields:
        field = getattr(form, field_name)
        if not field.data:
            fallback = connection_settings.get(field_name)
            if fallback:
                field.data = fallback
            else:
                main_form_valid = False
                flash(f'{field.label.text} is required', 'error')
    
    # Validate max_rows_limit if provided
    if form.max_rows_limit.data is not None and form.max_rows_limit.data < 0:
        main_form_valid = False
        flash('Max Rows Limit must be a positive number or zero (0 = no limit)', 'error')
    
    # Validate table pairs
    if not table_pairs_data:
        main_form_valid = False
        flash('At least one table pair must be configured', 'error')
    else:
        for i, pair in enumerate(table_pairs_data):
            if not pair['prod_table'] or not pair['dev_table']:
                main_form_valid = False
                flash(f'Table pair {i+1}: Both PROD and DEV tables must be selected', 'error')
            if not pair['prod_primary_keys']:
                main_form_valid = False
                flash(f'Table pair {i+1}: PROD primary keys are required', 'error')
            if not pair['dev_primary_keys']:
                main_form_valid = False
                flash(f'Table pair {i+1}: DEV primary keys are required', 'error')
    
    if main_form_valid:
        # Generate unique comparison ID
        comparison_id = str(uuid.uuid4())
        
        # Save comparison ID to session
        session['last_comparison_id'] = comparison_id
        
        # Extract form data
        form_data = {
            'dev_host': form.dev_host.data,
            'dev_port': form.dev_port.data,
            'dev_database': form.dev_database.data,
            'dev_token': form.dev_token.data,
            'prod_host': form.prod_host.data,
            'prod_port': form.prod_port.data,
            'prod_database': form.prod_database.data,
            'prod_token': form.prod_token.data,
            'float_tolerance': form.float_tolerance.data,
            'max_rows_limit': form.max_rows_limit.data  # ADICIONAR ESTA LINHA
        }

        # Save non-sensitive form data to session (do not persist credentials)
        # Normalize ignored columns to pipe-separated in session persistence
        def normalize_ignored_cols(pairs):
            normalized = []
            for p in pairs:
                ic = (p.get('ignored_columns') or '').replace('\n','|')
                normalized.append({**p, 'ignored_columns': ic})
            return normalized

        session['last_comparison'] = {
            'float_tolerance': form.float_tolerance.data,
            'max_rows_limit': form.max_rows_limit.data,  # ADICIONAR ESTA LINHA
            'table_pairs': normalize_ignored_cols(table_pairs_data)
        }
        
        # Start comparison in background thread
        thread = threading.Thread(target=run_comparison_async, args=(comparison_id, form_data, table_pairs_data))
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('results', comparison_id=comparison_id))
    
    # Validation failed - return to form with errors
    available_tables = load_available_tables()
    return render_template('compare.html', form=form, available_tables=available_tables, 
                         saved_table_pairs=table_pairs_data, user_settings=session.get('user_settings', {}))

@app.route('/results/<comparison_id>')
def results(comparison_id):
    """Show comparison results."""
    return render_template('results.html', comparison_id=comparison_id)


@app.route('/tables', methods=['GET'])
def tables_page():
    """Dedicated page for managing available tables."""
    tables = load_available_tables()
    return render_template('tables.html', available_tables=tables)


@app.route('/settings', methods=['GET', 'POST'])
def settings_page():
    """Page to manage user-level settings such as float tolerance and row limit."""
    if request.method == 'POST':
        try:
            float_tol_raw = request.form.get('float_tolerance', str(COMPARISON_DEFAULTS['float_tolerance']))
            max_rows_raw = request.form.get('max_rows_limit', str(SAMPLING_CONFIG['max_rows_for_comparison']))

            float_tolerance = float(float_tol_raw)
            max_rows_limit = int(max_rows_raw)
            if max_rows_limit < 0:
                raise ValueError('Max rows must be >= 0')

            # Save user settings
            session['user_settings'] = {
                'float_tolerance': float_tolerance,
                'max_rows_limit': max_rows_limit
            }

            # Persist connection settings locally (safe):
            # - host/warehouse/database in a git-ignored JSON file
            # - tokens in OS keyring (Windows Credential Manager)
            save_connection_settings({
                'dev_host': request.form.get('dev_host', ''),
                'dev_port': request.form.get('dev_port', ''),
                'dev_database': request.form.get('dev_database', ''),
                'dev_token': request.form.get('dev_token', ''),
                'prod_host': request.form.get('prod_host', ''),
                'prod_port': request.form.get('prod_port', ''),
                'prod_database': request.form.get('prod_database', ''),
                'prod_token': request.form.get('prod_token', ''),
            })

            flash('Settings saved. These values will be used as defaults for new comparisons.', 'info')
            return redirect(url_for('compare_page'))
        except Exception as e:
            flash(f'Invalid settings: {str(e)}', 'error')
            return redirect(url_for('settings_page'))

    # GET
    user_settings = session.get('user_settings', {})
    default_float = user_settings.get('float_tolerance', COMPARISON_DEFAULTS['float_tolerance'])
    default_max = user_settings.get('max_rows_limit', SAMPLING_CONFIG['max_rows_for_comparison'])
    saved = _get_connection_settings()
    connection_settings = {
        'dev_host': saved.get('dev_host', DEV_DEFAULTS['host']),
        'dev_port': saved.get('dev_port', DEV_DEFAULTS['port']),
        'dev_database': saved.get('dev_database', DEV_DEFAULTS['database']),
        'dev_token': saved.get('dev_token', DEV_DEFAULTS['token']),
        'prod_host': saved.get('prod_host', PROD_DEFAULTS['host']),
        'prod_port': saved.get('prod_port', PROD_DEFAULTS['port']),
        'prod_database': saved.get('prod_database', PROD_DEFAULTS['database']),
        'prod_token': saved.get('prod_token', PROD_DEFAULTS['token']),
    }
    return render_template('settings.html', default_float=default_float, default_max=default_max, connection_settings=connection_settings)


@app.route('/api/status/<comparison_id>')
def get_status(comparison_id):
    """Get comparison status via API."""
    status = comparison_status.get(comparison_id, {'status': 'not_found', 'progress': 'Comparison not found'})
    return jsonify(status)


@app.route('/api/status/latest')
def get_latest_status():
    """Get the status of the most recent comparison."""
    if 'last_comparison_id' in session:
        comparison_id = session['last_comparison_id']
        status = comparison_status.get(comparison_id, {'status': 'not_found', 'progress': 'Comparison not found'})
        status['comparison_id'] = comparison_id
        return jsonify(status)
    else:
        return jsonify({'status': 'no_comparison', 'message': 'No comparison found'})


@app.route('/api/clear-session', methods=['POST'])
def clear_session():
    """Clear saved form data from session."""
    if 'last_comparison' in session:
        session.pop('last_comparison', None)
    return jsonify({'success': True, 'message': 'Session cleared'})


@app.route('/api/clear-saved-credentials', methods=['POST'])
def clear_saved_credentials():
    """Clear persisted local connection settings (file + OS keyring tokens)."""
    clear_connection_settings()
    flash('Saved credentials cleared (file + OS keyring).', 'info')
    return redirect(url_for('settings_page'))

@app.route('/api/cancel/<comparison_id>', methods=['POST'])
def cancel_comparison(comparison_id):
    """Cancel a running comparison."""
    if comparison_id not in comparison_status:
        return jsonify({'success': False, 'error': 'Comparison not found'}), 404
    
    status = comparison_status[comparison_id]
    if status['status'] != 'running':
        return jsonify({'success': False, 'error': 'Comparison is not running'}), 400
    
    if not status.get('can_cancel', False):
        return jsonify({'success': False, 'error': 'Comparison cannot be cancelled at this time'}), 400
    
    # Mark for cancellation
    cancellation_requests[comparison_id] = True
    logger.info(f"Cancellation requested for comparison {comparison_id}")
    
    return jsonify({'success': True, 'message': 'Cancellation requested'})

@app.route('/api/results/<comparison_id>')
def get_results(comparison_id):
    """Get comparison results via API."""
    if comparison_id not in comparison_results:
        return jsonify({'error': 'Results not found'}), 404
    
    data = comparison_results[comparison_id]
    batch_result = data['result']
    
    # Convert batch result to JSON-serializable format
    result_data = {
        'is_batch': len(batch_result.results) > 1,
        'total_pairs': batch_result.total_pairs,
        'successful_comparisons': batch_result.successful_comparisons,
        'failed_comparisons': batch_result.failed_comparisons,
        'identical_tables': batch_result.identical_tables,
        'different_tables': batch_result.different_tables,
        'total_duration': batch_result.total_duration,
        'summary': batch_result.summary,
        'config': data['config'],
        'table_pairs': data.get('table_pairs', []),
        'timestamp': data['timestamp'],
        'was_cancelled': data.get('was_cancelled', False),  # ADICIONAR ESTA LINHA
        'results': []
    }
    
    # Convert individual results
    for result in batch_result.results:
        result_dict = {
            'prod_table': result.prod_table,
            'dev_table': result.dev_table,
            'display_name': result.display_name,
            'tables_identical': result.tables_identical,
            'dev_row_count': result.dev_row_count,
            'prod_row_count': result.prod_row_count,
            'dev_compared_rows': result.dev_compared_rows,
            'prod_compared_rows': result.prod_compared_rows,
            'was_limited': result.was_limited,
            'max_rows_setting': result.max_rows_setting,
            'sampling_method': result.sampling_method,
            'missing_from_dev': result.missing_from_dev[:50],
            'missing_from_prod': result.missing_from_prod[:50],
            'differing_rows': result.differing_rows[:20],
            'schema_differences': result.schema_differences,
            'ignored_columns': result.ignored_columns,
            'compared_columns': result.compared_columns,
            'comparison_duration': result.comparison_duration,
            'error_message': result.error_message,
            'prod_primary_key_columns': result.prod_primary_key_columns,
            'dev_primary_key_columns': result.dev_primary_key_columns,
            'executed_queries': result.executed_queries or {'DEV': [], 'PROD': []},
            'summary': {
                'total_missing_dev': len(result.missing_from_dev),
                'total_missing_prod': len(result.missing_from_prod),
                'total_differing': len(result.differing_rows)
            }
        }
        result_data['results'].append(result_dict)
    
    return jsonify(result_data)


@app.route('/shutdown', methods=['POST'])
def shutdown():
    """Shutdown the Flask server."""
    import os
    import signal
    
    def shutdown_server():
        # Get the current process ID
        pid = os.getpid()
        # Send SIGTERM signal to terminate the process
        os.kill(pid, signal.SIGTERM)
    
    # Schedule shutdown in a separate thread to allow response to be sent
    threading.Timer(1.0, shutdown_server).start()
    
    return jsonify({'message': 'Server shutting down...'}), 200


if __name__ == '__main__':
    # Auto-open browser once on startup.
    # When debug reloader is enabled, the app process starts twice; only open on the reloader child.
    should_open = (not APP_CONFIG.get('debug')) or (os.environ.get('WERKZEUG_RUN_MAIN') == 'true')
    if should_open:
        url = f"http://127.0.0.1:{APP_CONFIG['port']}"
        threading.Timer(0.8, lambda: webbrowser.open_new_tab(url)).start()

    app.run(
        debug=APP_CONFIG['debug'],
        host=APP_CONFIG['host'],
        port=APP_CONFIG['port']
    )