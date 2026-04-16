import streamlit as st
import pandas as pd
import os
from databricks import sql
from databricks.sdk.core import Config
import numpy as np
import json
import re

# --- 1. Connection Utility ---
# This function's only job is to create and cache a database connection.

H3_COLUMN_NAME = os.getenv("H3_COLUMN_NAME")
MAIN_DATA_TABLE = os.getenv("MAIN_DATA_TABLE")
DIMENSIONAL_TABLE = os.getenv("DIMENSIONAL_TABLE")
SAVED_AUDIENCE_TABLE = os.getenv("SAVED_AUDIENCE_TABLE")

@st.cache_resource
def get_db_connection():
    """
    Creates and caches a Databricks SQL connection using native app authentication.
    Returns the connection object or None if it fails.
    """
    try:
        cfg = Config()
        connection = sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
            credentials_provider=lambda: cfg.authenticate
        )
        print("✅ Connection to Databricks successful.")
        return connection
    except Exception as e:
        st.error(f"❌ Failed to connect to Databricks: {e}")
        return None

# --- 2. Generic Query Utility ---
# This function can run ANY SQL query. It's your reusable workhorse.

@st.cache_data(ttl=600) # Cache the result of each unique query for 10 minutes
def run_query(_conn, query: str) -> pd.DataFrame:
    """
    Executes a given SQL query using the provided connection and returns a
    Pandas DataFrame. Caches the result to avoid re-running identical queries.
    """
    if _conn is None:
        st.warning("Cannot run query: No Databricks connection available.")
        return pd.DataFrame()
    
    print(f"🚀 Running query:\n{query[:200]}...") # Log the start of the query
    try:
        with _conn.cursor() as cursor:
            cursor.execute(query)
            df = cursor.fetchall_arrow().to_pandas()
            print(f"✅ Query successful, returned {len(df)} rows.")
            return df
    except Exception as e:
        st.error(f"❌ Query failed: {e}")
        return pd.DataFrame()

# --- 3. Specific Data-Fetching Helpers ---
# These functions use the generic `run_query` to get specific pieces of data.

def get_last_modified_date(_conn, table_name: str):
    """Gets the last modified date of a specific Delta Table."""
    query = f"""
    SELECT DATE(timestamp) AS lastModified
    FROM (DESCRIBE HISTORY {table_name})
    ORDER BY version DESC
    LIMIT 1;
    """
    df = run_query(_conn, query)
    if not df.empty:
        return df['lastModified'].iloc[0]
    return "N/A"

def get_demographic_data(_conn, table_name: str):
    """Fetches and aggregates the specific demographic data."""
    query = f"""
    SELECT 
        gender,
        age_group,
        income_range,
        region,
        device_type,
        COUNT(*) as count
    FROM {table_name}
    GROUP BY gender, age_group, income_range, region, device_type
    """
    return run_query(_conn, query)

# --- 4. Data Processing Utility ---
# This function's only job is to transform data. It remains the same.

def process_demographic_data(df: pd.DataFrame):
    """Processes the raw demographic data into a structured dictionary."""
    if df.empty:
        return {} # Return empty dict if there's no data to process
        
    demographic_data = {
        'gender': {}, 'age_groups': {}, 'income': {},
        'regions': {}, 'device_types': {}
    }
    total_users = df['count'].sum()

    def aggregate_group(group_name, column_name):
        group_data = df.groupby(column_name)['count'].sum()
        for item, count in group_data.items():
            demographic_data[group_name][item] = {
                'count': int(count),
                'percentage': round((count / total_users) * 100, 1) if total_users > 0 else 0
            }

    aggregate_group('gender', 'gender')
    aggregate_group('age_groups', 'age_group')
    aggregate_group('income', 'income_range')
    aggregate_group('regions', 'region')
    aggregate_group('device_types', 'device_type')
    
    return demographic_data


def execute_query(_conn, query: str) -> bool:
    """
    Executes a command (like INSERT) that doesn't return data.
    Returns True on success and False on failure.
    """
    if _conn is None:
        st.error("Cannot execute command: No Databricks connection available.")
        return False
    
    print(f"🚀 Executing command:\n{query[:200]}...")
    try:
        with _conn.cursor() as cursor:
            cursor.execute(query)
        print("✅ Command executed successfully.")
        return True
    except Exception as e:
        st.error(f"❌ Command execution failed: {e}")
        return False
    

@st.cache_data(ttl=3600)
def get_filter_options(_conn):
    """
    Gets available filter values by querying the dimensional table.
    Assumes the dimensional table has one row and VARIANT columns containing array data.
    """
    query = f"SELECT * FROM {DIMENSIONAL_TABLE} LIMIT 1"

    print(f"🚀 Fetching filter options from: {DIMENSIONAL_TABLE}")
    df_options = run_query(_conn, query)

    options = {}
    if df_options.empty:
        st.error(f"❌ Failed to load filter options from {DIMENSIONAL_TABLE}. Check table name and permissions.")
        return options

    option_row = df_options.iloc[0]

    # Map database column names to user-friendly names
    column_to_friendly_name = {
        "ds_faixa_score": "Score",
        "ds_renda_presumida": "Renda",
        "sg_uf": "UF",
        "nm_evento": "Evento",
        "cd_sexo": "Gênero",
        "fx_idade": "Idade",
        "nr_ddd": "DDD",
        "ds_segmento": "Segmento",
        "ds_sistema_operacional": "Sistema Operacional",
        "ds_modelo": "Modelo Dispositivo",
        "ds_calendario_evento": "Nome Evento Calendário",
        "ds_calendario_cat": "Categoria Calendário",
        "ds_calendario_subcat": "Subcategoria Calendário",
        "canal_principal": "Canal Principal",
        "canal_preferencia_horario": "Preferência Horario Canal",
        "persona_principal": "Persona Principal",
        "persona_afinidades": "Afinidades Persona",
        "aplicativos": "Aplicativos",
        # "OptinColumnName": "Optin"
    }

    print("Parsing options from dimensional table row (VARIANT type expected):")
    for db_col, friendly_name in column_to_friendly_name.items():
        if db_col in option_row:
            raw_value = option_row[db_col]
            print(f"  DEBUG: Processing column '{db_col}', raw value: {raw_value} (Type: {type(raw_value)})")
            parsed_list = [] # Initialize as empty list
            try:
                # Check if the value is already list-like (list, numpy array)
                if isinstance(raw_value, (list, np.ndarray)): # This line needs np
                    parsed_list = list(raw_value) # Convert numpy array to list
                # If it's a string, try parsing as JSON
                elif isinstance(raw_value, str):
                     try:
                         parsed_list_json = json.loads(raw_value)
                         if isinstance(parsed_list_json, list):
                              parsed_list = parsed_list_json
                         else:
                              print(f"  ⚠️ JSON parsed value for '{db_col}' is not a list.")
                     except json.JSONDecodeError:
                         print(f"  ⚠️ Could not parse string value for '{db_col}' as JSON list.")
                else:
                    print(f"  ⚠️ Unexpected data type for '{db_col}': {type(raw_value)}")

                # Clean and sort the list if successfully parsed
                if parsed_list:
                    options[friendly_name] = sorted([str(item).strip() for item in parsed_list if item is not None])
                    print(f"  ✅ Parsed '{friendly_name}' ({len(options[friendly_name])} items)")
                else:
                    options[friendly_name] = []

            except Exception as e:
                print(f"  ❌ Error processing column '{db_col}' for '{friendly_name}': {e} - Value: {raw_value}")
                options[friendly_name] = []
        else:
             print(f"  ❓ Column '{db_col}' not found in dimensional table.")
             options[friendly_name] = []

    if "Optin" not in options:
         options["Optin"] = ["V", "F"]
         print("  ℹ️ Added default 'Optin' options [V, F]")

    print(f"DEBUG: Final options dictionary: {options}")
    return options


def ensure_quotes_in_filter(filter_sql: str) -> str:
    """
    Parses a SQL WHERE clause string and specifically ensures the value inside
    contains(column::string, value) is enclosed in single quotes if it's not
    already quoted or numeric. Leaves other parts of the string untouched.
    """
    if not filter_sql:
        return ""

    # Helper function to add quotes if needed (remains the same)
    def quote_value_if_needed(val_str):
        val_str = val_str.strip()
        # Don't quote if numeric
        if re.fullmatch(r"-?\d+(\.\d+)?", val_str):
            return val_str
        # Don't quote if already properly quoted ('Value' or '')
        if val_str.startswith("'") and val_str.endswith("'"):
            inner_val = val_str[1:-1]
            # Use triple quotes for f-string to handle internal quotes
            return f''' '{inner_val.replace("'", "''")}' '''
        # Add quotes and escape internal quotes
        escaped_val = val_str.replace("'", "''")
        return f"'{escaped_val}'"

    corrected_lines = []
    lines = filter_sql.split('\n')

    for line in lines:
        stripped_line = line.strip()
        if not stripped_line: continue

        # --- Target only the specific contains pattern ---
        # Pattern: captures (contains(col::string, space*), (the value), )
        contains_pattern = r"(contains\(\w+::string,\s*)(.*)(\))"
        contains_match = re.search(contains_pattern, stripped_line)

        if contains_match:
            operator_part = contains_match.group(1) # e.g., "contains(ds_aplicativo:app_list::string, "
            value_part = contains_match.group(2).strip()
            closing_paren = contains_match.group(3) # ")"

            # Apply quoting only to the value part
            quoted_value = quote_value_if_needed(value_part)

            # Reconstruct the line
            corrected_line = f"{operator_part}{quoted_value}{closing_paren}"
            # Check if AND/OR needs space after correction
            match_suffix = re.search(r"('\)|\))\s*(AND|OR)", corrected_line)
            if match_suffix and match_suffix.group(1) == ')': # If parenthesis was right before AND/OR
                 corrected_line = corrected_line.replace(match_suffix.group(0), f") {match_suffix.group(2)}")
            elif match_suffix and match_suffix.group(1) == "'": # If quote was right before AND/OR
                 corrected_line = corrected_line.replace(match_suffix.group(0), f"' {match_suffix.group(2)}")

            corrected_lines.append(corrected_line)
        else:
            # If the line doesn't match the specific contains pattern,
            # assume it's already correct (like `col = 'Value'` or `col = 1`)
            # or it's an AND/OR line. Add it as is.
            corrected_lines.append(stripped_line)
        # --- End specific contains pattern handling ---

    final_sql = "\n".join(corrected_lines)

    # Simple post-processing for spacing consistency before AND/OR
    final_sql = re.sub(r"'\s+(AND|OR)", r"' \1", final_sql)
    final_sql = re.sub(r"\)\s+(AND|OR)", r") \1", final_sql)

    print(f"DEBUG (ensure_quotes v3): Input:\n{filter_sql}")
    print(f"DEBUG (ensure_quotes v3): Output:\n{final_sql}") # Check this output
    return final_sql


@st.cache_data(ttl=300)
def get_audience_data(_conn, audience_filter_sql: str):
    # Ensure quotes are present before using the filter
    where_clause = ensure_quotes_in_filter(audience_filter_sql) if audience_filter_sql else "1=1"
    query = f"SELECT * FROM {MAIN_DATA_TABLE} WHERE {where_clause}"
    df_result = run_query(_conn, query)
    return df_result, query

@st.cache_data(ttl=300)
def get_h3_data_for_map(_conn, audience_filter_sql: str):
    base_where = ensure_quotes_in_filter(audience_filter_sql) if audience_filter_sql else "1=1"
    where_clause = f"({base_where}) AND {H3_COLUMN_NAME} IS NOT NULL"
    query = f"""
    SELECT
        h3_toparent({H3_COLUMN_NAME}, 6) as h3_large,
        COUNT(*) as contagem_clientes
    FROM {MAIN_DATA_TABLE}
    WHERE {where_clause}
    GROUP BY 1
    HAVING count(*) > 1
    ORDER BY 2 DESC
    LIMIT 50000
    """
    return run_query(_conn, query)


def get_saved_audiences(_conn):
    """Fetches the list of saved audience names and their filters."""
    query = f"SELECT audience_name, query_filter FROM {SAVED_AUDIENCE_TABLE} ORDER BY created_at DESC"
    return run_query(_conn, query)


def convert_h3_to_string(h3_value):
    """Convert H3 index (usually int64 from Databricks) to hex string for Kepler.gl"""
    if pd.isna(h3_value): # Use pandas isnull check
        return None
    try:
        # Ensure it's treated as integer before formatting to hex
        return format(int(h3_value), 'x') # 'x' gives lowercase hex string
    except (ValueError, TypeError):
        # Fallback if it's already a string or another type
        return str(h3_value)


@st.cache_data(ttl=300)
def get_h3_aggregated_chart_data(_conn, audience_filter_sql_corrected: str):
    """
    Fetches data grouped by H3 and key dimensions, based on the corrected filter.
    Used specifically for creating distribution charts.
    Ensures h3_code is not NULL for meaningful grouping.
    """
    # Base WHERE clause: use the provided filter string OR '1=1' if none
    base_where = audience_filter_sql_corrected if audience_filter_sql_corrected else "1=1"

    # Combine with the necessary h3_code IS NOT NULL condition
    where_clause = f"({base_where}) AND h3_code IS NOT NULL" # Use correct h3 column name 'h3_code'

    # Define the H3 resolution for grouping charts (can be different from map)
    chart_h3_resolution = 7 # Example: Use resolution 7 for charts

    # Define dimensions to group by for charting
    chart_dimensions = [
        "cd_sexo",
        "fx_idade",
        "ds_faixa_score",
        "ds_renda_presumida",
        # Add other dimensions you want to chart by
    ]
    group_by_columns = ", ".join([f"`{col}`" for col in chart_dimensions])

    query = f"""
    SELECT
        h3_toparent(h3_code, {chart_h3_resolution}) as h3_chart_res, -- Use correct h3 column
        {group_by_columns},
        COUNT(*) as count_per_group
    FROM {MAIN_DATA_TABLE} -- Ensure MAIN_DATA_TABLE is defined
    WHERE {where_clause}
    GROUP BY h3_toparent(h3_code, {chart_h3_resolution}), {group_by_columns}
    ORDER BY h3_chart_res, {group_by_columns}
    LIMIT 20000 -- Limit to keep charting manageable
    """
    return run_query(_conn, query)




