import streamlit as st
import pandas as pd
import numpy as np
import os
from utils.databricks_utils import get_db_connection, execute_query, run_query, get_filter_options
import ast


MAIN_DATA_TABLE = os.getenv("MAIN_DATA_TABLE")
SAVED_AUDIENCE_TABLE = os.getenv("SAVED_AUDIENCE_TABLE")


# --- Field to Column Mapping ---
# Map user-friendly names to actual database columns OR JSON paths within VARIANT columns
field_to_column_map = {
    # --- Regular Columns ---
    "Score": "ds_faixa_score",
    "Renda": "ds_renda_presumida",
    "UF": "sg_uf",
    "Evento Status": "nm_evento",
    "Gênero": "cd_sexo",
    "Idade": "fx_idade",
    "DDD": "nr_ddd",
    "Segmento Cliente": "ds_segmento",
    "Sistema Operacional": "ds_sistema_operacional",
    "Modelo Dispositivo": "ds_modelo",
    "Nome Evento Calendário": "ds_calendario_evento",
    "Categoria Calendário": "ds_calendario_cat",
    "Subcategoria Calendário": "ds_calendario_subcat",
    "Optin": "Optin",

    # --- Fields inside VARIANT Columns ---
    "Aplicativos": "ds_aplicativo:app_list",             # VARIANT -> JSON Array 'app_list'
    "Canal Principal": "ds_canal:canal_principal",       # VARIANT -> JSON String 'canal_principal'
    "Preferência Horário Canal": "ds_canal:preferencia_horario", # VARIANT -> JSON String 'preferencia_horario'
    "Persona Principal (Segmento)": "ds_personas:segmento_principal", # VARIANT -> JSON String 'segmento_principal'
    "Afinidades Persona": "ds_personas:afinidades",      # VARIANT -> JSON Array 'afinidades'
}

# --- Identify fields corresponding to JSON ARRAYS within VARIANT columns ---
# List the user-friendly names
VARIANT_ARRAY_FIELDS = ["Aplicativos", "Afinidades Persona"]

# --- Identify fields corresponding to JSON STRINGS within VARIANT columns ---
# List the user-friendly names
VARIANT_STRING_FIELDS = ["Canal Principal", "Preferência Horário Canal", "Persona Principal (Segmento)"]


def render_criar_audiencia_page():
    """Renders the CRIAR AUDIÊNCIA page with dynamic filters, volumetria, saving, and datapoints sidebar."""

    # --- Session State Initialization (MUST BE AT THE TOP) ---
    if 'show_volumetria' not in st.session_state:
        st.session_state.show_volumetria = False
    if 'volumetria_count' not in st.session_state:
        st.session_state.volumetria_count = 0 # Initialize count
    if 'condition_rows' not in st.session_state:
        # Store row data more explicitly
        st.session_state.condition_rows = {0: {'field': "Selecione um campo", 'value': "Selecione um valor"}} # Initialize first row state
    if 'next_row_id' not in st.session_state:
        st.session_state.next_row_id = 1

    # --- Load Filter Options ---
    conn = get_db_connection()
    if not conn:
        st.error("❌ Falha na conexão com Databricks. Não é possível carregar a página.")
        st.stop() # Stop if connection fails

    # Load options dynamically using the utility function from databricks_utils.py
    with st.spinner("Carregando opções de filtro..."):
        # This function should now query your dimensional table
        filter_options_from_db = get_filter_options(conn)

    if not filter_options_from_db:
         st.warning("⚠️ Não foi possível carregar as opções de filtro do Databricks. Usando opções padrão.")
         # Provide minimal default options if loading fails
         filter_options_from_db = {"Optin": ["V", "F"]} # Example default

    # --- Sidebar Datapoints Section ---
    with st.sidebar:
        st.markdown("### Datapoints")
        st.markdown("*Consulte os segmentos e características disponíveis para uso.*")

        # Sort the filter names alphabetically for consistent order
        sorted_filter_names = sorted(filter_options_from_db.keys())

        for filter_name in sorted_filter_names:
            options_list = filter_options_from_db.get(filter_name, [])
            if options_list: # Only show expander if there are options
                with st.expander(filter_name):
                    # Display the options as a simple list using markdown
                    # Wrap each option in backticks for a code-like appearance
                    markdown_content = "\n".join([f"- `{option}`" for option in options_list])
                    st.markdown(markdown_content)

    # --- Helper Function to Build SQL Clause (Defined inside render function scope) ---
    # This function generates the correct WHERE clause string WITH single quotes
    def build_filter_sql_clause():
        """
        Builds the SQL WHERE clause string (without the 'WHERE' keyword)
        based on the current selections in st.session_state.condition_rows.
        Returns the clause string (with necessary quotes) and the count of valid conditions.
        """
        query_parts = []
        valid_conditions = 0
        for i, (row_id, row_data) in enumerate(st.session_state.condition_rows.items()):
            selected_field = row_data.get('field')
            selected_value = row_data.get('value')
            # Use widget state for logic operator if it exists (for rows > 0)
            logic = st.session_state.get(f"logic_operator_{row_id}", "E") if i > 0 else ""

            if selected_field and selected_field != "Selecione um campo" and \
               selected_value and selected_value != "Selecione um valor":

                sql_logic = "AND"
                db_column = field_to_column_map.get(selected_field)
                if not db_column: continue # Skip if field cannot be mapped

                # Escape single quotes WITHIN the value string itself (' -> '')
                # This ensures values like "St. John's" become 'St. John''s'
                sql_value_escaped = selected_value.replace("'", "''")

                # --- Type-specific SQL ---
                if selected_field in VARIANT_ARRAY_FIELDS:
                    # ✅ Adds single quotes -> contains(..., 'Value')
                    condition_sql = f"contains({db_column}::string, '{sql_value_escaped}')"
                elif selected_field in VARIANT_STRING_FIELDS:
                    # ✅ Adds single quotes -> ... = 'Value'
                    condition_sql = f"{db_column}::string = '{sql_value_escaped}'"
                elif selected_field == "Optin":
                     # ✅ No quotes for bool/int
                     sql_value_bool = "1" if selected_value == "V" else "0"
                     condition_sql = f"`{db_column}` = {sql_value_bool}"
                # Add elif for other numeric/non-string fields if necessary (e.g., DDD)
                # elif selected_field == "DDD":
                #    condition_sql = f"`{db_column}` = {sql_value_escaped}" # No quotes if numeric
                else: # Default for regular string columns
                     # ✅ Adds single quotes -> `col` = 'Value'
                     condition_sql = f"`{db_column}` = '{sql_value_escaped}'"
                # --- End Type-specific SQL ---

                if valid_conditions == 0:
                    query_parts.append(condition_sql) # First condition doesn't need AND/OR
                else:
                    query_parts.append(f"{sql_logic} {condition_sql}")
                valid_conditions += 1

        # Returns the string like: contains(..., 'Value') AND `col` = 'Other'
        return "\n".join(query_parts), valid_conditions
    # --- End Helper Function ---

    # --- Page UI ---
    st.markdown('<h2 style="color: #333; margin-bottom: 2rem;">Query Builder</h2>', unsafe_allow_html=True)
    st.markdown("**Nome da audiência** *")
    audience_name = st.text_input("Audience Name Input", placeholder="Digite o nome da audiência", key="audience_name", label_visibility="collapsed")
    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2 = st.columns([1, 8])
    with col1:
        def add_condition():
            new_id = st.session_state.next_row_id
            st.session_state.condition_rows[new_id] = {'field': "Selecione um campo", 'value': "Selecione um valor"}
            st.session_state.next_row_id += 1
        st.button("➕ Nova condição", type="secondary", on_click=add_condition)

    st.markdown("<hr>", unsafe_allow_html=True)

    # --- Dynamic Filter Rows ---
    row_ids_to_render = list(st.session_state.condition_rows.keys())
    for i, row_id in enumerate(row_ids_to_render):
        if row_id not in st.session_state.condition_rows: continue
        logic_key, field_key, value_key, delete_key = f"logic_{row_id}", f"field_{row_id}", f"value_{row_id}", f"del_{row_id}"
        cond_col1, cond_col3, cond_col5, cond_col6 = st.columns([0.5, 1.5, 1.5, 0.3])

        with cond_col1:
            if i > 0: st.selectbox("Logic", ["E"], key=logic_key, label_visibility="collapsed")
            else: st.write("")

        with cond_col3:
            field_options = ["Selecione um campo"] + sorted(list(field_to_column_map.keys()))
            def field_changed_callback(r_id): st.session_state.condition_rows[r_id]['value'] = "Selecione um valor"
            current_field = st.session_state.condition_rows[row_id].get('field', "Selecione um campo")
            try: field_index = field_options.index(current_field)
            except ValueError: field_index = 0
            selected_field = st.selectbox("Field", field_options, key=field_key, label_visibility="collapsed", index=field_index, on_change=field_changed_callback, args=(row_id,))
            st.session_state.condition_rows[row_id]['field'] = st.session_state[field_key]

        with cond_col5:
            value_options = ["Selecione um valor"]
            is_value_disabled = True
            current_value_index = 0
            stored_field_for_value = st.session_state.condition_rows[row_id].get('field')
            if stored_field_for_value and stored_field_for_value != "Selecione um campo":
                current_values = filter_options_from_db.get(stored_field_for_value, [])
                if current_values:
                    value_options.extend(current_values)
                    is_value_disabled = False
                    stored_value = st.session_state.condition_rows[row_id].get('value', "Selecione um valor")
                    if stored_value in value_options:
                        try: current_value_index = value_options.index(stored_value)
                        except ValueError: current_value_index = 0
                    else:
                        current_value_index = 0
                        st.session_state.condition_rows[row_id]['value'] = "Selecione um valor"
            def value_changed_callback(r_id, key): st.session_state.condition_rows[r_id]['value'] = st.session_state[key]
            selected_value = st.selectbox("Value", value_options, key=value_key, label_visibility="collapsed", disabled=is_value_disabled, index=current_value_index, on_change=value_changed_callback, args=(row_id, value_key))

        with cond_col6:
            if len(st.session_state.condition_rows) > 1:
                def delete_condition(r_id):
                    if r_id in st.session_state.condition_rows: del st.session_state.condition_rows[r_id]
                    keys_to_delete = [f"logic_{r_id}", f"field_{r_id}", f"value_{r_id}"]
                    for k in keys_to_delete:
                        if k in st.session_state: del st.session_state[k]
                st.button("🗑️", key=delete_key, help="Deletar", on_click=delete_condition, args=(row_id,))

        if i < len(row_ids_to_render) - 1: st.empty()

    st.markdown("<hr>", unsafe_allow_html=True)

    # --- Volumetria Display ---
    if st.session_state.show_volumetria:
        st.markdown("<br>", unsafe_allow_html=True)
        calculated_count = st.session_state.get('volumetria_count', 0)
        st.markdown(f"""
        <div style="border-top: 1px solid #ddd; padding-top: 1rem; margin-top: 1rem;">
            <h3 style="color: #666;">Volumetria Calculada</h3>
            <p style="font-size: 2.5rem; font-weight: bold; color: #333;">{calculated_count:,} <span style="font-size: 1rem; color: #666;">pessoas</span></p>
        </div>
        """, unsafe_allow_html=True)

    # --- Action Buttons ---
    st.markdown("<br>", unsafe_allow_html=True)
    action_col1, action_col2, action_col3 = st.columns([1.5, 1.5, 5])

    # --- Calcular Volumetria ---
    with action_col1:
        if st.button("Calcular volumetria"):
            where_clause_str, valid_conditions = build_filter_sql_clause() # Gets WHERE string with quotes
            if valid_conditions == 0:
                st.warning("Adicione condição válida.")
                st.session_state.show_volumetria = False; st.session_state.volumetria_count = 0
            else:
                count_query = f"SELECT COUNT(*) as total_count FROM {MAIN_DATA_TABLE} WHERE {where_clause_str}"
                print(f"DEBUG: Volumetria Query:\n{count_query}")
                with st.spinner("Calculando..."): count_result_df = run_query(conn, count_query)
                print(f"DEBUG: Volumetria Result:\n{count_result_df}")
                if count_result_df is not None and not count_result_df.empty and 'total_count' in count_result_df.columns:
                    st.session_state.volumetria_count = count_result_df['total_count'].iloc[0]
                    st.session_state.show_volumetria = True
                else:
                    st.session_state.volumetria_count = 0; st.session_state.show_volumetria = False
                    st.error("Falha ao calcular.")
                    print(f"DEBUG: Volumetria Query Failed.")
            st.rerun()

    # --- Salvar Grupo ---
    with action_col2:
        if st.button("Salvar Grupo", type="primary", disabled=not audience_name):
            if not audience_name: st.error("⚠️ Nome obrigatório.")
            else:
                # 1. Get the CORRECT WHERE clause string (includes single quotes)
                final_query_string, valid_conditions = build_filter_sql_clause()

                if valid_conditions == 0: st.warning("Adicione condição válida.")
                else:
                    safe_audience_name = audience_name.replace("'", "''") # Escape name for INSERT
                    check_query = f"SELECT COUNT(*) as count FROM {SAVED_AUDIENCE_TABLE} WHERE audience_name = '{safe_audience_name}'"
                    with st.spinner("Verificando nome..."): result_df = run_query(conn, check_query)

                    if result_df is None: st.error("Erro check.")
                    elif not result_df.empty and result_df['count'].iloc[0] > 0: st.warning(f"Nome '{audience_name}' já existe.")
                    else:
                        # 2. Escape quotes (' -> '') and newlines (\n -> space) in the WHERE string
                        #    ONLY FOR INSERTING it as a value.
                        safe_query_filter_for_insert = final_query_string.replace("'", "''").replace("\n", " ")

                        # 3. Build INSERT using the escaped string
                        insert_sql = f"""INSERT INTO {SAVED_AUDIENCE_TABLE} (audience_name, query_filter, created_at) VALUES ('{safe_audience_name}', '{safe_query_filter_for_insert}', CONVERT_TIMEZONE('America/Sao_Paulo', current_timestamp()))"""

                        # Debugging: Show both versions
                        print(f"DEBUG: Original Filter String (Correct WHERE):\n{final_query_string}")
                        print(f"DEBUG: Escaped Filter String (For INSERT):\n{safe_query_filter_for_insert}")
                        print(f"DEBUG: Full INSERT SQL:\n{insert_sql}")

                        # 4. Execute INSERT
                        with st.spinner("Salvando..."): success = execute_query(conn, insert_sql)
                        if success: st.success(f"Grupo '{audience_name}' salvo!")
                        else: st.error("Falha ao salvar.")













