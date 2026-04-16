import streamlit as st
import pandas as pd
import os
from utils.databricks_utils import get_db_connection, execute_query, run_query, get_filter_options

# ---- Map display fields to columns (same as CRIAR AUDIÊNCIA) ----
field_to_column_map = {
    "Score": "ds_faixa_score",
    "Renda": "ds_renda_presumida",
    "UF": "sg_uf",
    "Evento Status": "nm_evento",
    "Gênero": "cd_sexo",
    "Idade": "fx_idade",
    "DDD": "nr_ddd",
    "Segmento Cliente": "ds_segmento",
    "Aplicativos": "ds_aplicativo:app_list",
    "Canal Principal": "ds_canal:canal_principal",
    "Preferência Horário Canal": "ds_canal:preferencia_horario",
    "Persona Principal (Segmento)": "ds_personas:segmento_principal",
    "Afinidades Persona": "ds_personas:afinidades",
}

VARIANT_ARRAY_FIELDS = ["Aplicativos", "Afinidades Persona"]
VARIANT_STRING_FIELDS = ["Canal Principal", "Preferência Horário Canal", "Persona Principal (Segmento)"]
SAVED_AUDIENCE_TABLE = os.getenv("SAVED_AUDIENCE_TABLE")


# ------- Parsing and serialization functions ---------
def parse_filter_string(query_filter):
    """Returns a list of (field_key, value) for existing filter"""
    field_pairs = []
    for part in query_filter.split(" AND "):
        if "contains" in part:
            left, right = part.split("contains(", 1)[1].split(",", 1)
            col = left.strip().replace("::string", "")
            value = right.strip().replace(")", "").replace("'", "")
        elif "=" in part:
            col, value = part.split("=", 1)
            col = col.strip().replace("::string", "")
            value = value.strip().replace("'", "")
        else:
            continue
        field = next((k for k, v in field_to_column_map.items() if v == col), col)
        field_pairs.append({'field': field, 'value': value})
    return field_pairs

def build_filter_sql_clause(clauses):
    sql_parts = []
    for clause in clauses:
        field = clause['field']
        value = clause['value']
        db_column = field_to_column_map.get(field)
        escaped_value = value.replace("'", "''")
        if field in VARIANT_ARRAY_FIELDS:
            sql_parts.append(f"contains({db_column}::string, '{escaped_value}')")
        elif field in VARIANT_STRING_FIELDS:
            sql_parts.append(f"{db_column}::string = '{escaped_value}'")
        else:
            sql_parts.append(f"`{db_column}` = '{escaped_value}'")
    return " AND ".join(sql_parts)

def render_editar_audiencia_page():
    st.title("Editar Audiência Salva")
    conn = get_db_connection()
    if not conn:
        st.error("❌ Falha na conexão com Databricks."); st.stop()
    st.info("Selecione e edite uma audiência salva.")

    if st.button("🔄 Recarregar Audiências"):
        st.cache_data.clear()

    # 1. Load saved audiences for selection
    load_query = f"SELECT audience_name, query_filter FROM {SAVED_AUDIENCE_TABLE} ORDER BY created_at DESC"
    df = run_query(conn, load_query)
    if df is None or df.empty:
        st.warning("Não existem audiências salvas para editar."); st.stop()

    audience_names = df['audience_name'].tolist()
    selected_name = st.selectbox("Selecione a audiência para editar", audience_names)
    show_row = df[df['audience_name'] == selected_name].iloc[0]

    # 2. Parse current filters
    current_clauses = parse_filter_string(show_row['query_filter'])

    # 3. Load filter options dynamically
    filter_options_from_db = get_filter_options(conn)
    if not filter_options_from_db:
        filter_options_from_db = {"UF": ["SP","RJ"]} # fallback

    # 4. Use session state to track modifiable conditions
    if 'edit_condition_rows' not in st.session_state or st.session_state.get("aud_edit_last_name") != selected_name:
        st.session_state.edit_condition_rows = {i: d.copy() for i, d in enumerate(current_clauses)}
        st.session_state.edit_next_row_id = len(current_clauses)
        st.session_state.aud_edit_last_name = selected_name

    st.markdown("#### Editar Condições da Audiência")
    # Dynamic editing rows
    row_ids_to_render = list(st.session_state.edit_condition_rows.keys())
    for i, row_id in enumerate(row_ids_to_render):
        if row_id not in st.session_state.edit_condition_rows: continue
        cond_col1, cond_col2, cond_col3, cond_col4 = st.columns([1.5,2.5,2.5,0.3])

        with cond_col1:
            logic_label = "E" if i != 0 else ""
            st.write(logic_label)
        with cond_col2:
            field_options = ["Selecione um campo"] + sorted(list(field_to_column_map.keys()))
            previous_field = st.session_state.edit_condition_rows[row_id].get('field', "Selecione um campo")
            # Field select
            select_field = st.selectbox("Campo", field_options, key=f"edit_field_{row_id}", index=field_options.index(previous_field) if previous_field in field_options else 0)
            st.session_state.edit_condition_rows[row_id]['field'] = select_field
        with cond_col3:
            value_options = ["Selecione um valor"]
            is_disabled = True
            selected_field = st.session_state.edit_condition_rows[row_id].get('field', "Selecione um campo")
            previous_value = st.session_state.edit_condition_rows[row_id].get('value', "Selecione um valor")
            if selected_field and selected_field != "Selecione um campo":
                current_values = filter_options_from_db.get(selected_field, [])
                value_options.extend(current_values)
                is_disabled = False
            try:
                value_index = value_options.index(previous_value) if previous_value in value_options else 0
            except: value_index = 0
            select_value = st.selectbox("Valor", value_options, index=value_index, key=f"edit_value_{row_id}", disabled=is_disabled)
            st.session_state.edit_condition_rows[row_id]['value'] = select_value
        with cond_col4:
            if len(st.session_state.edit_condition_rows) > 1:
                def del_row(rid):
                    st.session_state.edit_condition_rows.pop(rid)
                st.button("🗑️", key=f"del_edit_{row_id}", on_click=del_row, args=(row_id,))
        if i < len(row_ids_to_render) - 1: st.empty()

    # 5. Add new condition
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("➕ Nova condição"):
        next_id = st.session_state.edit_next_row_id
        st.session_state.edit_condition_rows[next_id] = {'field': "Selecione um campo", 'value': "Selecione um valor"}
        st.session_state.edit_next_row_id += 1

    # 6. Save
    st.markdown("<hr>", unsafe_allow_html=True)
    # Filter out invalid
    valid_clauses = [
        d for d in st.session_state.edit_condition_rows.values()
        if d['field'] != "Selecione um campo" and d['value'] != "Selecione um valor"
    ]
    if st.button("Salvar alterações", type="primary"):
        if not valid_clauses:
            st.warning("Adicione pelo menos uma condição válida para salvar.")
        else:
            new_where = build_filter_sql_clause(valid_clauses)
            update_query = f"""
            UPDATE {SAVED_AUDIENCE_TABLE}
            SET query_filter = '{new_where.replace("'", "''")}', updated_at = current_timestamp()
            WHERE audience_name = '{selected_name.replace("'", "''")}'
            """
            success = execute_query(conn, update_query)
            if success: st.success("Audiência atualizada com sucesso!")
            else: st.error("Erro ao atualizar audiência.")




