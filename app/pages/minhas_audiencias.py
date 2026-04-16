import streamlit as st
import pandas as pd
import os
from utils.databricks_utils import get_db_connection, run_query, execute_query


SAVED_AUDIENCE_TABLE = os.getenv("SAVED_AUDIENCE_TABLE")

def render_minhas_audiencias_page():
    """Renders the MINHAS AUDIÊNCIAS page with a delete button and a manual refresh button."""
    
    st.markdown("# Minhas Audiências")
    st.markdown("Veja e gerencie as audiências que você salvou.")

    conn = get_db_connection()
    if not conn:
        st.error("Não foi possível conectar ao Databricks para carregar as audiências.")
        st.stop()

    # --- A single function to clear the cache, used by both buttons ---
    def refresh_data():
        """Clears the data cache, forcing run_query to re-fetch from Databricks."""
        st.cache_data.clear()

    # --- The delete function ---
    def delete_audience(audience_name_to_delete: str):
        """Builds and runs the DELETE query, then clears the cache."""
        safe_name = audience_name_to_delete.replace("'", "''")
        delete_sql = f"DELETE FROM {SAVED_AUDIENCE_TABLE} WHERE audience_name = '{safe_name}'"
        
        success = execute_query(conn, delete_sql)
        if success:
            st.toast(f"Audiência '{audience_name_to_delete}' deletada com sucesso!")
            refresh_data() # Clear the cache
        else:
            st.error(f"Falha ao deletar a audiência '{audience_name_to_delete}'.")
    
    # --- Refresh Button at the top ---
    st.button("🔄 Atualizar Lista", on_click=refresh_data, help="Clique para recarregar a lista de audiências do banco de dados.")
    
    # Query the Delta Table to get all saved audiences.
    # This will now fetch fresh data after a delete or refresh because the cache was cleared.
    query = f"SELECT audience_name, query_filter, created_at FROM {SAVED_AUDIENCE_TABLE} ORDER BY created_at DESC"
    
    with st.spinner("Carregando audiências salvas..."):
        audiences_df = run_query(conn, query)

    # Display the results
    if not audiences_df.empty:
        st.markdown(f"**Total de audiências salvas: {len(audiences_df)}**")
        
        for index, row in audiences_df.iterrows():
            audience_name = row['audience_name']
            query_filter = row['query_filter']
            created_at = row['created_at'].strftime('%d/%m/%Y %H:%M')
            
            with st.expander(f"{audience_name} (Salvo em: {created_at})"):
                st.markdown("##### Filtros da Query:")
                st.code(query_filter, language='sql')
                st.markdown("---")
                
                # The on_click callback now calls the simplified delete function
                st.button(
                    "🗑️ Deletar Audiência",
                    key=f"delete_{audience_name}",
                    help=f"Deletar permanentemente a audiência '{audience_name}'",
                    on_click=delete_audience,
                    args=(audience_name,)
                )
    else:
        st.info("Nenhuma audiência foi salva ainda. Crie uma na página 'CRIAR AUDIÊNCIA'.")


