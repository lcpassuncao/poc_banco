import streamlit as st
import os


MAIN_DATA_TABLE = os.getenv("MAIN_DATA_TABLE")
SAVED_AUDIENCE_TABLE = os.getenv("SAVED_AUDIENCE_TABLE")


def render_home_page():
    """Render the HOME page content"""
    # HOME PAGE CONTENT - Matching the updated screenshot
    st.markdown("""
    <div style="background-color: #00249d; color: white; padding: 3rem 2rem 6rem 2rem; margin: -1rem -2rem 0rem -2rem; border-radius: 16px; position: relative;">
        <h1 style="color: white; margin-bottom: 1rem; font-size: 2.5rem;">Bem-vindo(a) ao Audience Builder!</h1>
        <p style="color: white; font-size: 1.1rem; line-height: 1.6; margin-bottom: 2rem; max-width: 600px;">
            O Audience Builder é uma ferramenta digital para planejadores de mídia, permitindo criar e 
            segmentar públicos para campanhas de marketing de forma prática e rápida, usando dados 
            demográficos, localização, interesses e comportamento digital dos consumidores.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Metric Cards - positioned to overlap the blue section and extend into white
    col1, col2 = st.columns(2)

    # Load Last Modified Date
    from utils.databricks_utils import (
        get_db_connection,
        get_last_modified_date,
        get_demographic_data,
        process_demographic_data,
        run_query
    )
    import os

    # Initialize variables with default values to prevent errors
    formatted_date = "Indisponível"
    demographic_data = {}

    # Get a connection to Databricks
    conn = get_db_connection()

    try:
        if conn:
            # If connection is successful, load the data
            with st.spinner("Carregando dados..."):                
                # Get the last update date
                max_date = get_last_modified_date(conn, MAIN_DATA_TABLE)
                
                # Format the date for display
                if hasattr(max_date, 'strftime'):
                    formatted_date = max_date.strftime("%d/%m/%Y")
                else:
                    formatted_date = str(max_date)

    except Exception as e:
        st.sidebar.warning("Não foi possível carregar a data de atualização.")
        print(f"Error loading data for sidebar: {e}")


    query = f"SELECT COUNT(*) AS total FROM {SAVED_AUDIENCE_TABLE}"
    
    with st.spinner("Carregando audiências salvas..."):
        audiences_df = run_query(conn, query)

    if not audiences_df.empty:
        audience_count = int(audiences_df.iloc[0, 0])
    else:
        st.write("Nenhuma audiência encontrada.")


    with col1:
        st.markdown(f"""
        <div style="background-color: white; padding: 2rem 1.5rem; border-radius: 8px 8px 0 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); text-align: center; margin-top: -3rem; position: relative; z-index: 10; height: 80px; display: flex; align-items: center; justify-content: center;">
            <h3 style="color: #666; margin: 0; font-size: 1rem; font-weight: normal; text-align: center; width: 100%;">Última atualização</h3>
        </div>
        <div style="background-color: #f5f5f5; padding: 2.5rem 1.5rem; border-radius: 0 0 8px 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); text-align: center; margin-top: 0; height: 160px; display: flex; flex-direction: column; justify-content: center; align-items: center;">
            <h2 style="color: #333; margin: 0; font-size: 3.5rem; font-weight: bold; text-align: center; width: 100%;">{formatted_date}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div style="background-color: white; padding: 2rem 1.5rem; border-radius: 8px 8px 0 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); text-align: center; margin-top: -3rem; position: relative; z-index: 10; height: 80px; display: flex; align-items: center; justify-content: center;">
            <h3 style="color: #666; margin: 0; font-size: 1rem; font-weight: normal; text-align: center; width: 100%;">Total de audiências</h3>
        </div>
        <div style="background-color: #f5f5f5; padding: 2.5rem 1.5rem; border-radius: 0 0 8px 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); text-align: center; margin-top: 0; height: 160px; display: flex; flex-direction: column; justify-content: center; align-items: center;">
            <h2 style="color: #333; margin: 0; font-size: 3.5rem; font-weight: bold; text-align: center; width: 100%;">{audience_count}</h2>
        </div>
        """, unsafe_allow_html=True)


    



