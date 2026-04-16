import streamlit as st
import pandas as pd
import numpy as np
import os
import plotly.express as px
import plotly.graph_objects as go
import keplergl
from streamlit_keplergl import keplergl_static
from databricks import sql
from databricks.sdk.core import Config
from datetime import datetime
import pytz
import random
import hashlib
import re


# --- Function to fix filter quotes ---
def ensure_quotes_in_filter(filter_str):
    # For contains(..., Value) -> contains(..., 'Value')
    filter_str = re.sub(
        r"(contains\([^,]+,\s*)([^\)']+)(\))",
        lambda m: f"{m.group(1)}'{m.group(2).strip()}'{m.group(3)}",
        filter_str
    )
    # For = Value (only if not already quoted or number)
    filter_str = re.sub(
        r"([=]\s*)([^\s')]+)",
        lambda m: f"{m.group(1)}'{m.group(2).strip()}'"
        if not m.group(2).startswith("'") and not m.group(2).replace('.', '', 1).isdigit()
        else m.group(0),
        filter_str
    )
    return filter_str


from utils.databricks_utils import (
    get_db_connection,
    run_query,
    execute_query,
    get_saved_audiences,
    convert_h3_to_string
)

SAVED_AUDIENCE_TABLE = os.getenv("SAVED_AUDIENCE_TABLE")
MAIN_DATA_TABLE = os.getenv("MAIN_DATA_TABLE")
H3_COLUMN_NAME = os.getenv("H3_COLUMN_NAME")


@st.cache_data(ttl=300)
def get_audience_data_filtered(_conn, audience_filter_sql_corrected: str):
    where_clause = audience_filter_sql_corrected if audience_filter_sql_corrected else "1=1"
    query = f"SELECT nr_msisdn FROM {MAIN_DATA_TABLE} WHERE {where_clause}"
    df_result = run_query(_conn, query)
    return df_result, query


@st.cache_data(ttl=300)
def get_h3_map_data_filtered(_conn, audience_filter_sql_corrected: str):
    base_where = audience_filter_sql_corrected if audience_filter_sql_corrected else "1=1"
    where_clause = f"({base_where}) AND {H3_COLUMN_NAME} IS NOT NULL"
    query = f"""
    SELECT
        h3_toparent({H3_COLUMN_NAME}, 6) as h3_large,
        COUNT(*) as contagem_clientes
    FROM {MAIN_DATA_TABLE}
    WHERE {where_clause}
    GROUP BY 1
    """
    return run_query(_conn, query)


@st.cache_data(ttl=300)
def get_chart_data_filtered(_conn, audience_filter_sql_corrected: str):
    base_where = audience_filter_sql_corrected if audience_filter_sql_corrected else "1=1"
    where_clause = f"({base_where})"
    chart_h3_resolution = 6
    chart_dimensions = ["cd_sexo", "fx_idade", "ds_faixa_score", "ds_renda_presumida"]
    group_by_columns = ", ".join([f"`{col}`" for col in chart_dimensions])
    query = f"""
    SELECT
        {group_by_columns},
        COUNT(*) as count_per_group
    FROM {MAIN_DATA_TABLE}
    WHERE {where_clause}
    GROUP BY {group_by_columns}
    """
    return run_query(_conn, query)


def create_kepler_h3_insights_map(df_h3_agg):
    if df_h3_agg is None or df_h3_agg.empty:
        st.warning("⚠️ Nenhum dado H3 agregado para exibir no mapa.")
        return
    df_processed = df_h3_agg.copy()
    df_processed['h3'] = df_processed['h3_large'].apply(convert_h3_to_string)
    df_processed = df_processed.dropna(subset=['h3', 'contagem_clientes'])
    df_processed['contagem_clientes'] = df_processed['contagem_clientes'].astype(int)
    if len(df_processed) == 0:
        st.warning("⚠️ Nenhum hexágono H3 válido após processamento para exibir no mapa.")
        return


    # Kepler.gl configuration
    kepler_config = {
        "version": "v1",
        "config": {
            "visState": {
                "layers": [{
                    "id": "tim_h3_layer",
                    "type": "h3",
                    "config": {
                        "dataId": "tim_data",
                        "label": "Distribuição de Clientes TIM",
                        "columns": {"hex_id": "h3"},
                        "isVisible": True,
                        "visConfig": {
                            "opacity": 0.8,
                            "colorRange": {
                                "name": "Global Warming",
                                "type": "sequential", 
                                "category": "Uber",
                                "colors": ['#FFC300', '#F1920E', '#E3611C', '#C70039', '#900C3F', '#5A1846']
                            },
                            "coverage": 0.9,
                            "filled": True,
                            "enable3d": False
                        },
                        "colorField": {
                            "name": "contagem_clientes",
                            "type": "integer"
                        },
                        "colorAggregation": "sum"
                    }
                }],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "tim_data": [
                                {"name": "contagem_clientes", "format": None}
                            ]
                        },
                        "enabled": True
                    }
                }
            },
            "mapState": {
                "latitude": -14.2350,
                "longitude": -51.9253,
                "zoom": 4.5,
                "pitch": 0,
                "bearing": 0
            }
        }
    }


    # Create and display Kepler map
    try:
        map_1 = keplergl.KeplerGl(height=600, config=kepler_config)
        map_1.add_data(data=df_processed, name="h3_audience_data")
        
        map_html = map_1._repr_html_()
        st.components.v1.html(map_html, height=600)
            
    except Exception as e:
        st.error(f"❌ Erro ao criar mapa Kepler.gl: {str(e)}")
        raise e


def render_insights_page(formatted_date="Indisponível"):
    st.title("📊 Insights da Audiência Salva")
    conn = get_db_connection()
    if not conn:
        st.error("❌ Falha na conexão com Databricks.")
        st.stop()
    with st.spinner("Carregando audiências salvas..."):
        audiences_df = get_saved_audiences(conn)
    if audiences_df.empty:
        st.warning("Nenhuma audiência salva encontrada. Crie uma na página 'Criar Audiência'.")
        st.stop()
    if st.button("🔄 Recarregar Audiências"):
        st.cache_data.clear()
    audience_names = ["Selecione uma Audiência"] + audiences_df['audience_name'].tolist()
    selected_audience_name = st.selectbox(
        "**Selecione a Audiência para Análise:**",
        audience_names,
        index=0,
        key="selected_audience"
    )
    audience_data_df = pd.DataFrame()
    h3_map_data_df = pd.DataFrame()
    h3_chart_data_df = pd.DataFrame()
    audience_filter_sql_corrected = ""
    query_used_for_download = ""
    if selected_audience_name != "Selecione uma Audiência":
        selected_row = audiences_df[audiences_df['audience_name'] == selected_audience_name]
        if not selected_row.empty:
            raw_audience_filter_sql = selected_row['query_filter'].iloc[0]
            audience_filter_sql_corrected = ensure_quotes_in_filter(raw_audience_filter_sql)
            with st.spinner(f"Carregando dados para '{selected_audience_name}'..."):
                # audience_data_df, query_used_for_download = get_audience_data_filtered(conn, audience_filter_sql_corrected)
                h3_map_data_df = get_h3_map_data_filtered(conn, audience_filter_sql_corrected)
                h3_chart_data_df = get_chart_data_filtered(conn, audience_filter_sql_corrected)
        else:
            st.error("Filtro não encontrado para a audiência selecionada.")
            audience_filter_sql_corrected = ""

    col1, col2 = st.columns([1.5, 6.5])
    with col1:
        file_name_download = "tim_audience_data.csv"
        disable_download = (
            selected_audience_name == "Selecione uma Audiência"
            or audience_filter_sql_corrected == ""
        )
        csv_data_to_download = b""
        if st.button("📥 Download Dados (CSV)", disabled=disable_download, type="primary", use_container_width=True, key=f"dl_btn_{selected_audience_name}"):
            with st.spinner("Gerando CSV para download..."):
                # Only run the SQL now!
                audience_data_df, query_used_for_download = get_audience_data_filtered(conn, audience_filter_sql_corrected)
                if audience_data_df is not None and not audience_data_df.empty:
                    csv_data_to_download = audience_data_df.to_csv(index=False).encode('utf-8')
                    safe_name = "".join(c if c.isalnum() else "_" for c in selected_audience_name)

                    # Get São Paulo timezone aware datetime
                    sao_paulo_tz = pytz.timezone('America/Sao_Paulo')
                    now_sp = datetime.now(sao_paulo_tz)
                    
                    file_name_download = f"tim_audience_{safe_name}_{now_sp.strftime('%Y%m%d_%H%M')}.csv"

                    st.markdown("""
                    <style>
                    button[title="Clique aqui para baixar"], .stDownloadButton button {
                        color: white !important;
                        background-color: #1c4587 !important;
                    }
                    </style>
                    """, unsafe_allow_html=True)
                    st.download_button(
                        label="Clique aqui para baixar",
                        data=csv_data_to_download,
                        file_name=file_name_download,
                        mime="text/csv",
                        key=f"real_dl_btn_{selected_audience_name}"
                    )
                else:
                    st.error("Nenhum dado para exportar.")
    st.markdown("<br>", unsafe_allow_html=True)


    # Show the filter string after ensuring quotes are correct
    if audience_filter_sql_corrected:
        st.markdown("##### Filtro Aplicado:")
        st.code(audience_filter_sql_corrected, language='sql')
    st.markdown("---")
    st.markdown("### Métricas Principais")
    total_customers = h3_chart_data_df['count_per_group'].sum() if h3_chart_data_df is not None and 'count_per_group' in h3_chart_data_df else 0
    total_h3s = len(h3_map_data_df) if h3_map_data_df is not None else 0
    kpi_col1, kpi_col2 = st.columns(2)
    with kpi_col1:
        st.markdown(f"""<div style="..."><h4 style="...">Clientes na Audiência</h4><h2 style="...">{total_customers:,}</h2></div>""", unsafe_allow_html=True)
    with kpi_col2:
        st.markdown(f"""<div style="..."><h4 style="...">H3s Únicos (Mapa)</h4><h2 style="...">{total_h3s:,}</h2></div>""", unsafe_allow_html=True)


    st.markdown("---")
    st.markdown("### Distribuição por Dimensão")

    if h3_chart_data_df is not None and not h3_chart_data_df.empty:
        # Chart 1: Sexo
        sexo_chart = h3_chart_data_df.groupby('cd_sexo')['count_per_group'].sum().reset_index()
        fig_sexo = px.bar(sexo_chart, x='cd_sexo', y='count_per_group', text_auto=True,
                        title="Por Gênero", labels={'cd_sexo': 'Gênero', 'count_per_group': 'Total'})
        st.plotly_chart(fig_sexo, use_container_width=True)
        
        # Chart 2: Faixa Idade
        idade_chart = h3_chart_data_df.groupby('fx_idade')['count_per_group'].sum().reset_index()
        fig_idade = px.bar(idade_chart, x='fx_idade', y='count_per_group', text_auto=True,
                        title="Por Faixa de Idade", labels={'fx_idade': 'Faixa Idade', 'count_per_group': 'Total'})
        st.plotly_chart(fig_idade, use_container_width=True)
        
        # Chart 3: Faixa Score
        score_chart = h3_chart_data_df.groupby('ds_faixa_score')['count_per_group'].sum().reset_index()
        fig_score = px.bar(score_chart, x='ds_faixa_score', y='count_per_group', text_auto=True,
                        title="Por Score", labels={'ds_faixa_score': 'Score', 'count_per_group': 'Total'})
        st.plotly_chart(fig_score, use_container_width=True)
        
        # Chart 4: Faixa Renda Presumida
        renda_chart = h3_chart_data_df.groupby('ds_renda_presumida')['count_per_group'].sum().reset_index()
        fig_renda = px.bar(renda_chart, x='ds_renda_presumida', y='count_per_group', text_auto=True,
                        title="Por Renda", labels={'ds_renda_presumida': 'Renda Presumida', 'count_per_group': 'Total'})
        st.plotly_chart(fig_renda, use_container_width=True)
    else:
        st.info("Nenhum dado disponível para gráficos.")


    st.markdown("---")
    st.markdown("### 🗺️ Distribuição Geográfica (H3)")
    if selected_audience_name != "Selecione uma Audiência":
        if h3_map_data_df is not None and not h3_map_data_df.empty:
            create_kepler_h3_insights_map(h3_map_data_df)
        elif audience_data_df is not None and not audience_data_df.empty:
             st.info("Nenhum dado H3 válido encontrado.")
        elif audience_data_df is not None and audience_data_df.empty:
             st.info("Nenhum cliente encontrado com os filtros.")
    else:
        st.info("👈 Selecione uma audiência salva para visualizar o mapa.")








