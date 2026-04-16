import streamlit as st
import os

# Import page modules
from pages.home import render_home_page
from pages.criar_audiencia import render_criar_audiencia_page
from pages.minhas_audiencias import render_minhas_audiencias_page
from pages.insights import render_insights_page
from pages.editar_audiencia import render_editar_audiencia_page
from pages.chatbot import render_chatbot_page


MAIN_DATA_TABLE = os.getenv("MAIN_DATA_TABLE")


# Page configuration
st.set_page_config(
    page_title="TIM Audience Builder",
    page_icon="🧱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for TIM branding
st.markdown("""
<style>
/* Clean white background */
.stApp {
    background: white;
}

/* Main content area background */
.main-header {
    background-color: #00249d;
    padding: 1rem 2rem;
    color: white;
    position: fixed;
    top: 0;
    left: 21rem;
    right: 0;
    width: calc(100vw - 21rem);
    z-index: 1001;
    margin: 0;
    box-sizing: border-box;
}

/* Adjust main content to account for fixed header */
.main .block-container {
    margin-top: 5rem;
}
.tim-logo {
    font-size: 2rem;
    font-weight: bold;
    color: white;
}
.nav-menu {
    display: flex;
    gap: 2rem;
    margin-top: 0.5rem;
}
.nav-item {
    color: white;
    text-decoration: none;
    padding: 0.5rem 1rem;
    border-radius: 4px;
}
.nav-item:hover {
    background-color: rgba(255,255,255,0.1);
}
.insights-title {
    font-size: 1.5rem;
    font-weight: bold;
    margin-bottom: 1rem;
}
.action-buttons {
    display: flex;
    gap: 1rem;
    margin-bottom: 2rem;
}
.demographic-section {
    background-color: white;
    padding: 2rem;
    border-radius: 8px;
    margin-bottom: 2rem;
}

/* Blue sidebar styling */
.css-1d391kg {
    background-color: #00249d;
}
.css-1d391kg .css-1v0mbdj {
    color: white;
}
.css-1d391kg .markdown-text-container {
    color: white;
}
.css-1d391kg .stMarkdown {
    color: white;
}
.css-1d391kg h1, .css-1d391kg h2, .css-1d391kg h3, .css-1d391kg h4, .css-1d391kg h5, .css-1d391kg h6 {
    color: white;
}
.css-1d391kg .stText {
    color: white;
}
.css-1d391kg .css-1cpxqw2 {
    color: white;
}
.css-1d391kg label {
    color: white !important;
}
.css-1d391kg .stSelectbox label {
    color: white !important;
}
.css-1d391kg .stMultiSelect label {
    color: white !important;
}
.css-1d391kg .stSlider label {
    color: white !important;
}
.css-1d391kg .stRadio label {
    color: white !important;
}
.css-1d391kg .stCheckbox label {
    color: white !important;
}
.css-1d391kg .stTextInput label {
    color: white !important;
}
.css-1d391kg p {
    color: white;
}
.css-1d391kg .stExpander {
    background-color: rgba(255,255,255,0.1);
    border: 1px solid rgba(255,255,255,0.2);
}
.css-1d391kg .stExpander .streamlit-expanderHeader {
    color: black;
    background-color: transparent;
}
.css-1d391kg .stExpander .streamlit-expanderContent {
    background-color: transparent;
}

/* Comprehensive sidebar styling for all Streamlit versions */
.stSidebar, .stSidebar > div, .stSidebar .block-container {
    background-color: #00249d !important;
}

/* All text elements in sidebar */
.stSidebar * {
    color: white !important;
}

/* Specific elements */
.stSidebar .stMarkdown, 
.stSidebar .stMarkdown *,
.stSidebar .markdown-text-container,
.stSidebar .markdown-text-container * {
    color: white !important;
}

/* Headers */
.stSidebar h1, .stSidebar h2, .stSidebar h3, .stSidebar h4, .stSidebar h5, .stSidebar h6 {
    color: white !important;
}

/* Paragraphs and text */
.stSidebar p, .stSidebar span, .stSidebar div {
    color: white !important;
}

/* Form labels and elements */
.stSidebar label, .stSidebar .css-1cpxqw2, .stSidebar .css-1v0mbdj {
    color: white !important;
}

/* Widget icons and arrows */
.stSidebar svg, .stSidebar svg path {
    fill: white !important;
    stroke: white !important;
}

/* Expander icons */
.stSidebar .streamlit-expanderHeader svg {
    fill: white !important;
}

/* Dropdown and select arrows */
.stSidebar .stSelectbox svg, 
.stSidebar .stMultiSelect svg {
    fill: white !important;
}

/* Radio button and checkbox icons */
.stSidebar .stRadio svg,
.stSidebar .stCheckbox svg {
    fill: white !important;
}

/* Button text and icons */
.stSidebar .stButton button {
    color: white !important;
    border-color: white !important;
}

.stSidebar .stButton svg {
    fill: white !important;
}

/* Slider components */
.stSidebar .stSlider .st-ae {
    color: white !important;
}

/* Text input components */
.stSidebar .stTextInput input {
    color: white !important;
    border-color: rgba(255,255,255,0.3) !important;
}

/* Help text and captions */
.stSidebar .help, .stSidebar .caption {
    color: rgba(255,255,255,0.8) !important;
}

/* Expandable sections styling */
.stSidebar .stExpander {
    background-color: rgba(255,255,255,0.1) !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
}

.stSidebar .stExpander .streamlit-expanderHeader {
    color: black !important;
    background-color: transparent !important;
}

.stSidebar .stExpander .streamlit-expanderContent {
    background-color: transparent !important;
}

/* Target common text elements INSIDE the sidebar expander's content */
.stSidebar [data-testid="stExpander"] div[data-testid="stExpanderDetails"] p,
.stSidebar [data-testid="stExpander"] div[data-testid="stExpanderDetails"] li,
.stSidebar [data-testid="stExpander"] div[data-testid="stExpanderDetails"] code, /* Targets text in backticks `like this` */
.stSidebar [data-testid="stExpander"] div[data-testid="stExpanderDetails"] span {
    color: black !important; /* Override the general white rule to make text black */
}

/* Hide sidebar collapse button and make sidebar fixed */
[data-testid="collapsedControl"] {
    display: none !important;
}

/* Ensure sidebar is always visible */
[data-testid="stSidebar"] {
    display: block !important;
    visibility: visible !important;
}

/* Force sidebar to always be expanded */
section[data-testid="stSidebar"] {
    width: 21rem !important;
    min-width: 21rem !important;
    max-width: 21rem !important;
    transform: none !important;
    transition: none !important;
    margin-top: 5rem !important;
    z-index: 999;
}

/* Remove any collapse animations */
section[data-testid="stSidebar"] > div {
    width: 21rem !important;
    transform: none !important;
    transition: none !important;
}

/* Download button styling to match sidebar color */
.stDownloadButton > button {
    background-color: #00249d !important;
    border-color: #00249d !important;
}
</style>
""", unsafe_allow_html=True)

# Page routing setup
if "current_page" not in st.session_state:
    st.session_state.current_page = "HOME"


# Sidebar
# Add TIM branding image to sidebar
st.sidebar.image("attached_assets/tim_logo.png", width=250)

# Load Last Modified Date
from utils.databricks_utils import (
    get_db_connection,
    get_last_modified_date,
    get_demographic_data,
    process_demographic_data
)

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

if st.session_state.current_page == "HOME":
    pass

elif st.session_state.current_page == "CRIAR AUDIÊNCIA":
    pass

elif st.session_state.current_page == "INSIGHTS":
    st.sidebar.markdown(f"**Atualização Base:** {formatted_date}")

# Navigation Header - Clean blue navigation
home_bg = 'rgba(255,255,255,0.2)' if st.session_state.current_page == 'HOME' else 'rgba(255,255,255,0.1)'
create_bg = 'rgba(255,255,255,0.2)' if st.session_state.current_page == 'CRIAR AUDIÊNCIA' else 'rgba(255,255,255,0.1)'
mine_bg = 'rgba(255,255,255,0.2)' if st.session_state.current_page == 'MINHAS AUDIÊNCIAS' else 'rgba(255,255,255,0.1)'

# Navigation buttons with TIM blue styling
nav_col1, nav_col2, nav_col3, nav_col4, nav_col5, nav_col6 = st.columns([1, 1, 1, 1, 1, 1])

# Custom CSS for navigation buttons - Exact same blue as sidebar
st.markdown("""
<style>
/* Navigation buttons styling */
div[data-testid="column"] > div > div > button {
    background-color: #00249d !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 0.75rem 1.5rem !important;
    margin: 0.25rem !important;
    font-weight: normal !important;
    width: 100% !important;
    transition: background-color 0.2s ease !important;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
}

div[data-testid="column"] > div > div > button:hover {
    background-color: #00249d !important;
    opacity: 0.9 !important;
}

/* Active button styling */
div[data-testid="column"] > div > div > button[kind="primaryFormSubmit"] {
    background-color: #00249d !important;
    opacity: 1 !important;
    box-shadow: 0 2px 6px rgba(0,0,0,0.2) !important;
    font-weight: bold !important;
}
</style>
""", unsafe_allow_html=True)

with nav_col1:
    if st.button("HOME", key="nav_home", type="primary" if st.session_state.current_page == "HOME" else "secondary"):
        st.session_state.current_page = "HOME"
        st.rerun()

with nav_col2:
    if st.button("CRIAR AUDIÊNCIA", key="nav_create", type="primary" if st.session_state.current_page == "CRIAR AUDIÊNCIA" else "secondary"):
        st.session_state.current_page = "CRIAR AUDIÊNCIA"  
        st.rerun()

with nav_col3:
    if st.button("EDITAR AUDIÊNCIA", key="nav_edit", type="primary" if st.session_state.current_page == "EDITAR AUDIÊNCIA" else "secondary"):
        st.session_state.current_page = "EDITAR AUDIÊNCIA"
        st.rerun()

with nav_col4:
    if st.button("MINHAS AUDIÊNCIAS", key="nav_mine", type="primary" if st.session_state.current_page == "MINHAS AUDIÊNCIAS" else "secondary"):
        st.session_state.current_page = "MINHAS AUDIÊNCIAS"
        st.rerun()

with nav_col5:
    if st.button("INSIGHTS", key="nav_insights", type="primary" if st.session_state.current_page == "INSIGHTS" else "secondary"):
        st.session_state.current_page = "INSIGHTS"
        st.rerun()

with nav_col6:
    if st.button("CHATBOT", key="nav_chatbot", type="primary" if st.session_state.current_page == "CHATBOT" else "secondary"):
        st.session_state.current_page = "CHATBOT"
        st.rerun()


# Page Content Based on Current Page
if st.session_state.current_page == "HOME":
    render_home_page()

elif st.session_state.current_page == "CRIAR AUDIÊNCIA":
    render_criar_audiencia_page()

elif st.session_state.current_page == "EDITAR AUDIÊNCIA":
    render_editar_audiencia_page()

elif st.session_state.current_page == "MINHAS AUDIÊNCIAS":
    render_minhas_audiencias_page()

elif st.session_state.current_page == "INSIGHTS":
    render_insights_page()

elif st.session_state.current_page == "CHATBOT":
    render_chatbot_page()



