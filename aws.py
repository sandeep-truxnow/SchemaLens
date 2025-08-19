"""
SchemaLens - AWS Database ERD Application
Refactored version with modular architecture and complexity ≤15 per function
"""

import streamlit as st
from ui.connection_ui import render_aws_credentials_section, render_connection_section
from ui.erd_ui import render_erd_tab
from ui.query_ui import render_query_tab
from ui.impact_analysis_ui import render_impact_analysis_tab
from utils.session_utils import initialize_session_state
from tabs.environment_compare import render_environment_compare_tab

# Set page config
st.set_page_config(
    page_title="AWS DB ERD – Full Schema",
    layout="wide",
    page_icon="☁️"
)

st.title("📘 AWS Database ERD – Full Schema")
st.caption("Connect to your AWS RDS/Aurora MySQL, inspect metadata, and render a rich ERD with PK/FK, datatypes, nullability, indexes, and optional row counts.")


def main():
    """Main application entry point"""
    # Initialize session state
    initialize_session_state()
    
    # Render sidebar components
    render_aws_credentials_section()
    render_connection_section()
    
    # Render main content
    if not st.session_state.connected:
        st.info("Please connect to AWS using the sidebar to view ERD.")
    else:
        render_main_tabs()


def render_main_tabs():
    """Render main application tabs"""
    tab1, tab2, tab3, tab4 = st.tabs([
        "ERD Diagram", 
        "Query Runner", 
        "Environment Compare", 
        "Code Impact Analysis"
    ])
    
    with tab1:
        render_erd_tab()
    
    with tab2:
        render_query_tab()
    
    with tab3:
        render_environment_compare_tab()
    
    with tab4:
        render_impact_analysis_tab()


if __name__ == "__main__":
    main()