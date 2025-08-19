"""Session state utility functions"""

import streamlit as st


def initialize_session_state():
    """Initialize all session state variables"""
    session_vars = {
        'connected': False,
        'engine': None,
        'available_schemas': [],
        'connection_params': {},
        'erd_generated': False,
        'erd_data': None,
        'query_results': None,
        'last_query': "",
        'schema_metadata': {},
        'metadata_loading': False,
        'env_connections': {},
        'env_schemas': {}
    }
    
    for var, default_value in session_vars.items():
        if var not in st.session_state:
            st.session_state[var] = default_value