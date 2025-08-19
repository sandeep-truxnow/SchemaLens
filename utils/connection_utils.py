"""Connection utility functions"""

import streamlit as st
import time
from sqlalchemy import create_engine
from services.database_service import execute_reconnect_scripts, read_sql_df


def reconnect_if_needed():
    """Reconnect to database if connection is lost"""
    if not st.session_state.connected or not st.session_state.connection_params:
        return False
    
    try:
        _test_connection()
        return True
    except Exception:
        st.sidebar.info("🔄 Connection lost, attempting reconnect...")
        try:
            return _attempt_reconnect()
        except Exception as e:
            st.session_state.connected = False
            st.sidebar.error(f"❌ Reconnection failed: {e}")
            return False


def _test_connection():
    """Test current database connection"""
    with st.session_state.engine.connect() as conn:
        read_sql_df(conn, "SELECT 1")


def _create_engine(params):
    """Create database engine from parameters"""
    return create_engine(f"mysql+mysqlconnector://{params['username']}:{params['password']}@{params['host']}:{params['port']}")


def _retry_connection(engine):
    """Retry database connection with backoff"""
    for attempt in range(3):
        try:
            with engine.connect() as conn:
                read_sql_df(conn, "SELECT 1")
            return True
        except Exception:
            if attempt < 2:
                time.sleep(2)
                continue
            raise


def _attempt_reconnect():
    """Attempt to reconnect to database"""
    success, _ = execute_reconnect_scripts(st.session_state.get('environment', 'QA'))
    if not success:
        return False
    
    time.sleep(3)
    engine = _create_engine(st.session_state.connection_params)
    
    if _retry_connection(engine):
        st.session_state.engine = engine
        st.sidebar.success("🔄 Connection restored")
        return True
    return False