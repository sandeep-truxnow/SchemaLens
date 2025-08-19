"""Connection UI module for AWS credentials and database connections"""

import streamlit as st
import os
import time
from sqlalchemy import create_engine
from services.database_service import execute_reconnect_scripts, read_sql_df
from config import ENVIRONMENTS, CONNECTION_CONFIG


def render_aws_credentials_section():
    """Render AWS credentials setup section"""
    st.sidebar.header("🔧 AWS Setup")
    with st.sidebar.expander("📋 Setup Instructions", expanded=True):
        aws_credentials = st.text_area(
            "AWS Credentials",
            placeholder='export AWS_ACCESS_KEY_ID="AAAA"\nexport AWS_SECRET_ACCESS_KEY="XXXXX"\nexport AWS_SESSION_TOKEN="YYYY"',
            help="Enter your AWS credentials in export format",
            height=100
        )
        
        if st.button("⚙️ Set AWS Credentials"):
            _set_aws_credentials(aws_credentials)


def _set_aws_credentials(aws_credentials):
    """Set AWS credentials from export format"""
    if aws_credentials.strip():
        try:
            credentials_set = []
            for line in aws_credentials.strip().split('\n'):
                if line.strip().startswith('export '):
                    line = line.strip()[7:]  # Remove 'export '
                    if '=' in line:
                        key, value = line.split('=', 1)
                        value = value.strip('"\'')
                        os.environ[key] = value
                        credentials_set.append(key)
            
            if credentials_set:
                st.success(f"✅ AWS credentials set successfully! ({', '.join(credentials_set)})")
                time.sleep(1)
            else:
                st.warning("⚠️ No valid credentials found in export format")
        except Exception as e:
            st.error(f"❌ Error setting AWS credentials: {e}")
    else:
        st.warning("⚠️ Please enter AWS credentials.")


def render_connection_section():
    """Render database connection section"""
    st.sidebar.header("🔐 Connection")
    environment = st.sidebar.selectbox("Environment", ["QA", "UAT"])
    connect_btn = st.sidebar.button("🔗 Connect to Server", type="secondary")
    
    if connect_btn:
        _handle_connection(environment)
    
    return environment


def _handle_connection(environment):
    """Handle database connection logic"""
    st.sidebar.info("🔄 Connect button clicked...")
    st.session_state.environment = environment
    
    # Use hardcoded connection config
    db_type = CONNECTION_CONFIG["db_type"]
    host = CONNECTION_CONFIG["host"]
    port = CONNECTION_CONFIG["port"]
    username = CONNECTION_CONFIG["username"]
    password = CONNECTION_CONFIG["password"]
    
    st.sidebar.info(f"🔗 Attempting connection to {environment} environment")
    
    try:
        _establish_tunnel_and_connect(environment, host, port, username, password, db_type)
    except Exception as e:
        st.sidebar.error(f"❌ Connection failed: {e}")
        st.session_state.connected = False


def _establish_tunnel_and_connect(environment, host, port, username, password, db_type):
    """Establish tunnel and database connection"""
    st.sidebar.info("🚇 Setting up tunnel first...")
    
    # Check AWS credentials
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY') 
    aws_session_token = os.environ.get('AWS_SESSION_TOKEN')
    
    if not all([aws_access_key, aws_secret_key, aws_session_token]):
        st.sidebar.error("❌ AWS credentials not set. Please set credentials first.")
        st.stop()
    
    st.sidebar.info(f"✅ AWS credentials found: {aws_access_key[:8]}...")
    success, result = execute_reconnect_scripts(environment, ENVIRONMENTS)
    
    if not success:
        _handle_tunnel_failure(result, environment)
        return
    
    local_port = result
    host = "localhost"
    st.sidebar.info("✅ Tunnel established, connecting to database...")
    
    _test_database_connection(username, password, host, local_port, environment, db_type)


def _handle_tunnel_failure(result, environment):
    """Handle tunnel establishment failure"""
    st.sidebar.error(f"❌ Tunnel failed: {result}")
    if "aws: not found" in str(result) or "SessionManagerPlugin is not found" in str(result):
        st.sidebar.warning("☁️ Streamlit Cloud doesn't support AWS SSM tunneling. Attempting direct connection...")
        try:
            rds_host = ENVIRONMENTS[environment]['host']
            rds_port = 3306  # Standard MySQL port for RDS
            st.sidebar.info(f"🔗 Attempting direct connection to {rds_host}:{rds_port}")
        except Exception as e:
            st.sidebar.error(f"❌ Direct connection setup failed: {e}")
            st.stop()
    else:
        st.sidebar.error("💡 Try running locally for full functionality.")
        st.stop()


def _test_database_connection(username, password, host, local_port, environment, db_type):
    """Test database connection and fetch schemas"""
    engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{local_port}")
    
    st.sidebar.info("🔌 Testing database connection...")
    try:
        with engine.connect() as conn:
            st.sidebar.info("✅ Database connected, fetching schemas...")
            q = "show databases"
            dbs_df = read_sql_df(conn, q)
            db_col = dbs_df.columns[0]
            available_schemas = [db for db in dbs_df[db_col].tolist() 
                               if db not in ('information_schema', 'performance_schema', 'mysql', 'sys')]
            
            _store_connection_state(engine, db_type, host, local_port, username, password, environment, available_schemas)
            
    except Exception as conn_error:
        _handle_connection_error(conn_error)


def _store_connection_state(engine, db_type, host, port, username, password, environment, available_schemas):
    """Store connection state in session"""
    st.session_state.engine = engine
    st.session_state.connected = True
    st.session_state.available_schemas = available_schemas
    st.session_state.connection_params = {
        'db_type': db_type,
        'host': host,
        'port': port,
        'username': username,
        'password': password,
        'environment': environment
    }
    st.sidebar.success(f"✅ Connected! Found {len(available_schemas)} schemas/databases.")
    
    # Initialize empty cache - load on demand
    st.session_state.schema_metadata = {}
    st.sidebar.info("💾 Metadata cache initialized - schemas will load on demand")


def _handle_connection_error(conn_error):
    """Handle database connection errors"""
    if "Connection timed out" in str(conn_error) or "Can't connect" in str(conn_error):
        st.sidebar.error("❌ Direct RDS connection failed - database not publicly accessible")
        st.sidebar.warning("🏠 This app requires local execution with AWS CLI for database access")
        st.sidebar.info("💡 Run locally: streamlit run aws.py")
    else:
        raise conn_error