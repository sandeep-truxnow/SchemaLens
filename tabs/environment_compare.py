"""Environment Compare tab module"""

import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine
from services.database_service import load_schema_metadata, execute_reconnect_scripts, read_sql_df
from config import ENVIRONMENTS, CONNECTION_CONFIG


def render_environment_compare_tab():
    """Render Environment Comparison tab"""
    st.header("🔄 Environment Comparison")
    st.caption("Compare schemas, tables, and columns between different environments")
    
    # Environment connection setup
    st.subheader("🔗 Environment Connections")
    
    env1, env2 = _render_environment_connections()
    
    # Schema comparison section
    if env1 and env1 in st.session_state.env_connections and env2 in st.session_state.env_connections:
        _render_schema_comparison(env1, env2)
    elif env1:
        st.info(f"🔗 Connect to {env2} above to enable cross-environment comparison")
        st.info(f"📊 You can still compare schemas within {env1} environment")
    else:
        st.info("🔗 Please connect to the main environment first")


def _render_environment_connections():
    """Render environment connections section"""
    col1, col2 = st.columns(2)
    
    # Base environment (current connection)
    with col1:
        env1 = _render_base_environment()
    
    # Comparison environment (optional)
    with col2:
        env2 = _render_comparison_environment()
    
    return env1, env2


def _render_base_environment():
    """Render base environment connection"""
    st.write("**Base Environment (Current)**")
    
    if st.session_state.connected:
        current_env = st.session_state.connection_params.get('environment', 'QA')
        st.text_input("Environment", value=current_env, disabled=True, key="env1_display")
        
        # Auto-populate from current connection
        if current_env not in st.session_state.env_connections:
            st.session_state.env_connections[current_env] = {
                'engine': st.session_state.engine,
                'params': st.session_state.connection_params
            }
            st.session_state.env_schemas[current_env] = st.session_state.available_schemas
        
        st.success(f"✅ Using current {current_env} connection")
        st.info(f"💾 {len(st.session_state.available_schemas)} schemas available")
        
        return current_env
    else:
        st.warning("⚠️ No current connection. Please connect first in the main app.")
        return None


def _render_comparison_environment():
    """Render comparison environment connection"""
    st.write("**Comparison Environment (Optional)**")
    
    if st.session_state.connected:
        current_env = st.session_state.connection_params.get('environment', 'QA')
        compare_env = 'UAT' if current_env == 'QA' else 'QA'
        st.text_input("Environment", value=compare_env, disabled=True, key="env2_display")
    else:
        compare_env = 'UAT'
    
    env2 = compare_env if st.session_state.connected else 'UAT'
    
    # Show schema differences if both environments are connected
    _show_schema_differences(env2)
    
    # Handle connection/disconnection
    if env2 not in st.session_state.env_connections:
        _render_connection_interface(env2)
    else:
        _render_disconnect_interface(env2)
    
    return env2


def _show_schema_differences(env2):
    """Show schema differences between environments"""
    if st.session_state.connected and env2 in st.session_state.env_schemas:
        schemas1_set = set(st.session_state.available_schemas)
        schemas2_set = set(st.session_state.env_schemas[env2])
        
        only_in_env1 = schemas1_set - schemas2_set
        if only_in_env1:
            current_env = st.session_state.connection_params.get('environment', 'QA')
            st.error(f"🔴 Only in {current_env}: {', '.join(sorted(only_in_env1))}")


def _render_connection_interface(env2):
    """Render connection interface for second environment"""
    with st.expander(f"Connect to {env2}", expanded=False):
        st.info(f"🔧 Independent connection to {env2} on port {ENVIRONMENTS[env2]['local_port']}")
        st.warning(f"⚠️ Ensure {env2} instance is running and you have valid credentials")
        
        aws_credentials2 = st.text_area(
            "AWS Credentials",
            placeholder='export AWS_ACCESS_KEY_ID="AAAA"\nexport AWS_SECRET_ACCESS_KEY="XXXXX"\nexport AWS_SESSION_TOKEN="YYYY"',
            help="Enter your AWS credentials in export format",
            height=100,
            key="aws_creds2"
        )
        
        if st.button(f"🔗 Connect to {env2}", key="connect2"):
            _handle_second_environment_connection(env2, aws_credentials2)


def _render_disconnect_interface(env2):
    """Render disconnect interface for second environment"""
    st.success(f"✅ Connected to {env2}")
    schemas2_count = len(st.session_state.env_schemas.get(env2, []))
    st.info(f"💾 {schemas2_count} schemas available")
    
    # Show schemas only in this environment
    if st.session_state.connected:
        schemas1_set = set(st.session_state.available_schemas)
        schemas2_set = set(st.session_state.env_schemas.get(env2, []))
        
        only_in_env2 = schemas2_set - schemas1_set
        if only_in_env2:
            st.success(f"🟢 Only in {env2}: {', '.join(sorted(only_in_env2))}")
    
    if st.button(f"Disconnect {env2}", key="disconnect2"):
        del st.session_state.env_connections[env2]
        del st.session_state.env_schemas[env2]
        st.rerun()


def _handle_second_environment_connection(env2, aws_credentials2):
    """Handle connection to second environment"""
    if not aws_credentials2.strip():
        st.warning("Please enter AWS credentials in export format")
        return
    
    try:
        # Parse and set environment variables
        _parse_aws_credentials(aws_credentials2)
        
        # Establish tunnel with parsed credentials
        aws_creds = {
            'access_key': os.environ.get('AWS_ACCESS_KEY_ID'),
            'secret_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),
            'session_token': os.environ.get('AWS_SESSION_TOKEN')
        }
        
        success, result = execute_reconnect_scripts(env2, ENVIRONMENTS, aws_creds)
        
        if success:
            _establish_second_environment_connection(env2, result)
        else:
            _handle_connection_error(env2, result)
            
    except Exception as e:
        st.error(f"❌ Connection failed: {str(e)}")


def _parse_aws_credentials(aws_credentials2):
    """Parse AWS credentials from export format"""
    for line in aws_credentials2.strip().split('\n'):
        if line.strip().startswith('export '):
            line = line.strip()[7:]  # Remove 'export '
            if '=' in line:
                key, value = line.split('=', 1)
                value = value.strip('"\'')
                os.environ[key] = value


def _establish_second_environment_connection(env2, local_port):
    """Establish connection to second environment"""
    st.success(f"✅ {env2} tunnel established on port {local_port}")
    
    # Create engine with correct port
    engine2 = create_engine(f"mysql+mysqlconnector://{CONNECTION_CONFIG['username']}:{CONNECTION_CONFIG['password']}@localhost:{local_port}")
    
    with engine2.connect() as conn:
        dbs_df = read_sql_df(conn, "show databases")
        db_col = dbs_df.columns[0]
        schemas2 = [db for db in dbs_df[db_col].tolist() 
                   if db not in ('information_schema', 'performance_schema', 'mysql', 'sys')]
        
        st.session_state.env_connections[env2] = {
            'engine': engine2,
            'params': {
                'username': CONNECTION_CONFIG['username'],
                'password': CONNECTION_CONFIG['password'],
                'host': 'localhost',
                'port': local_port
            }
        }
        st.session_state.env_schemas[env2] = schemas2
        st.success(f"✅ Connected to {env2}! Found {len(schemas2)} schemas")
        st.rerun()


def _handle_connection_error(env2, error_msg):
    """Handle connection errors for second environment"""
    if "TargetNotConnected" in error_msg:
        st.error(f"❌ {env2} instance is not running or not accessible")
    elif "403" in error_msg or "Forbidden" in error_msg:
        st.error(f"❌ Invalid AWS credentials for {env2}")
    else:
        st.error(f"❌ Failed to connect to {env2}: {error_msg}")


def _render_schema_comparison(env1, env2):
    """Render schema comparison section"""
    st.subheader("🔍 Schema Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        schemas1 = st.session_state.env_schemas.get(env1, [])
        schema1 = st.selectbox(f"Schema from {env1}", schemas1, key="schema1")
    
    with col2:
        schemas2 = st.session_state.env_schemas.get(env2, [])
        # st.info(f"💾 {len(schemas2)} schemas available")
        
        # Auto-select matching schema if available
        default_index = 0
        if schema1 and schema1 in schemas2:
            default_index = schemas2.index(schema1)
        schema2 = st.selectbox(f"Schema from {env2}", schemas2, index=default_index, key="schema2")
    
    if st.button("🔍 Compare Schemas"):
        _perform_schema_comparison(env1, env2, schema1, schema2)


def _perform_schema_comparison(env1, env2, schema1, schema2):
    """Perform schema comparison between environments"""
    if not (schema1 and schema2):
        st.warning("Please select both schemas to compare")
        return
    
    # Load schema metadata from respective environments
    with st.spinner(f"Loading {schema1} from {env1}..."):
        data1 = load_schema_metadata(schema1, st.session_state.env_connections[env1]['params'])
    
    with st.spinner(f"Loading {schema2} from {env2}..."):
        data2 = load_schema_metadata(schema2, st.session_state.env_connections[env2]['params'])
    
    # Compare tables and columns
    _display_table_comparison(env1, env2, schema1, schema2, data1, data2)
    _display_column_comparison(env1, env2, data1, data2)


def _display_table_comparison(env1, env2, schema1, schema2, data1, data2):
    """Display table comparison results"""
    tables1 = set(data1.get('tables', []))
    tables2 = set(data2.get('tables', []))
    
    st.subheader("📊 Table Comparison")
    col1, col2, col3 = st.columns(3)
    
    only_in_1 = tables1 - tables2
    only_in_2 = tables2 - tables1
    common = tables1 & tables2
    
    with col1:
        st.metric(f"{env1} ({schema1})", len(tables1))
        if only_in_1:
            st.error(f"🔴 **{len(only_in_1)} tables only here:**")
            st.write(", ".join(list(only_in_1)[:5]))
            if len(only_in_1) > 5:
                st.caption(f"... and {len(only_in_1) - 5} more")
    
    with col2:
        st.success(f"✅ **Common**: {len(common)}")
    
    with col3:
        st.metric(f"{env2} ({schema2})", len(tables2))
        if only_in_2:
            st.warning(f"🟡 **{len(only_in_2)} tables only here:**")
            st.write(", ".join(list(only_in_2)[:5]))
            if len(only_in_2) > 5:
                st.caption(f"... and {len(only_in_2) - 5} more")


def _display_column_comparison(env1, env2, data1, data2):
    """Display column comparison for common tables"""
    tables1 = set(data1.get('tables', []))
    tables2 = set(data2.get('tables', []))
    common = tables1 & tables2
    
    if not common:
        return
    
    st.subheader("🔍 Column Differences in Common Tables")
    
    col_diffs = []
    tables_with_diffs = 0
    
    for table in sorted(common):
        cols1 = set(data1.get('columns', {}).get(table, []))
        cols2 = set(data2.get('columns', {}).get(table, []))
        
        if cols1 != cols2:
            tables_with_diffs += 1
            only_in_1 = cols1 - cols2
            only_in_2 = cols2 - cols1
            
            col_diffs.append({
                '🔍 Table': f"**{table}**",
                f'🔴 Only in {env1}': ', '.join(sorted(only_in_1)) if only_in_1 else '✅ None',
                f'🟡 Only in {env2}': ', '.join(sorted(only_in_2)) if only_in_2 else '✅ None',
                '✅ Common Columns': len(cols1 & cols2)
            })
    
    if col_diffs:
        st.warning(f"⚠️ Found column differences in {tables_with_diffs} out of {len(common)} common tables")
        diff_df = pd.DataFrame(col_diffs)
        st.dataframe(diff_df, use_container_width=True)
        
        # Summary metrics
        _display_comparison_metrics(tables_with_diffs, common)
    else:
        st.success("✅ All common tables have identical column structures")


def _display_comparison_metrics(tables_with_diffs, common):
    """Display comparison summary metrics"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Tables with Differences", tables_with_diffs)
    with col2:
        st.metric("Tables Identical", len(common) - tables_with_diffs)
    with col3:
        match_rate = ((len(common) - tables_with_diffs) / len(common) * 100) if common else 0
        st.metric("Match Rate", f"{match_rate:.1f}%")