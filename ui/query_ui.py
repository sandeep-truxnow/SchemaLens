"""Query Runner UI module"""

import streamlit as st
import pandas as pd
import time
import re
from sqlalchemy import create_engine
from services.database_service import load_schema_metadata, read_sql_df


def render_query_tab():
    """Render SQL Query Runner tab"""
    st.header("📊 SQL Query Runner")
    
    # Auto-populate env_connections from current connection if needed
    if st.session_state.connected and not st.session_state.env_connections:
        current_env = st.session_state.connection_params.get('environment', 'QA')
        st.session_state.env_connections[current_env] = {
            'engine': st.session_state.engine,
            'params': st.session_state.connection_params
        }
        st.session_state.env_schemas[current_env] = st.session_state.available_schemas
    
    # Environment and schema selection
    query_env, query_schema = _render_environment_selection()
    
    # Show cache status
    _show_cache_status()
    
    # Load and display schema data
    tables, all_columns, table_info = _load_schema_data(query_env, query_schema)
    
    # Display available tables and columns
    if tables:
        _render_tables_info(tables, all_columns, table_info)
    
    # Query input and execution
    _render_query_interface(tables, all_columns, query_env, query_schema)
    
    # Display query results
    _render_query_results()


def _render_environment_selection():
    """Render environment and schema selection"""
    col1, col2 = st.columns([1, 2])
    
    with col1:
        available_envs = list(st.session_state.env_connections.keys())
        if available_envs:
            query_env = st.selectbox(
                "Environment",
                options=available_envs,
                help="Choose environment to query"
            )
        else:
            query_env = None
            st.warning("No environments connected")
    
    with col2:
        if query_env:
            env_schemas = st.session_state.env_schemas.get(query_env, [])
            query_schema = st.selectbox(
                "Schema",
                options=env_schemas,
                help=f"Choose schema from {query_env} environment"
            )
        else:
            query_schema = None
    
    return query_env, query_schema


def _show_cache_status():
    """Show metadata cache status"""
    if st.session_state.get('schema_metadata', {}):
        loaded_schemas = len(st.session_state.schema_metadata)
        total_schemas = len(st.session_state.available_schemas)
        st.info(f"💾 Cache: {loaded_schemas}/{total_schemas} schemas loaded")


def _load_schema_data(query_env, query_schema):
    """Load schema data for query interface"""
    if not (query_env and query_schema):
        return [], {}, {}
    
    # Auto-load schema metadata if not cached
    cache_key = f"{query_env}_{query_schema}"
    if cache_key not in st.session_state.get('schema_metadata', {}):
        with st.spinner(f"Loading {query_schema} from {query_env}..."):
            start_time = time.time()
            schema_data = load_schema_metadata(query_schema, st.session_state.env_connections[query_env]['params'])
            load_time = time.time() - start_time
            
            st.session_state.schema_metadata[cache_key] = schema_data
            st.success(f"✅ {query_schema} loaded from {query_env} in {load_time:.2f}s - {len(schema_data.get('tables', []))} tables found")
    
    # Use cached metadata
    schema_data = st.session_state.schema_metadata[cache_key]
    return (
        schema_data.get('tables', []),
        schema_data.get('columns', {}),
        schema_data.get('table_info', {})
    )


def _render_tables_info(tables, all_columns, table_info):
    """Render available tables and columns information"""
    with st.expander("📊 Available Tables & Columns", expanded=False):
        active_tables, unused_tables = _categorize_tables(tables, table_info)
        
        # Display active tables first
        if active_tables:
            st.markdown("**🟢 Active Tables:**")
            for table in sorted(active_tables):
                cols = all_columns.get(table, [])
                st.write(f"**{table}**: {', '.join(sorted(cols)[:5])}{'...' if len(cols) > 5 else ''}")
        
        # Display unused tables with separator
        if unused_tables:
            st.markdown("---")
            st.markdown("**🔴 Unused Tables:**")
            for table in sorted(unused_tables):
                cols = all_columns.get(table, [])
                st.write(f"**{table}**: {', '.join(sorted(cols)[:5])}{'...' if len(cols) > 5 else ''}")
    
    # Display table statistics
    if table_info:
        _render_table_statistics(tables, table_info)


def _categorize_tables(tables, table_info):
    """Categorize tables into active and unused"""
    active_tables = []
    unused_tables = []
    
    for table in tables:
        info = table_info.get(table, {})
        last_update = info.get('last_update')
        
        # Check if table is unused (same logic as ERD filtering)
        table_lower = table.lower()
        is_enum_table = any(pattern in table_lower for pattern in [
            'status', 'type', 'category', 'enum', 'lookup', 'reference', 
            'config', 'setting', 'option', 'code', 'list', 'reason',
            # below are added to include specific enum tables
            'complete_by', 'job_truck_unit', 'dispatch_order', 'attribute', 'transcription_field', 'entity_note', 'equipment_attribute'
        ])
        
        if is_enum_table or (last_update and not pd.isna(last_update) and 
                           str(last_update).lower() not in ['nat', 'none', 'null', 'unknown']):
            active_tables.append(table)
        else:
            unused_tables.append(table)
    
    return active_tables, unused_tables


def _render_table_statistics(tables, table_info):
    """Render table statistics and usage information"""
    with st.expander("🕒 Table Statistics & Usage", expanded=False):
        usage_data = []
        for table in tables:
            info = table_info.get(table, {})
            data_size = info.get('data_size', 0) or 0
            index_size = info.get('index_size', 0) or 0
            total_size = (data_size + index_size) / 1024 / 1024  # Convert to MB
            
            usage_data.append({
                'Table': table,
                'Rows': f"{info.get('rows', 0) or 0:,}",
                'Size (MB)': f"{total_size:.2f}",
                'Last Updated': str(info.get('last_update', 'Unknown'))[:19] if info.get('last_update') else 'Unknown',
                'Created': str(info.get('created', 'Unknown'))[:19] if info.get('created') else 'Unknown'
            })
        
        if usage_data:
            usage_df = pd.DataFrame(usage_data)
            st.dataframe(usage_df, use_container_width=True)


def _render_query_interface(tables, all_columns, query_env, query_schema):
    """Render query input interface"""
    st.subheader("Write your SQL query:")
    
    # Create smart help text
    help_text = _create_help_text(tables, all_columns)
    
    query = st.text_area(
        "SQL Query",
        value=st.session_state.last_query,
        height=150,
        placeholder=f"SELECT * FROM {tables[0] if tables else 'table_name'} LIMIT 10;",
        help=help_text
    )
    
    # Smart column suggestions
    _show_column_suggestions(tables, all_columns, query)
    
    # Query execution controls
    _render_query_controls(query, query_env, query_schema, tables)


def _create_help_text(tables, all_columns):
    """Create help text with table suggestions"""
    if tables and all_columns:
        suggestions = []
        for table in sorted(tables)[:3]:  # Show top 3 tables
            cols = sorted(all_columns.get(table, []))[:3]  # Show top 3 columns
            suggestions.append(f"{table}.{cols[0]}" if cols else table)
        return f"Available: {', '.join(suggestions)}... (Type table_name. to see columns)"
    else:
        return "Select schema first to see available tables and columns"


def _show_column_suggestions(tables, all_columns, query):
    """Show smart column suggestions based on query"""
    if tables and all_columns and query:
        # Look for table_name. pattern in query
        table_dot_matches = re.findall(r'\b(\w+)\.$', query.split('\n')[-1])
        if table_dot_matches:
            suggested_table = table_dot_matches[-1]
            if suggested_table in all_columns:
                cols = sorted(all_columns[suggested_table])
                st.info(f"💡 **{suggested_table}** columns: {', '.join(cols[:10])}{'...' if len(cols) > 10 else ''}")


def _render_query_controls(query, query_env, query_schema, tables):
    """Render query execution controls"""
    col1, col2 = st.columns([1, 4])
    
    with col1:
        run_query_btn = st.button("▶️ Run Query", type="primary")
    
    with col2:
        limit_results = st.number_input("Limit results", min_value=1, max_value=10000, value=100)
    
    if run_query_btn and query.strip() and query_env and query_schema and tables:
        _execute_query(query, query_env, query_schema, limit_results)


def _execute_query(query, query_env, query_schema, limit_results):
    """Execute SQL query"""
    try:
        # Add LIMIT if not present and it's a SELECT query
        query_lower = query.lower().strip()
        if query_lower.startswith('select') and 'limit' not in query_lower:
            query = f"{query.rstrip(';')} LIMIT {limit_results}"
        
        # Track execution time
        start_time = time.time()
        
        with st.spinner(f"Executing query on {query_env}..."):
            # Use connection from selected environment
            env_params = st.session_state.env_connections[query_env]['params']
            query_engine = create_engine(f"mysql+mysqlconnector://{env_params['username']}:{env_params['password']}@{env_params['host']}:{env_params['port']}/{query_schema}")
            
            with query_engine.connect() as query_conn:
                result_df = read_sql_df(query_conn, query)
        
        end_time = time.time()
        execution_time = round(end_time - start_time, 3)
        
        # Store query results in session state
        st.session_state.query_results = {
            'df': result_df,
            'query': query,
            'schema': query_schema,
            'execution_time': execution_time
        }
        st.session_state.last_query = query
        
        st.success(f"✅ Query executed on {query_env} successfully! Returned {len(result_df)} rows in {execution_time}s")
        
    except Exception as e:
        st.error(f"❌ Query failed: {str(e)}")


def _render_query_results():
    """Render query results if available"""
    if st.session_state.query_results:
        result_data = st.session_state.query_results
        result_df = result_data['df']
        
        if not result_df.empty:
            # Header with execution time
            col1, col2 = st.columns([3, 1])
            with col1:
                st.subheader("Query Results:")
            with col2:
                st.metric("Execution Time", f"{result_data.get('execution_time', 0)}s")
            
            st.dataframe(result_df, use_container_width=True)
            
            # Download option
            csv = result_df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"query_results_{result_data['schema']}.csv",
                mime="text/csv"
            )
        else:
            st.info("Query executed but returned no results.")