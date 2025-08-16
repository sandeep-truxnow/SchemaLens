# streamlit_aws_db_erd_full.py

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import graphviz
from io import BytesIO
import os
import time
from datetime import datetime, timezone

# Import services
from services.database_service import load_schema_metadata, execute_reconnect_scripts, read_sql_df
from services.git_analysis_service import GitAnalysisService, CodeImpactAnalyzer
from services.erd_service import (
    fetch_columns, fetch_primary_keys, fetch_foreign_keys, 
    fetch_indexes, fetch_row_counts, build_graph
)
from config import ENVIRONMENTS, CONNECTION_CONFIG

# Set page config
st.set_page_config(
    page_title="AWS DB ERD – Full Schema",
    layout="wide",
    page_icon="☁️"
)

st.title("📘 AWS Database ERD – Full Schema")
st.caption("Connect to your AWS RDS/Aurora MySQL, inspect metadata, and render a rich ERD with PK/FK, datatypes, nullability, indexes, and optional row counts.")



def load_schema_metadata_for_env(schema, connection_params, env_name):
    """Load metadata for a schema from specific environment"""
    return load_schema_metadata(schema, connection_params)

def _test_connection():
    with st.session_state.engine.connect() as conn:
        read_sql_df(conn, "SELECT 1")

def _create_engine(params):
    return create_engine(f"mysql+mysqlconnector://{params['username']}:{params['password']}@{params['host']}:{params['port']}")

def _retry_connection(engine):
    import time
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
    import time
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

def reconnect_if_needed():
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

def display_table_impact_results(results, table_name):
    """Display table impact analysis results"""
    st.subheader(f"📊 Impact Analysis for Table: {table_name}")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Files Affected", len(results['files']))
    with col2:
        st.metric("Total References", results['total_references'])
    
    if results['files']:
        st.subheader("📁 Affected Files")
        for file_info in results['files']:
            with st.expander(f"{file_info['path']} ({file_info['count']} references)"):
                for match in file_info['matches']:
                    st.code(f"Line {match['line']}: {match['content']}")
    else:
        st.info(f"No references to table '{table_name}' found in the codebase")

def display_column_impact_results(results, table_name, column_name):
    """Display column impact analysis results"""
    st.subheader(f"📊 Impact Analysis for Column: {table_name}.{column_name}")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Files Affected", len(results['files']))
    with col2:
        st.metric("Total References", results['total_references'])
    
    if results['files']:
        st.subheader("📁 Affected Files")
        for file_info in results['files']:
            with st.expander(f"{file_info['path']} ({file_info['count']} references)"):
                for match in file_info['matches']:
                    st.code(f"Line {match['line']}: {match['content']}")
    else:
        st.info(f"No references to column '{table_name}.{column_name}' found in the codebase")

def display_unused_objects_results(results):
    """Display unused objects analysis results"""
    st.subheader("🗑️ Unused Database Objects")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Tables", results['total_tables'])
    with col2:
        st.metric("Unused Tables", len(results['unused_tables']))
    with col3:
        st.metric("Total Columns", results['total_columns'])
    with col4:
        st.metric("Unused Columns", len(results['unused_columns']))
    
    if results['unused_tables']:
        st.subheader("📋 Unused Tables")
        unused_tables_df = pd.DataFrame(results['unused_tables'], columns=['Table'])
        st.dataframe(unused_tables_df, use_container_width=True)
        
        csv = unused_tables_df.to_csv(index=False)
        st.download_button(
            "📥 Download Unused Tables CSV",
            data=csv,
            file_name="unused_tables.csv",
            mime="text/csv"
        )
    
    if results['unused_columns']:
        st.subheader("📋 Unused Columns (Sample)")
        unused_columns_df = pd.DataFrame(results['unused_columns'], columns=['Column'])
        st.dataframe(unused_columns_df, use_container_width=True)
        st.caption(f"Showing first {len(results['unused_columns'])} unused columns")



# AWS Credentials Setup Instructions
st.sidebar.header("🔧 AWS Setup")
with st.sidebar.expander("📋 Setup Instructions", expanded=True):
    aws_credentials = st.text_area(
        "AWS Credentials",
        placeholder="export AWS_ACCESS_KEY_ID=\"AAAA\"\nexport AWS_SECRET_ACCESS_KEY=\"XXXXX\"\nexport AWS_SESSION_TOKEN=\"YYYY\"",
        help="Enter your AWS credentials in export format",
        height=100
    )
    
    if st.button("⚙️ Set AWS Credentials"):
        if aws_credentials.strip():
            try:
                credentials_set = []
                for line in aws_credentials.strip().split('\n'):
                    if line.strip().startswith('export '):
                        # Parse export AWS_ACCESS_KEY_ID="value" format
                        line = line.strip()[7:]  # Remove 'export '
                        if '=' in line:
                            key, value = line.split('=', 1)
                            # Remove quotes from value
                            value = value.strip('"\'')
                            os.environ[key] = value
                            credentials_set.append(key)
                
                if credentials_set:
                    st.success(f"✅ AWS credentials set successfully! ({', '.join(credentials_set)})")
                    # Force display before rerun
                    time.sleep(1)
                else:
                    st.warning("⚠️ No valid credentials found in export format")
                # Remove rerun to keep success message visible
                # st.rerun()
            except Exception as e:
                st.error(f"❌ Error setting AWS credentials: {e}")
        else:
            st.warning("⚠️ Please enter AWS credentials.")

st.sidebar.header("🔐 Connection")
environment = st.sidebar.selectbox("Environment", ["QA", "UAT"])
connect_btn = st.sidebar.button("🔗 Connect to Server", type="secondary")

# Initialize session state
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'engine' not in st.session_state:
    st.session_state.engine = None
if 'available_schemas' not in st.session_state:
    st.session_state.available_schemas = []
if 'connection_params' not in st.session_state:
    st.session_state.connection_params = {}
if 'erd_generated' not in st.session_state:
    st.session_state.erd_generated = False
if 'erd_data' not in st.session_state:
    st.session_state.erd_data = None
if 'query_results' not in st.session_state:
    st.session_state.query_results = None
if 'last_query' not in st.session_state:
    st.session_state.last_query = ""
if 'schema_metadata' not in st.session_state:
    st.session_state.schema_metadata = {}
if 'metadata_loading' not in st.session_state:
    st.session_state.metadata_loading = False
if 'env_connections' not in st.session_state:
    st.session_state.env_connections = {}
if 'env_schemas' not in st.session_state:
    st.session_state.env_schemas = {}


# Connection logic
if connect_btn:
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
        st.sidebar.info("🚇 Setting up tunnel first...")
        # Check if AWS credentials are set
        aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY') 
        aws_session_token = os.environ.get('AWS_SESSION_TOKEN')
        
        if not all([aws_access_key, aws_secret_key, aws_session_token]):
            st.sidebar.error("❌ AWS credentials not set. Please set credentials first.")
            st.stop()
        
        st.sidebar.info(f"✅ AWS credentials found: {aws_access_key[:8]}...")
        success, result = execute_reconnect_scripts(environment, ENVIRONMENTS)
        if not success:
            st.sidebar.error(f"❌ Tunnel failed: {result}")
            if "aws: not found" in str(result):
                st.sidebar.warning("🌐 AWS CLI not available in Streamlit Cloud. Attempting direct connection...")
                # Try direct connection to RDS endpoint
                try:
                    rds_host = ENVIRONMENTS[environment]['host']
                    rds_port = 3306  # Standard MySQL port for RDS
                    st.sidebar.info(f"🔗 Attempting direct connection to {rds_host}:{rds_port}")
                    local_port = rds_port
                    host = rds_host
                except Exception as e:
                    st.sidebar.error(f"❌ Direct connection setup failed: {e}")
                    st.stop()
            else:
                st.sidebar.error("💡 Try running locally for full functionality.")
                st.stop()
        else:
            local_port = result
            host = "localhost"
        st.sidebar.info("✅ Tunnel established, connecting to database...")
        
        # Use the returned local port and host
        engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{local_port}")
        
        st.sidebar.info("🔌 Testing database connection...")
        try:
            with engine.connect() as conn:
                st.sidebar.info("✅ Database connected, fetching schemas...")
                q = "show databases"
                dbs_df = read_sql_df(conn, q)
                db_col = dbs_df.columns[0]
                st.session_state.available_schemas = [db for db in dbs_df[db_col].tolist() if db not in ('information_schema', 'performance_schema', 'mysql', 'sys')]
                
                st.session_state.engine = engine
                st.session_state.connected = True
                st.session_state.connection_params = {
                    'db_type': db_type,
                    'host': host,
                    'port': port,
                    'username': username,
                    'password': password,
                    'environment': environment
                }
                st.sidebar.success(f"✅ Connected! Found {len(st.session_state.available_schemas)} schemas/databases.")
                
                # Initialize empty cache - load on demand
                st.session_state.schema_metadata = {}
                st.sidebar.info("💾 Metadata cache initialized - schemas will load on demand")
        except Exception as conn_error:
            if "Connection timed out" in str(conn_error) or "Can't connect" in str(conn_error):
                st.sidebar.error("❌ Direct RDS connection failed - database not publicly accessible")
                st.sidebar.warning("🏠 This app requires local execution with AWS CLI for database access")
                st.sidebar.info("💡 Run locally: streamlit run aws.py")
            else:
                raise conn_error
    except Exception as e:
        st.sidebar.error(f"❌ Connection failed: {e}")
        st.session_state.connected = False

# Main content area
# Main content area

if not st.session_state.connected:
    st.info("Please connect to AWS using the sidebar to view ERD.")
else:
    tab1, tab2, tab3, tab4 = st.tabs(["ERD Diagram", "Query Runner", "Environment Compare", "Code Impact Analysis"])
    
    with tab1:
        # col1, col2 = st.columns([1, 1])
        
        st.header("📋 Database Info")
        st.write(f"**Environment:** {st.session_state.connection_params.get('environment', 'Unknown')}")
        st.write(f"**Available Schemas:** {len(st.session_state.available_schemas)}")
        
        if st.session_state.available_schemas:
            st.subheader("Schema Selection")
            # st.header("📊 Schema Selection")
            sel_schemas = st.multiselect(
                "Select schemas/databases to analyze",
                options=st.session_state.available_schemas,
                default=st.session_state.available_schemas[:3] if len(st.session_state.available_schemas) <= 3 else st.session_state.available_schemas[:1],
                help="Choose which schemas/databases to include in the ERD"
            )
            
            col1a, col1b = st.columns(2)
            with col1a:
                include_row_counts = st.checkbox("Estimate row counts", value=False)
                include_indexes = st.checkbox("Include indexes in table panels", value=True)
            with col1b:
                cluster_by_schema = st.checkbox("Cluster diagram by schema", value=True)
                show_schema_prefix = st.checkbox("Show schema prefix on table nodes", value=True)
            
            max_cols_in_node = st.slider("Max columns listed per table", 10, 200, 80)
            run_btn = st.button("🔁 Generate ERD", type="primary", disabled=not sel_schemas)
        
        # with col2:
        #     st.header("📋 Database Info")
        #     st.write(f"**Environment:** {st.session_state.connection_params.get('environment', 'Unknown')}")
        #     st.write(f"**Available Schemas:** {len(st.session_state.available_schemas)}")
            
        #     if st.session_state.available_schemas:
        #         st.subheader("Available Schemas:")
        #         for schema in st.session_state.available_schemas:
        #             st.write(f"• {schema}")
        
        if run_btn:
            if not reconnect_if_needed():
                st.error("Connection lost. Please reconnect to server.")
            else:
                try:
                    import time
                    start_time = time.time()
                    
                    with st.session_state.engine.connect() as conn:
                        st.success("✅ Generating ERD...")
                        
                        # Get connection params
                        db_type = st.session_state.connection_params['db_type']
                        username = st.session_state.connection_params['username']
                        password = st.session_state.connection_params['password']
                        host = st.session_state.connection_params['host']
                        port = st.session_state.connection_params['port']

                        # Fetch metadata for selected schemas
                        all_cols, all_pks, all_fks, all_idx, all_rc = [], [], [], [], []
                        for schema in sel_schemas:
                            schema_engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{schema}")
                            with schema_engine.connect() as schema_conn:
                                cols = fetch_columns(schema_conn, db_type, [schema])
                                pks = fetch_primary_keys(schema_conn, db_type, [schema])
                                fks = fetch_foreign_keys(schema_conn, db_type, [schema])
                                idx = fetch_indexes(schema_conn, db_type, [schema])
                                rc = fetch_row_counts(schema_conn, db_type, [schema], include_row_counts)
                                
                                # Add schema name to results
                                if not cols.empty:
                                    cols['schema'] = schema
                                    all_cols.append(cols)
                                if not pks.empty:
                                    pks['schema'] = schema
                                    all_pks.append(pks)
                                if not fks.empty:
                                    fks['child_schema'] = schema
                                    all_fks.append(fks)
                                if not idx.empty:
                                    idx['schema'] = schema
                                    all_idx.append(idx)
                                if not rc.empty:
                                    rc['schema'] = schema
                                    all_rc.append(rc)
                        
                        # Combine all results
                        cols = pd.concat(all_cols, ignore_index=True) if all_cols else pd.DataFrame()
                        pks = pd.concat(all_pks, ignore_index=True) if all_pks else pd.DataFrame()
                        fks = pd.concat(all_fks, ignore_index=True) if all_fks else pd.DataFrame()
                        idx = pd.concat(all_idx, ignore_index=True) if all_idx else pd.DataFrame()
                        rc = pd.concat(all_rc, ignore_index=True) if all_rc else pd.DataFrame()

                        # Tables list from columns
                        if cols.empty:
                            st.warning("No tables found in the selected schemas.")
                        else:
                            # Create tables DataFrame
                            actual_cols = list(cols.columns)
                            schema_col = next((col for col in actual_cols if 'schema' in col.lower()), None)
                            table_col = next((col for col in actual_cols if 'table' in col.lower()), None)
                            
                            tables = cols[[schema_col, table_col]].drop_duplicates().sort_values([schema_col, table_col]).reset_index(drop=True)
                            tables.columns = ['schema', 'table_name']
                            
                            # Filter out unused tables (no UPDATE_TIME or NaT)
                            excluded_details = []
                            table_info = {}
                            
                            # Collect table info from cached metadata
                            for schema in sel_schemas:
                                cache_key = f"{st.session_state.connection_params.get('environment', 'QA')}_{schema}"
                                if cache_key in st.session_state.get('schema_metadata', {}):
                                    schema_data = st.session_state.schema_metadata[cache_key]
                                    for table, info in schema_data.get('table_info', {}).items():
                                        table_info[(schema, table)] = info
                                else:
                                    # Load metadata if not cached
                                    schema_data = load_schema_metadata(schema, st.session_state.connection_params)
                                    st.session_state.schema_metadata[cache_key] = schema_data
                                    for table, info in schema_data.get('table_info', {}).items():
                                        table_info[(schema, table)] = info
                            
                            # Filter tables based on last update with detailed reasons
                            filtered_tables = []
                            excluded_details = []
                            
                            for _, row in tables.iterrows():
                                schema_name = row['schema']
                                table_name = row['table_name']
                                info = table_info.get((schema_name, table_name), {})
                                last_update = info.get('last_update')
                                created = info.get('created')
                                rows = info.get('rows', 0)
                                
                                # Check if table is enum/lookup table (always include these)
                                table_lower = table_name.lower()
                                is_enum_table = any(pattern in table_lower for pattern in [
                                    'status', 'type', 'category', 'enum', 'lookup', 'reference', 
                                    'config', 'setting', 'option', 'code', 'list', 'reason'
                                ])
                                
                                # Include enum tables regardless of UPDATE_TIME
                                if is_enum_table:
                                    filtered_tables.append(row)
                                # Strict filtering for non-enum tables: exclude if UPDATE_TIME is null, NaT, or missing
                                elif (last_update is None or 
                                      pd.isna(last_update) or 
                                      str(last_update).lower() in ['nat', 'none', 'null', 'unknown']):
                                    
                                    # Determine specific reason
                                    if last_update is None:
                                        reason = "No UPDATE_TIME metadata (non-enum table)"
                                    elif pd.isna(last_update):
                                        reason = "UPDATE_TIME is NaT (non-enum table)"
                                    else:
                                        reason = f"UPDATE_TIME is '{last_update}' (non-enum table)"
                                    
                                    excluded_details.append({
                                        'Table': f"{schema_name}.{table_name}",
                                        'Reason': reason,
                                        'Created': str(created)[:19] if created and not pd.isna(created) else 'Unknown',
                                        'Rows': f"{rows:,}" if rows else '0',
                                        'Last Updated': 'None'
                                    })
                                else:
                                    # Table has valid UPDATE_TIME - include in ERD
                                    filtered_tables.append(row)
                                

                            
                            if filtered_tables:
                                tables = pd.DataFrame(filtered_tables)
                            else:
                                tables = pd.DataFrame(columns=['schema', 'table_name'])
                            
                            # Show detailed exclusion information
                            if excluded_details:
                                st.warning(f"⚠️ Excluded {len(excluded_details)} unused tables from ERD")
                                with st.expander(f"View all {len(excluded_details)} excluded tables and reasons", expanded=False):
                                    excluded_df = pd.DataFrame(excluded_details)
                                    st.dataframe(excluded_df, use_container_width=True)
                                    st.caption("Tables are excluded from ERD when they have no recent update activity (UPDATE_TIME is null, NaT, or missing)")

                            # Filter other dataframes to match filtered tables
                            if not tables.empty:
                                table_pairs = [(row['schema'], row['table_name']) for _, row in tables.iterrows()]
                                
                                # Filter columns to only include active tables
                                if not cols.empty:
                                    cols_schema_col = next((col for col in cols.columns if 'schema' in col.lower()), 'schema')
                                    cols_table_col = next((col for col in cols.columns if 'table' in col.lower()), 'table_name')
                                    cols = cols[cols.apply(lambda x: (x[cols_schema_col], x[cols_table_col]) in table_pairs, axis=1)]
                                
                                # Filter primary keys
                                if not pks.empty:
                                    pk_schema_col = next((col for col in pks.columns if 'schema' in col.lower()), 'schema')
                                    pk_table_col = next((col for col in pks.columns if 'table' in col.lower()), 'table_name')
                                    pks = pks[pks.apply(lambda x: (x[pk_schema_col], x[pk_table_col]) in table_pairs, axis=1)]
                                
                                # Filter foreign keys
                                if not fks.empty:
                                    fk_child_schema_col = next((col for col in fks.columns if 'child' in col.lower() and 'schema' in col.lower()), 'child_schema')
                                    fk_child_table_col = next((col for col in fks.columns if 'child' in col.lower() and 'table' in col.lower()), 'child_table')
                                    fks = fks[fks.apply(lambda x: (x[fk_child_schema_col], x[fk_child_table_col]) in table_pairs, axis=1)]
                                
                                # Filter indexes
                                if not idx.empty:
                                    idx_schema_col = next((col for col in idx.columns if 'schema' in col.lower()), 'schema')
                                    idx_table_col = next((col for col in idx.columns if 'table' in col.lower()), 'table_name')
                                    idx = idx[idx.apply(lambda x: (x[idx_schema_col], x[idx_table_col]) in table_pairs, axis=1)]
                                
                                # Filter row counts
                                if not rc.empty:
                                    rc_schema_col = next((col for col in rc.columns if 'schema' in col.lower()), 'schema')
                                    rc_table_col = next((col for col in rc.columns if 'table' in col.lower()), 'table_name')
                                    rc = rc[rc.apply(lambda x: (x[rc_schema_col], x[rc_table_col]) in table_pairs, axis=1)]
                            
                            # Build ERD
                            dot = build_graph(
                                schema_tables=tables,
                                columns=cols,
                                pks=pks,
                                fks=fks,
                                indexes=idx,
                                rowcounts=rc,
                                cluster_by_schema=cluster_by_schema,
                                show_schema_prefix=show_schema_prefix,
                                max_cols=max_cols_in_node
                            )
                            
                            execution_time = time.time() - start_time
                            
                            # Store ERD data in session state
                            st.session_state.erd_data = {
                                'dot': dot,
                                'cols': cols,
                                'pks': pks,
                                'fks': fks,
                                'idx': idx,
                                'rc': rc,
                                'include_row_counts': include_row_counts,
                                'execution_time': execution_time
                            }
                            st.session_state.erd_generated = True
                            
                            st.success(f"✅ ERD generated successfully in {execution_time:.2f} seconds!")

                except Exception as e:
                    st.error(f"❌ ERD generation failed: {e}")
        
        # Display ERD if generated
        if st.session_state.erd_generated and st.session_state.erd_data:
            erd_data = st.session_state.erd_data
            
            # Show dataframes (expanders)
            with st.expander("📄 Columns", expanded=False):
                st.dataframe(erd_data['cols'], use_container_width=True)
            with st.expander("🔑 Primary Keys", expanded=False):
                if not erd_data['pks'].empty:
                    # Group primary keys by table to show composite keys together
                    pk_df = erd_data['pks'].copy()
                    pk_grouped = []
                    
                    # Get column names dynamically
                    schema_col = next((col for col in pk_df.columns if 'schema' in col.lower()), 'schema')
                    table_col = next((col for col in pk_df.columns if 'table' in col.lower()), 'table_name')
                    column_col = next((col for col in pk_df.columns if 'column' in col.lower()), 'column_name')
                    ordinal_col = next((col for col in pk_df.columns if 'ordinal' in col.lower()), None)
                    
                    for (schema, table), group in pk_df.groupby([schema_col, table_col]):
                        if ordinal_col:
                            columns = group.sort_values(ordinal_col)[column_col].tolist()
                        else:
                            columns = group[column_col].tolist()
                        pk_grouped.append({
                            'Schema': schema,
                            'Table': table,
                            'Primary Key Columns': ', '.join(columns),
                            'Type': 'Composite' if len(columns) > 1 else 'Single'
                        })
                    
                    grouped_df = pd.DataFrame(pk_grouped)
                    st.dataframe(grouped_df, use_container_width=True)
                else:
                    st.info("No primary keys found")
            with st.expander("🔗 Foreign Keys", expanded=False):
                if not erd_data['fks'].empty:
                    # Group foreign keys by constraint name to show multi-column FKs together
                    fk_df = erd_data['fks'].copy()
                    fk_grouped = []
                    
                    constraint_col = next((col for col in fk_df.columns if 'constraint' in col.lower()), 'constraint_name')
                    child_col_col = next((col for col in fk_df.columns if 'child' in col.lower() and 'column' in col.lower()), 'child_column')
                    parent_col_col = next((col for col in fk_df.columns if 'parent' in col.lower() and 'column' in col.lower()), 'parent_column')
                    
                    for constraint, group in fk_df.groupby(constraint_col):
                        first_row = group.iloc[0]
                        child_cols = group[child_col_col].tolist() if child_col_col in group.columns else []
                        parent_cols = group[parent_col_col].tolist() if parent_col_col in group.columns else []
                        
                        fk_grouped.append({
                            'Child Table': f"{first_row.get('child_schema', '')}.{first_row.get('child_table', '')}",
                            'Child Columns': ', '.join(child_cols),
                            'Parent Table': f"{first_row.get('parent_schema', '')}.{first_row.get('parent_table', '')}",
                            'Parent Columns': ', '.join(parent_cols),
                            'Constraint': constraint
                        })
                    
                    grouped_fk_df = pd.DataFrame(fk_grouped)
                    st.dataframe(grouped_fk_df, use_container_width=True)
                else:
                    st.info("No foreign keys found")
            with st.expander("📚 Indexes", expanded=False):
                if not erd_data['idx'].empty:
                    st.dataframe(erd_data['idx'], use_container_width=True)
                else:
                    st.info("No indexes found")
            if erd_data['include_row_counts']:
                with st.expander("🔢 Row Count Estimates", expanded=False):
                    if not erd_data['rc'].empty:
                        st.dataframe(erd_data['rc'], use_container_width=True)
                    else:
                        st.info("No row count data available")

            # Display ERD with execution time and zoom controls
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.subheader("🗺️ Entity Relationship Diagram")
            with col2:
                if 'execution_time' in erd_data:
                    st.metric("Generation Time", f"{erd_data['execution_time']:.2f}s")
            with col3:
                zoom_level = st.selectbox(
                    "Zoom Level",
                    options=["50%", "75%", "100%", "125%", "150%", "200%"],
                    index=2,  # Default to 100%
                    key="erd_zoom"
                )
            
            # Apply zoom styling
            zoom_value = int(zoom_level.replace('%', '')) / 100
            st.markdown(f"""
            <div style="transform: scale({zoom_value}); transform-origin: top left; width: {100/zoom_value}%; height: {100/zoom_value}%;">
            """, unsafe_allow_html=True)
            
            st.graphviz_chart(erd_data['dot'])
            
            st.markdown("</div>", unsafe_allow_html=True)

            # Export DOT / PNG
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="📥 Download .dot",
                    data=erd_data['dot'].source.encode("utf-8"),
                    file_name="erd.dot",
                    mime="text/vnd.graphviz",
                )
            with col2:
                try:
                    png_bytes = erd_data['dot'].pipe(format="png")
                    st.download_button(
                        label="🖼️ Download PNG",
                        data=png_bytes,
                        file_name="erd.png",
                        mime="image/png",
                    )
                except Exception:
                    st.info("PNG export requires Graphviz binaries on server.")
    
    with tab2:
        st.header("📊 SQL Query Runner")
        
        # Environment and schema selection for queries
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
        
        # Show cache status
        if st.session_state.get('schema_metadata', {}):
            loaded_schemas = len(st.session_state.schema_metadata)
            total_schemas = len(st.session_state.available_schemas)
            st.info(f"💾 Cache: {loaded_schemas}/{total_schemas} schemas loaded")
        
        if query_env and query_schema:
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
            tables = schema_data.get('tables', [])
            all_columns = schema_data.get('columns', {})
            table_info = schema_data.get('table_info', {})
            
            # Display available tables and columns
            if tables:
                with st.expander("📊 Available Tables & Columns", expanded=False):
                    # Separate active and unused tables
                    active_tables = []
                    unused_tables = []
                    
                    for table in tables:
                        info = table_info.get(table, {})
                        last_update = info.get('last_update')
                        
                        # Check if table is unused (same logic as ERD filtering)
                        table_lower = table.lower()
                        is_enum_table = any(pattern in table_lower for pattern in [
                            'status', 'type', 'category', 'enum', 'lookup', 'reference', 
                            'config', 'setting', 'option', 'code', 'list', 'reason'
                        ])
                        
                        if is_enum_table or (last_update and not pd.isna(last_update) and 
                                           str(last_update).lower() not in ['nat', 'none', 'null', 'unknown']):
                            active_tables.append(table)
                        else:
                            unused_tables.append(table)
                    
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
                
                # Display table usage info
                if table_info:
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
            else:
                st.warning(f"No tables found in {query_schema}")
        else:
            tables, all_columns, table_info = [], {}, {}

        # Query input with smart suggestions
        st.subheader("Write your SQL query:")
        
        # Create smart help text with table.column suggestions
        if tables and all_columns:
            suggestions = []
            for table in sorted(tables)[:3]:  # Show top 3 tables
                cols = sorted(all_columns.get(table, []))[:3]  # Show top 3 columns
                suggestions.append(f"{table}.{cols[0]}" if cols else table)
            help_text = f"Available: {', '.join(suggestions)}... (Type table_name. to see columns)"
        else:
            help_text = "Select schema first to see available tables and columns"
        
        query = st.text_area(
            "SQL Query",
            value=st.session_state.last_query,
            height=150,
            placeholder=f"SELECT * FROM {tables[0] if tables else 'table_name'} LIMIT 10;",
            help=help_text
        )
        
        # Smart column suggestions display
        if tables and all_columns and query:
            # Look for table_name. pattern in query
            import re
            table_dot_matches = re.findall(r'\b(\w+)\.$', query.split('\n')[-1])
            if table_dot_matches:
                suggested_table = table_dot_matches[-1]
                if suggested_table in all_columns:
                    cols = sorted(all_columns[suggested_table])
                    st.info(f"💡 **{suggested_table}** columns: {', '.join(cols[:10])}{'...' if len(cols) > 10 else ''}")
        
        col1, col2 = st.columns([1, 4])
        with col1:
            run_query_btn = st.button("▶️ Run Query", type="primary")
        with col2:
            limit_results = st.number_input("Limit results", min_value=1, max_value=10000, value=100)
            
        if run_query_btn and query.strip() and query_env and query_schema and tables:
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
            
        # Display query results if available
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
    
    with tab3:
        st.header("🔄 Environment Comparison")
        st.caption("Compare schemas, tables, and columns between different environments")
        
        # Environment connection setup
        st.subheader("🔗 Environment Connections")
        
        col1, col2 = st.columns(2)
        
        # Base environment (current connection)
        with col1:
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
                env1 = current_env
            else:
                st.warning("⚠️ No current connection. Please connect first in the main app.")
                env1 = None
        
        # Comparison environment (optional)
        with col2:
            st.write("**Comparison Environment (Optional)**")
            if st.session_state.connected:
                current_env = st.session_state.connection_params.get('environment', 'QA')
                compare_env = 'UAT' if current_env == 'QA' else 'QA'
                st.text_input("Environment", value=compare_env, disabled=True, key="env2_display")
                env2 = compare_env
            else:
                env2 = 'UAT'
            
            if env2 not in st.session_state.env_connections:
                with st.expander(f"Connect to {env2}", expanded=False):
                    st.info(f"🔧 Independent connection to {env2} on port {ENVIRONMENTS[env2]['local_port']}")
                    st.warning(f"⚠️ Ensure {env2} instance is running and you have valid credentials")
                    aws_credentials2 = st.text_area(
                        "AWS Credentials",
                        placeholder="export AWS_ACCESS_KEY_ID=\"AAAA\"\nexport AWS_SECRET_ACCESS_KEY=\"XXXXX\"\nexport AWS_SESSION_TOKEN=\"YYYY\"",
                        help="Enter your AWS credentials in export format",
                        height=100,
                        key="aws_creds2"
                    )
                    
                    if st.button(f"🔗 Connect to {env2}", key="connect2"):
                        if aws_credentials2.strip():
                            try:
                                # Parse and set environment variables
                                for line in aws_credentials2.strip().split('\n'):
                                    if line.strip().startswith('export '):
                                        line = line.strip()[7:]  # Remove 'export '
                                        if '=' in line:
                                            key, value = line.split('=', 1)
                                            value = value.strip('"\'')
                                            os.environ[key] = value
                                
                                # Establish tunnel with parsed credentials
                                aws_creds = {
                                    'access_key': os.environ.get('AWS_ACCESS_KEY_ID'),
                                    'secret_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),
                                    'session_token': os.environ.get('AWS_SESSION_TOKEN')
                                }
                                success, result = execute_reconnect_scripts(env2, ENVIRONMENTS, aws_creds)
                                if success:
                                    local_port = result
                                    st.success(f"✅ {env2} tunnel established on port {local_port}")
                                    # Create engine with correct port
                                    engine2 = create_engine(f"mysql+mysqlconnector://{CONNECTION_CONFIG['username']}:{CONNECTION_CONFIG['password']}@localhost:{local_port}")
                                    
                                    with engine2.connect() as conn:
                                        dbs_df = read_sql_df(conn, "show databases")
                                        db_col = dbs_df.columns[0]
                                        schemas2 = [db for db in dbs_df[db_col].tolist() if db not in ('information_schema', 'performance_schema', 'mysql', 'sys')]
                                        
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
                                else:
                                    error_msg = result
                                    if "TargetNotConnected" in error_msg:
                                        st.error(f"❌ {env2} instance is not running or not accessible")
                                    elif "403" in error_msg or "Forbidden" in error_msg:
                                        st.error(f"❌ Invalid AWS credentials for {env2}")
                                    else:
                                        st.error(f"❌ Failed to connect to {env2}: {error_msg}")
                            except Exception as e:
                                st.error(f"❌ Connection failed: {str(e)}")
                        else:
                            st.warning("Please enter AWS credentials in export format")
            else:
                st.success(f"✅ Connected to {env2}")
                if st.button(f"Disconnect {env2}", key="disconnect2"):
                    del st.session_state.env_connections[env2]
                    del st.session_state.env_schemas[env2]
                    st.rerun()
        
        # Schema comparison section
        if env1 and env1 in st.session_state.env_connections and env2 in st.session_state.env_connections:
            st.subheader("🔍 Schema Comparison")
            
            col1, col2 = st.columns(2)
            with col1:
                schemas1 = st.session_state.env_schemas.get(env1, [])
                schema1 = st.selectbox(f"Schema from {env1}", schemas1, key="schema1")
            with col2:
                schemas2 = st.session_state.env_schemas.get(env2, [])
                # Auto-select matching schema if available
                default_index = 0
                if schema1 and schema1 in schemas2:
                    default_index = schemas2.index(schema1)
                schema2 = st.selectbox(f"Schema from {env2}", schemas2, index=default_index, key="schema2")
            
            if st.button("🔍 Compare Schemas"):
                if schema1 and schema2:
                    # Load schema metadata from respective environments
                    with st.spinner(f"Loading {schema1} from {env1}..."):
                        data1 = load_schema_metadata(schema1, st.session_state.env_connections[env1]['params'])
                    
                    with st.spinner(f"Loading {schema2} from {env2}..."):
                        data2 = load_schema_metadata(schema2, st.session_state.env_connections[env2]['params'])
                    
                    tables1 = set(data1.get('tables', []))
                    tables2 = set(data2.get('tables', []))
                    
                    # Table comparison
                    st.subheader("📊 Table Comparison")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        only_in_1 = tables1 - tables2
                        st.metric(f"Only in {schema1}", len(only_in_1))
                        if only_in_1:
                            st.write("Tables:", ", ".join(list(only_in_1)[:5]))
                    
                    with col2:
                        common = tables1 & tables2
                        st.metric("Common Tables", len(common))
                    
                    with col3:
                        only_in_2 = tables2 - tables1
                        st.metric(f"Only in {schema2}", len(only_in_2))
                        if only_in_2:
                            st.write("Tables:", ", ".join(list(only_in_2)[:5]))
                    
                    # Column comparison for common tables
                    if common:
                        st.subheader("🔍 Column Differences in Common Tables")
                        
                        col_diffs = []
                        for table in sorted(common):
                            cols1 = set(data1.get('columns', {}).get(table, []))
                            cols2 = set(data2.get('columns', {}).get(table, []))
                            
                            if cols1 != cols2:
                                col_diffs.append({
                                    'Table': table,
                                    f'Only in {schema1}': ', '.join(cols1 - cols2) or 'None',
                                    f'Only in {schema2}': ', '.join(cols2 - cols1) or 'None',
                                    'Common Columns': len(cols1 & cols2)
                                })
                        
                        if col_diffs:
                            diff_df = pd.DataFrame(col_diffs)
                            st.dataframe(diff_df, use_container_width=True)
                        else:
                            st.success("✅ All common tables have identical column structures")
                else:
                    st.warning("Please select both schemas to compare")
        elif env1:
            st.info(f"🔗 Connect to {env2} above to enable cross-environment comparison")
            st.info(f"📊 You can still compare schemas within {env1} environment")
        else:
            st.info("🔗 Please connect to the main environment first")
    
    with tab4:
        st.header("🔍 Code Impact Analysis")
        st.caption("Analyze which services reference database tables/columns and find unused database objects")
        
        # Analysis Configuration (at the top)
        st.subheader("🎯 Analysis Configuration")
        col1, col2 = st.columns(2)
        with col1:
            analysis_type = st.radio(
                "Analysis Type",
                ["Table Impact Analysis", "Column Impact Analysis", "Unused Objects Detection"],
                key="analysis_type_selection"
            )
        with col2:
            if analysis_type in ["Table Impact Analysis", "Column Impact Analysis"]:
                if st.session_state.available_schemas:
                    selected_schema = st.selectbox(
                        "Schema", 
                        st.session_state.available_schemas,
                        key="selected_schema_analysis"
                    )
                    
                    # Load schema metadata if not cached
                    cache_key = f"{st.session_state.connection_params.get('environment', 'QA')}_{selected_schema}"
                    if cache_key not in st.session_state.get('schema_metadata', {}):
                        with st.spinner(f"Loading {selected_schema} metadata..."):
                            schema_data = load_schema_metadata(selected_schema, st.session_state.connection_params)
                            st.session_state.schema_metadata[cache_key] = schema_data
                    
                    schema_data = st.session_state.schema_metadata[cache_key]
                    tables = schema_data.get('tables', [])
                    
                    if analysis_type == "Table Impact Analysis":
                        target_table = st.selectbox(
                            "Select Table", 
                            tables,
                            key="target_table_analysis"
                        )
                    else:
                        target_table = st.selectbox(
                            "Select Table", 
                            tables,
                            key="target_table_column_analysis"
                        )
                        if target_table:
                            columns = schema_data.get('columns', {}).get(target_table, [])
                            target_column = st.selectbox(
                                "Select Column", 
                                columns,
                                key="target_column_analysis"
                            )
        
        file_extensions = st.multiselect(
            "File Extensions to Scan",
            options=[".java", ".py", ".js", ".ts", ".sql", ".xml", ".yml", ".yaml"],
            default=[".java", ".py", ".sql"],
            help="Select file types to search for database references",
            key="file_extensions_analysis"
        )
        
        # Repository Configuration
        st.subheader("📂 Repository Configuration")
        
        # Repository source selection
        repo_source = st.radio(
            "Repository Source",
            ["Git Repository (Remote)", "Local Directory"],
            help="Choose between remote Git repository or local directory"
        )
        
        if repo_source == "Git Repository (Remote)":
            st.write("**Git Repository Settings**")
            
            col1, col2 = st.columns(2)
            with col1:
                git_provider = st.selectbox(
                    "Git Provider",
                    ["GitHub", "GitLab", "Bitbucket", "Azure DevOps", "Custom"]
                )
            with col2:
                clone_method = st.selectbox(
                    "Clone Method",
                    ["HTTPS (Public)", "HTTPS (Token)", "SSH"]
                )
            
            # Repository URL input
            if git_provider == "GitHub":
                analysis_scope = st.radio(
                    "Analysis Scope",
                    ["Single Repository", "Entire Organization"],
                    help="Choose to analyze one repository or all repositories in an organization"
                )
                
                if analysis_scope == "Single Repository":
                    repo_url = st.text_input(
                        "Repository URL",
                        placeholder="https://github.com/username/repository.git",
                        help="GitHub repository URL"
                    )
                else:
                    org_name = st.text_input(
                        "Organization Name",
                        placeholder="company-name",
                        help="GitHub organization name (without https://github.com/)"
                    )
                    repo_url = f"https://github.com/{org_name}" if org_name else ""
                    
                    st.warning("⚠️ Organization analysis requires authentication. Please provide your GitHub Personal Access Token below.")
            elif git_provider == "GitLab":
                repo_url = st.text_input(
                    "Repository URL",
                    placeholder="https://gitlab.com/username/repository.git",
                    help="GitLab repository URL"
                )
            elif git_provider == "Bitbucket":
                repo_url = st.text_input(
                    "Repository URL",
                    placeholder="https://bitbucket.org/username/repository.git",
                    help="Bitbucket repository URL"
                )
            elif git_provider == "Azure DevOps":
                repo_url = st.text_input(
                    "Repository URL",
                    placeholder="https://dev.azure.com/org/project/_git/repository",
                    help="Azure DevOps repository URL"
                )
            else:
                repo_url = st.text_input(
                    "Repository URL",
                    placeholder="https://your-git-server.com/repo.git",
                    help="Custom Git repository URL"
                )
            
            # Authentication for private repos and organization analysis
            git_token = None
            
            # Show token input for HTTPS (Token) or GitHub organization analysis
            if clone_method == "HTTPS (Token)" or (git_provider == "GitHub" and analysis_scope == "Entire Organization"):
                token_help = "Personal access token for private repositories"
                if git_provider == "GitHub" and analysis_scope == "Entire Organization":
                    token_help = "GitHub Personal Access Token (REQUIRED for organization analysis)"
                
                # Use token from environment if available
                default_token = os.getenv('GIT_TOKEN') or os.getenv('GITHUB_TOKEN') or ''
                git_token = st.text_input(
                    "Access Token",
                    value=default_token,
                    type="password",
                    help=token_help + " (Auto-loaded from environment if available)"
                )
                
                if git_provider == "GitHub" and analysis_scope == "Entire Organization":
                    st.info("💡 To create a GitHub token: Settings → Developer settings → Personal access tokens → Generate new token (classic) → Select 'repo' scope")
            
            elif clone_method == "SSH":
                st.info("🔑 SSH key authentication - ensure SSH keys are configured on the server")
            
            # Branch selection
            git_branch = st.text_input(
                "Branch",
                value="main",
                help="Git branch to clone (default: main)"
            )
            
            # Show repository status
            if git_provider == "GitHub" and analysis_scope == "Entire Organization":
                if org_name and git_token:
                    st.success(f"✓ Ready to analyze organization: {org_name}")
                elif org_name and not git_token:
                    st.warning("⚠️ GitHub token required for organization analysis")
                elif not org_name:
                    st.info("📝 Enter organization name above")
            else:
                if repo_url:
                    st.success(f"✓ Ready to analyze repository: {repo_url}")
                else:
                    st.info("📝 Enter repository URL above")
        
        else:  # Local Directory
            st.write("**Local Directory Settings**")
            
            # Quick path selection buttons
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🏠 Home Directory"):
                    st.session_state.selected_repo_path = os.path.expanduser('~')
                    st.rerun()
            with col2:
                if st.button("📁 Current Project"):
                    st.session_state.selected_repo_path = "/Users/truxx/Sandeep/Project"
                    st.rerun()
            with col3:
                if st.button("💼 Desktop"):
                    st.session_state.selected_repo_path = os.path.expanduser('~/Desktop')
                    st.rerun()
            
            # Manual path input
            repo_path = st.text_input(
                "Local Directory Path",
                value=st.session_state.get('selected_repo_path', "/Users/truxx/Sandeep/Project"),
                help="Enter the full path to your local repository directory"
            )
            
            # Path validation
            if repo_path:
                if os.path.exists(repo_path) and os.path.isdir(repo_path):
                    st.success(f"✓ Valid directory: {repo_path}")
                    st.session_state.selected_repo_path = repo_path
                else:
                    st.error(f"✗ Directory not found: {repo_path}")
        

        
        # Get final repo configuration
        if repo_source == "Git Repository (Remote)":
            repo_path = None
            # Prepare remote repo config for analysis
            if git_provider == "GitHub" and analysis_scope == "Entire Organization":
                repo_config = {'type': 'github_org', 'org_name': org_name, 'branch': git_branch, 'token': git_token} if org_name and git_token else None
            else:
                repo_config = {'type': 'git_repo', 'url': repo_url, 'branch': git_branch, 'token': git_token} if repo_url else None
        else:
            repo_path = st.session_state.get('selected_repo_path', repo_path)
            repo_config = None
        

        
        # Single Run Analysis button
        if st.button("🔍 Run Analysis", type="primary", key="run_analysis_btn"):
            # Validate inputs
            if repo_source == "Git Repository (Remote)":
                if not repo_config:
                    st.error("Please configure repository settings above")
                    st.stop()
            else:
                if not repo_path or not os.path.exists(repo_path):
                    st.error("Please select a valid local directory")
                    st.stop()
            
            if not file_extensions:
                st.error("Please select at least one file extension")
                st.stop()
            
            try:
                analyzer = CodeImpactAnalyzer()
                
                # Step 1: Get repository data (if remote)
                if repo_source == "Git Repository (Remote)":
                    with st.spinner("Fetching repository data..."):
                        git_service = GitAnalysisService(repo_config['token'])
                        
                        if repo_config['type'] == 'github_org':
                            start_time = time.time()
                            repo_data = git_service.analyze_organization(repo_config['org_name'], repo_config['branch'])
                            analysis_time = time.time() - start_time
                            st.info(f"⏱️ Repository fetch completed in {analysis_time:.1f} seconds")
                            st.info(f"📊 Scanned {repo_data.get('total_repos', 0)} active repositories and downloaded {len(repo_data['files'])} source code files for analysis")
                        else:
                            repo_data = git_service.analyze_repository(repo_config['url'], repo_config['branch'])
                            st.info(f"📊 Downloaded {len(repo_data['files'])} source code files from repository for analysis")
                else:
                    repo_data = None
                
                # Step 2: Analyze code for database references
                with st.spinner("Analyzing database references..."):
                    if analysis_type == "Table Impact Analysis":
                        if repo_source == "Git Repository (Remote)":
                            results = analyzer.analyze_table_impact_api(repo_data, target_table, file_extensions)
                        else:
                            results = analyzer.analyze_table_impact_local(repo_path, target_table, file_extensions)
                        display_table_impact_results(results, target_table)
                    elif analysis_type == "Column Impact Analysis":
                        if repo_source == "Git Repository (Remote)":
                            results = analyzer.analyze_column_impact_api(repo_data, target_table, target_column, file_extensions)
                        else:
                            results = analyzer.analyze_column_impact_local(repo_path, target_table, target_column, file_extensions)
                        display_column_impact_results(results, target_table, target_column)
                    else:  # Unused Objects Detection
                        if st.session_state.available_schemas:
                            all_tables = set()
                            all_columns = set()
                            for schema in st.session_state.available_schemas:
                                cache_key = f"{st.session_state.connection_params.get('environment', 'QA')}_{schema}"
                                if cache_key not in st.session_state.get('schema_metadata', {}):
                                    schema_data = load_schema_metadata(schema, st.session_state.connection_params)
                                    st.session_state.schema_metadata[cache_key] = schema_data
                                else:
                                    schema_data = st.session_state.schema_metadata[cache_key]
                                
                                for table in schema_data.get('tables', []):
                                    all_tables.add(f"{schema}.{table}")
                                    for col in schema_data.get('columns', {}).get(table, []):
                                        all_columns.add(f"{schema}.{table}.{col}")
                            
                            if repo_source == "Git Repository (Remote)":
                                results = analyzer.find_unused_objects_api(repo_data, all_tables, all_columns, file_extensions)
                            else:
                                results = analyzer.find_unused_objects_local(repo_path, all_tables, all_columns, file_extensions)
                            display_unused_objects_results(results)
            except Exception as e:
                st.error(f"Analysis failed: {str(e)}")

