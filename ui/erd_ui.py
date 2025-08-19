"""ERD UI module for Entity Relationship Diagram generation and display"""

import streamlit as st
import pandas as pd
import time
from sqlalchemy import create_engine
from services.database_service import load_schema_metadata, read_sql_df
from services.erd_service import (
    fetch_columns, fetch_primary_keys, fetch_foreign_keys, 
    fetch_indexes, fetch_row_counts, build_graph
)
from utils.connection_utils import reconnect_if_needed


def render_erd_tab():
    """Render ERD diagram tab"""
    st.header("📋 Database Info")
    st.write(f"**Environment:** {st.session_state.connection_params.get('environment', 'Unknown')}")
    st.write(f"**Available Schemas:** {len(st.session_state.available_schemas)}")
    
    if st.session_state.available_schemas:
        sel_schemas = _render_schema_selection()
        options = _render_erd_options()
        run_btn = st.button("🔁 Generate ERD", type="primary", disabled=not sel_schemas)
        
        if run_btn:
            _handle_erd_generation(sel_schemas, options)
        
        _render_persistent_exclusions(sel_schemas)
        _render_erd_display(sel_schemas)


def _render_schema_selection():
    """Render schema selection UI"""
    st.subheader("Schema Selection")
    return st.multiselect(
        "Select schemas/databases to analyze",
        options=st.session_state.available_schemas,
        default=st.session_state.available_schemas[:3] if len(st.session_state.available_schemas) <= 3 else st.session_state.available_schemas[:1],
        help="Choose which schemas/databases to include in the ERD"
    )


def _render_erd_options():
    """Render ERD generation options"""
    col1a, col1b = st.columns(2)
    with col1a:
        include_row_counts = st.checkbox("Estimate row counts", value=False)
        include_indexes = st.checkbox("Include indexes in table panels", value=True)
    with col1b:
        cluster_by_schema = st.checkbox("Cluster diagram by schema", value=True)
        show_schema_prefix = st.checkbox("Show schema prefix on table nodes", value=True)
    
    max_cols_in_node = st.slider("Max columns listed per table", 10, 200, 80)
    
    return {
        'include_row_counts': include_row_counts,
        'include_indexes': include_indexes,
        'cluster_by_schema': cluster_by_schema,
        'show_schema_prefix': show_schema_prefix,
        'max_cols_in_node': max_cols_in_node
    }


def _handle_erd_generation(sel_schemas, options):
    """Handle ERD generation process"""
    if not reconnect_if_needed():
        st.error("Connection lost. Please reconnect to server.")
        return
    
    try:
        start_time = time.time()
        
        with st.session_state.engine.connect() as conn:
            st.success("✅ Generating ERD...")
            
            # Fetch metadata for all schemas
            all_data = _fetch_all_schema_metadata(sel_schemas, options['include_row_counts'])
            
            if all_data['cols'].empty:
                st.warning("No tables found in the selected schemas.")
                return
            
            # Filter tables and generate ERD
            filtered_data = _filter_and_process_tables(all_data, sel_schemas)
            
            if filtered_data['tables'].empty:
                st.warning("No active tables found after filtering.")
                return
            
            # Build ERD
            dot = build_graph(
                schema_tables=filtered_data['tables'],
                columns=filtered_data['cols'],
                pks=filtered_data['pks'],
                fks=filtered_data['fks'],
                indexes=filtered_data['idx'],
                rowcounts=filtered_data['rc'],
                cluster_by_schema=options['cluster_by_schema'],
                show_schema_prefix=options['show_schema_prefix'],
                max_cols=options['max_cols_in_node']
            )
            
            execution_time = time.time() - start_time
            
            # Store ERD data
            _store_erd_data(dot, filtered_data, options['include_row_counts'], execution_time)
            
            # Display results
            _display_generation_results(sel_schemas, execution_time)
            
    except Exception as e:
        st.error(f"❌ ERD generation failed: {e}")


def _fetch_all_schema_metadata(sel_schemas, include_row_counts):
    """Fetch metadata for all selected schemas"""
    conn_params = st.session_state.connection_params
    all_cols, all_pks, all_fks, all_idx, all_rc = [], [], [], [], []
    
    for schema in sel_schemas:
        schema_engine = create_engine(f"mysql+mysqlconnector://{conn_params['username']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{schema}")
        
        with schema_engine.connect() as schema_conn:
            cols = fetch_columns(schema_conn, conn_params['db_type'], [schema])
            pks = fetch_primary_keys(schema_conn, conn_params['db_type'], [schema])
            fks = fetch_foreign_keys(schema_conn, conn_params['db_type'], [schema])
            idx = fetch_indexes(schema_conn, conn_params['db_type'], [schema])
            rc = fetch_row_counts(schema_conn, conn_params['db_type'], [schema], include_row_counts)
            
            # Add schema name to results
            for df, name in [(cols, 'cols'), (pks, 'pks'), (idx, 'idx'), (rc, 'rc')]:
                if not df.empty:
                    df['schema'] = schema
                    if name == 'cols': all_cols.append(df)
                    elif name == 'pks': all_pks.append(df)
                    elif name == 'idx': all_idx.append(df)
                    elif name == 'rc': all_rc.append(df)
            
            if not fks.empty:
                fks['child_schema'] = schema
                all_fks.append(fks)
    
    return {
        'cols': pd.concat(all_cols, ignore_index=True) if all_cols else pd.DataFrame(),
        'pks': pd.concat(all_pks, ignore_index=True) if all_pks else pd.DataFrame(),
        'fks': pd.concat(all_fks, ignore_index=True) if all_fks else pd.DataFrame(),
        'idx': pd.concat(all_idx, ignore_index=True) if all_idx else pd.DataFrame(),
        'rc': pd.concat(all_rc, ignore_index=True) if all_rc else pd.DataFrame()
    }


def _filter_and_process_tables(all_data, sel_schemas):
    """Filter tables based on usage and process data"""
    cols = all_data['cols']
    
    # Create tables DataFrame
    actual_cols = list(cols.columns)
    schema_col = next((col for col in actual_cols if 'schema' in col.lower()), None)
    table_col = next((col for col in actual_cols if 'table' in col.lower()), None)
    
    tables = cols[[schema_col, table_col]].drop_duplicates().sort_values([schema_col, table_col]).reset_index(drop=True)
    tables.columns = ['schema', 'table_name']
    
    # Filter tables and collect exclusions
    filtered_tables, excluded_details = _filter_unused_tables(tables, sel_schemas)
    
    # Store exclusions in session state
    exclusion_key = f"excluded_tables_{'_'.join(sorted(sel_schemas))}"
    st.session_state[exclusion_key] = excluded_details
    
    # Show table statistics
    total_tables = len(tables)
    st.info(f"✅ Total tables in schema: {total_tables}")
    
    # Show exclusion info
    if excluded_details:
        st.warning(f"⚠️ Excluded {len(excluded_details)} unused tables from ERD")
        with st.expander(f"View all {len(excluded_details)} excluded tables and reasons", expanded=False):
            excluded_df = pd.DataFrame(excluded_details)
            st.dataframe(excluded_df, use_container_width=True)
            st.caption("Tables are excluded from ERD when they have no recent update activity (UPDATE_TIME is null, NaT, or missing)")
    
    # Filter other dataframes to match active tables
    if filtered_tables:
        tables_df = pd.DataFrame(filtered_tables)
        filtered_data = _filter_related_data(all_data, tables_df)
        filtered_data['tables'] = tables_df
        return filtered_data
    else:
        return {'tables': pd.DataFrame(columns=['schema', 'table_name']), **all_data}


def _filter_unused_tables(tables, sel_schemas):
    """Filter out unused tables based on UPDATE_TIME"""
    table_info = _collect_table_info(sel_schemas)
    filtered_tables = []
    excluded_details = []
    
    for _, row in tables.iterrows():
        schema_name = row['schema']
        table_name = row['table_name']
        info = table_info.get((schema_name, table_name), {})
        last_update = info.get('last_update')
        
        # Check if table is enum/lookup table
        is_enum_table = _is_enum_table(table_name)
        
        if is_enum_table:
            filtered_tables.append(row)
        elif _is_unused_table(last_update):
            excluded_details.append(_create_exclusion_record(schema_name, table_name, info, last_update))
        else:
            filtered_tables.append(row)
    
    return filtered_tables, excluded_details


def _collect_table_info(sel_schemas):
    """Collect table info from cached metadata"""
    table_info = {}
    
    for schema in sel_schemas:
        cache_key = f"{st.session_state.connection_params.get('environment', 'QA')}_{schema}"
        if cache_key in st.session_state.get('schema_metadata', {}):
            schema_data = st.session_state.schema_metadata[cache_key]
        else:
            schema_data = load_schema_metadata(schema, st.session_state.connection_params)
            st.session_state.schema_metadata[cache_key] = schema_data
        
        for table, info in schema_data.get('table_info', {}).items():
            table_info[(schema, table)] = info
    
    return table_info


def _is_enum_table(table_name):
    """Check if table is enum/lookup table"""
    table_lower = table_name.lower()
    return any(pattern in table_lower for pattern in [
        'status', 'type', 'category', 'enum', 'lookup', 'reference', 
        'config', 'setting', 'option', 'code', 'list', 'reason', 
        'complete_by', 'job_truck_unit', 'dispath_ordrer', 'attribiute', 
        'transcription_field', 'entity_note'
    ])


def _is_unused_table(last_update):
    """Check if table is unused based on UPDATE_TIME"""
    return (last_update is None or 
            pd.isna(last_update) or 
            str(last_update).lower() in ['nat', 'none', 'null', 'unknown'])


def _create_exclusion_record(schema_name, table_name, info, last_update):
    """Create exclusion record for unused table"""
    # Determine specific reason
    if last_update is None:
        reason = "No UPDATE_TIME metadata (non-enum table)"
    elif pd.isna(last_update):
        reason = "UPDATE_TIME is NaT (non-enum table)"
    else:
        reason = f"UPDATE_TIME is '{last_update}' (non-enum table)"
    
    # Calculate table size
    data_size = info.get('data_size', 0) or 0
    index_size = info.get('index_size', 0) or 0
    total_size_mb = (data_size + index_size) / (1024**2)
    
    size_display = f"{total_size_mb / 1024:.2f} GB" if total_size_mb >= 1024 else f"{total_size_mb:.2f} MB"
    
    return {
        'Table': f"{schema_name}.{table_name}",
        'Reason': reason,
        'Size': size_display,
        'Rows': f"{info.get('rows', 0) or 0:,}",
        'Created': str(info.get('created'))[:19] if info.get('created') and not pd.isna(info.get('created')) else 'Unknown',
        'Last Updated': 'None'
    }


def _filter_related_data(all_data, tables_df):
    """Filter related dataframes to match active tables"""
    table_pairs = [(row['schema'], row['table_name']) for _, row in tables_df.iterrows()]
    
    filtered_data = {}
    
    # Filter columns
    cols = all_data['cols']
    if not cols.empty:
        cols_schema_col = next((col for col in cols.columns if 'schema' in col.lower()), 'schema')
        cols_table_col = next((col for col in cols.columns if 'table' in col.lower()), 'table_name')
        filtered_data['cols'] = cols[cols.apply(lambda x: (x[cols_schema_col], x[cols_table_col]) in table_pairs, axis=1)]
    else:
        filtered_data['cols'] = cols
    
    # Filter other dataframes similarly
    for key, df in [('pks', all_data['pks']), ('idx', all_data['idx']), ('rc', all_data['rc'])]:
        if not df.empty:
            schema_col = next((col for col in df.columns if 'schema' in col.lower()), 'schema')
            table_col = next((col for col in df.columns if 'table' in col.lower()), 'table_name')
            filtered_data[key] = df[df.apply(lambda x: (x[schema_col], x[table_col]) in table_pairs, axis=1)]
        else:
            filtered_data[key] = df
    
    # Filter foreign keys
    fks = all_data['fks']
    if not fks.empty:
        fk_child_schema_col = next((col for col in fks.columns if 'child' in col.lower() and 'schema' in col.lower()), 'child_schema')
        fk_child_table_col = next((col for col in fks.columns if 'child' in col.lower() and 'table' in col.lower()), 'child_table')
        filtered_data['fks'] = fks[fks.apply(lambda x: (x[fk_child_schema_col], x[fk_child_table_col]) in table_pairs, axis=1)]
    else:
        filtered_data['fks'] = fks
    
    return filtered_data


def _store_erd_data(dot, filtered_data, include_row_counts, execution_time):
    """Store ERD data in session state"""
    st.session_state.erd_data = {
        'dot': dot,
        'cols': filtered_data['cols'],
        'pks': filtered_data['pks'],
        'fks': filtered_data['fks'],
        'idx': filtered_data['idx'],
        'rc': filtered_data['rc'],
        'include_row_counts': include_row_counts,
        'execution_time': execution_time
    }
    st.session_state.erd_generated = True


def _display_generation_results(sel_schemas, execution_time):
    """Display ERD generation results"""
    st.success(f"✅ ERD generated successfully in {execution_time:.2f} seconds!")
    
    # Calculate and display schema sizes
    schema_sizes = _calculate_schema_sizes(sel_schemas)
    if schema_sizes:
        st.subheader("💾 Schema Size Information")
        size_cols = st.columns(len(sel_schemas) + 1)
        
        total_size_gb = 0
        for i, (schema, size_gb) in enumerate(schema_sizes.items()):
            with size_cols[i]:
                st.metric(f"{schema} Size", f"{size_gb:.2f} GB")
            total_size_gb += size_gb
        
        with size_cols[-1]:
            st.metric("Total Size", f"{total_size_gb:.2f} GB")


def _calculate_schema_sizes(sel_schemas):
    """Calculate schema sizes from cached metadata"""
    schema_sizes = {}
    
    for schema in sel_schemas:
        cache_key = f"{st.session_state.connection_params.get('environment', 'QA')}_{schema}"
        if cache_key in st.session_state.schema_metadata:
            schema_data = st.session_state.schema_metadata[cache_key]
            schema_size_bytes = 0
            
            for table, info in schema_data.get('table_info', {}).items():
                data_size = info.get('data_size', 0) or 0
                index_size = info.get('index_size', 0) or 0
                schema_size_bytes += (data_size + index_size)
            
            schema_size_gb = schema_size_bytes / (1024**3)  # Convert to GB
            schema_sizes[schema] = schema_size_gb
    
    return schema_sizes


def _render_persistent_exclusions(sel_schemas):
    """Render persistent exclusion list"""
    if sel_schemas:
        exclusion_key = f"excluded_tables_{'_'.join(sorted(sel_schemas))}"
        if exclusion_key in st.session_state and st.session_state[exclusion_key]:
            excluded_details = st.session_state[exclusion_key]
            st.warning(f"⚠️ {len(excluded_details)} unused tables will be excluded from ERD")
            with st.expander(f"View {len(excluded_details)} excluded tables", expanded=False):
                excluded_df = pd.DataFrame(excluded_details)
                st.dataframe(excluded_df, use_container_width=True)
                st.caption("These tables are excluded when UPDATE_TIME is null, NaT, or missing")


def _render_erd_display(sel_schemas):
    """Render ERD display if generated"""
    if st.session_state.erd_generated and st.session_state.erd_data:
        from ui.erd_display import render_erd_data_sections, render_erd_diagram
        
        render_erd_data_sections(st.session_state.erd_data, sel_schemas)
        render_erd_diagram(st.session_state.erd_data, sel_schemas)