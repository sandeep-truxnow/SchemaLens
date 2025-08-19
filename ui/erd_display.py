"""ERD display module for rendering ERD data and diagrams"""

import streamlit as st
import pandas as pd


def render_erd_data_sections(erd_data, sel_schemas):
    """Render ERD data sections (columns, keys, indexes, etc.)"""
    _render_columns_section(erd_data['cols'])
    _render_primary_keys_section(erd_data['pks'])
    _render_foreign_keys_section(erd_data['fks'])
    _render_indexes_section(erd_data['idx'])
    
    if erd_data['include_row_counts']:
        _render_row_counts_section(erd_data['rc'])
    
    _render_table_sizes_section(sel_schemas)


def _render_columns_section(cols):
    """Render columns section"""
    with st.expander("📄 Columns", expanded=False):
        st.dataframe(cols, use_container_width=True)


def _render_primary_keys_section(pks):
    """Render primary keys section"""
    with st.expander("🔑 Primary Keys", expanded=False):
        if not pks.empty:
            pk_grouped = _group_primary_keys(pks)
            grouped_df = pd.DataFrame(pk_grouped)
            st.dataframe(grouped_df, use_container_width=True)
        else:
            st.info("No primary keys found")


def _group_primary_keys(pk_df):
    """Group primary keys by table to show composite keys"""
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
    
    return pk_grouped


def _render_foreign_keys_section(fks):
    """Render foreign keys section"""
    with st.expander("🔗 Foreign Keys", expanded=False):
        if not fks.empty:
            fk_grouped = _group_foreign_keys(fks)
            grouped_fk_df = pd.DataFrame(fk_grouped)
            st.dataframe(grouped_fk_df, use_container_width=True)
        else:
            st.info("No foreign keys found")


def _group_foreign_keys(fk_df):
    """Group foreign keys by constraint name"""
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
    
    return fk_grouped


def _render_indexes_section(idx):
    """Render indexes section"""
    with st.expander("📚 Indexes", expanded=False):
        if not idx.empty:
            st.dataframe(idx, use_container_width=True)
        else:
            st.info("No indexes found")


def _render_row_counts_section(rc):
    """Render row counts section"""
    with st.expander("🔢 Row Count Estimates", expanded=False):
        if not rc.empty:
            st.dataframe(rc, use_container_width=True)
        else:
            st.info("No row count data available")


def _render_table_sizes_section(sel_schemas):
    """Render table sizes section"""
    with st.expander("💾 Table Sizes", expanded=False):
        size_data = _collect_table_size_data(sel_schemas)
        
        if size_data:
            size_df = pd.DataFrame(size_data)
            # Sort by total size descending
            size_df['sort_size'] = size_df['Total Size (MB)'].astype(float)
            size_df = size_df.sort_values('sort_size', ascending=False).drop('sort_size', axis=1)
            st.dataframe(size_df, use_container_width=True)
        else:
            st.info("No table size data available")


def _collect_table_size_data(sel_schemas):
    """Collect table size data from metadata"""
    size_data = []
    
    for schema in sel_schemas:
        cache_key = f"{st.session_state.connection_params.get('environment', 'QA')}_{schema}"
        if cache_key in st.session_state.schema_metadata:
            schema_data = st.session_state.schema_metadata[cache_key]
            
            for table, info in schema_data.get('table_info', {}).items():
                data_size = info.get('data_size', 0) or 0
                index_size = info.get('index_size', 0) or 0
                total_size_mb = (data_size + index_size) / (1024**2)
                
                if total_size_mb > 0:  # Only show tables with size data
                    size_data.append({
                        'Schema': schema,
                        'Table': table,
                        'Data Size (MB)': f"{data_size / (1024**2):.2f}",
                        'Index Size (MB)': f"{index_size / (1024**2):.2f}",
                        'Total Size (MB)': f"{total_size_mb:.2f}",
                        'Rows': f"{info.get('rows', 0) or 0:,}"
                    })
    
    return size_data


def render_erd_diagram(erd_data, sel_schemas):
    """Render ERD diagram with controls"""
    # Display ERD with execution time and zoom controls
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        st.subheader("🗺️ Entity Relationship Diagram")
    
    with col2:
        if 'execution_time' in erd_data:
            st.metric("Generation Time", f"{erd_data['execution_time']:.2f}s")
    
    with col3:
        total_size_gb = _calculate_total_size(sel_schemas)
        st.metric("Total Size", f"{total_size_gb:.2f} GB")
    
    with col4:
        zoom_level = st.selectbox(
            "Zoom Level",
            options=["100%", "200%", "500%"],
            index=0,  # Default to 100%
            key="erd_zoom"
        )
    
    # Apply zoom styling and render diagram
    _render_diagram_with_zoom(erd_data['dot'], zoom_level)
    
    # Export options
    _render_export_options(erd_data['dot'])


def _calculate_total_size(sel_schemas):
    """Calculate total size from cached metadata"""
    total_size_gb = 0
    
    for schema in sel_schemas:
        cache_key = f"{st.session_state.connection_params.get('environment', 'QA')}_{schema}"
        if cache_key in st.session_state.schema_metadata:
            schema_data = st.session_state.schema_metadata[cache_key]
            for table, info in schema_data.get('table_info', {}).items():
                data_size = info.get('data_size', 0) or 0
                index_size = info.get('index_size', 0) or 0
                total_size_gb += (data_size + index_size) / (1024**3)
    
    return total_size_gb


def _render_diagram_with_zoom(dot, zoom_level):
    """Render diagram with zoom styling"""
    zoom_value = int(zoom_level.replace('%', '')) / 100
    st.markdown(f"""
    <div style="transform: scale({zoom_value}); transform-origin: top left; width: {100/zoom_value}%; height: {100/zoom_value}%;">
    """, unsafe_allow_html=True)
    
    st.graphviz_chart(dot)
    
    st.markdown("</div>", unsafe_allow_html=True)


def _render_export_options(dot):
    """Render export options for ERD"""
    col1, col2 = st.columns(2)
    
    with col1:
        st.download_button(
            label="📥 Download .dot",
            data=dot.source.encode("utf-8"),
            file_name="erd.dot",
            mime="text/vnd.graphviz",
        )
    
    with col2:
        try:
            png_bytes = dot.pipe(format="png")
            st.download_button(
                label="🖼️ Download PNG",
                data=png_bytes,
                file_name="erd.png",
                mime="image/png",
            )
        except Exception:
            st.info("PNG export requires Graphviz binaries on server.")