"""ERD generation service"""
import pandas as pd
import graphviz
from sqlalchemy import create_engine
from .database_service import read_sql_df

def fetch_columns(conn, engine_type, schemas):
    q = """
    select table_schema as `schema`,
           table_name,
           column_name,
           data_type,
           is_nullable,
           coalesce(character_maximum_length,'') as char_len,
           coalesce(numeric_precision,'') as num_precision,
           coalesce(numeric_scale,'') as num_scale,
           column_default
    from information_schema.columns
    where table_schema = database()
    order by table_name, ordinal_position
    """
    return read_sql_df(conn, q)

def fetch_primary_keys(conn, engine_type, schemas):
    q = """
    select k.table_schema as `schema`, k.table_name, k.column_name, k.ordinal_position
    from information_schema.table_constraints t
    join information_schema.key_column_usage k
      on t.constraint_name = k.constraint_name
     and t.table_schema = k.table_schema
    where t.constraint_type = 'PRIMARY KEY'
      and k.table_schema = database()
    order by k.table_name, k.ordinal_position
    """
    return read_sql_df(conn, q)

def fetch_foreign_keys(conn, engine_type, schemas):
    q = """
    select
      k.table_schema as child_schema,
      k.table_name   as child_table,
      k.column_name  as child_column,
      k.referenced_table_schema as parent_schema,
      k.referenced_table_name   as parent_table,
      k.referenced_column_name  as parent_column,
      k.constraint_name
    from information_schema.key_column_usage k
    where k.referenced_table_name is not null
      and k.table_schema = database()
    order by child_table
    """
    return read_sql_df(conn, q)

def fetch_indexes(conn, engine_type, schemas):
    q = """
    select table_schema as `schema`,
           table_name,
           index_name,
           group_concat(column_name order by seq_in_index) as index_columns,
           min(non_unique) as non_unique
    from information_schema.statistics
    where table_schema = database()
    group by table_schema, table_name, index_name
    order by table_name, index_name
    """
    return read_sql_df(conn, q)

def fetch_row_counts(conn, engine_type, schemas, include_row_counts):
    if not include_row_counts:
        return pd.DataFrame(columns=["schema","table_name","row_count"])
    q = """
    select table_schema as `schema`,
           table_name,
           table_rows as row_count
    from information_schema.tables
    where table_schema = database()
      and table_type = 'BASE TABLE'
    """
    return read_sql_df(conn, q)

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def build_graph(schema_tables, columns, pks, fks, indexes, rowcounts, cluster_by_schema=True, show_schema_prefix=True, max_cols=80):
    dot = graphviz.Digraph(graph_attr={"rankdir": "LR", "fontsize": "10"})

    # Fast lookups with dynamic column names
    pk_set = set()
    if not pks.empty:
        pk_schema_col = next((col for col in pks.columns if 'schema' in col.lower()), 'schema')
        pk_table_col = next((col for col in pks.columns if 'table' in col.lower()), 'table_name')
        pk_column_col = next((col for col in pks.columns if 'column' in col.lower()), 'column_name')
        pk_set = {(r[pk_schema_col], r[pk_table_col], r[pk_column_col]) for _, r in pks.iterrows()}
    
    fk_cols_map = {}
    if not fks.empty:
        fk_child_schema_col = next((col for col in fks.columns if 'child' in col.lower() and 'schema' in col.lower()), 'child_schema')
        fk_child_table_col = next((col for col in fks.columns if 'child' in col.lower() and 'table' in col.lower()), 'child_table')
        fk_child_column_col = next((col for col in fks.columns if 'child' in col.lower() and 'column' in col.lower()), 'child_column')
        fk_parent_schema_col = next((col for col in fks.columns if 'parent' in col.lower() and 'schema' in col.lower()), 'parent_schema')
        fk_parent_table_col = next((col for col in fks.columns if 'parent' in col.lower() and 'table' in col.lower()), 'parent_table')
        fk_parent_column_col = next((col for col in fks.columns if 'parent' in col.lower() and 'column' in col.lower()), 'parent_column')
        fk_cols_map = {(r[fk_child_schema_col], r[fk_child_table_col], r[fk_child_column_col]): (r[fk_parent_schema_col], r[fk_parent_table_col], r[fk_parent_column_col]) for _, r in fks.iterrows()}

    # Index map per table
    idx_map = {}
    if not indexes.empty:
        idx_schema_col = next((col for col in indexes.columns if 'schema' in col.lower()), 'schema')
        idx_table_col = next((col for col in indexes.columns if 'table' in col.lower()), 'table_name')
        for _, r in indexes.iterrows():
            key = (r[idx_schema_col], r[idx_table_col])
            idx_map.setdefault(key, []).append(r.to_dict())

    # Rowcount map
    rc_map = {}
    if not rowcounts.empty:
        rc_schema_col = next((col for col in rowcounts.columns if 'schema' in col.lower()), 'schema')
        rc_table_col = next((col for col in rowcounts.columns if 'table' in col.lower()), 'table_name')
        rc_count_col = next((col for col in rowcounts.columns if 'count' in col.lower() or 'row' in col.lower()), 'row_count')
        for _, r in rowcounts.iterrows():
            rc_map[(r[rc_schema_col], r[rc_table_col])] = int(r.get(rc_count_col, 0) or 0)

    # Build nodes (cluster per schema)
    if cluster_by_schema:
        for schema, group in schema_tables.groupby("schema"):
            with dot.subgraph(name=f"cluster_{schema}") as c:
                c.attr(label=schema, style="rounded", color="gray")
                for _, t in group.iterrows():
                    schema_name = t['schema']
                    table_name = t['table_name']
                    schema_col_name = next((col for col in columns.columns if 'schema' in col.lower()), 'schema')
                    table_col_name = next((col for col in columns.columns if 'table' in col.lower()), 'table_name')
                    cols_df = columns[(columns[schema_col_name] == schema_name) & (columns[table_col_name] == table_name)]
                    idx_df = pd.DataFrame(idx_map.get((schema_name, table_name), []))
                    rowc = rc_map.get((schema_name, table_name))
                    label = build_table_label(schema_name, table_name, cols_df, pk_set, fk_cols_map, idx_df, rowc, show_schema_prefix, max_cols)
                    c.node(f"{schema_name}.{table_name}", label=label, shape="plaintext")
    else:
        for _, t in schema_tables.iterrows():
            schema_name = t['schema']
            table_name = t['table_name']
            schema_col_name = next((col for col in columns.columns if 'schema' in col.lower()), 'schema')
            table_col_name = next((col for col in columns.columns if 'table' in col.lower()), 'table_name')
            cols_df = columns[(columns[schema_col_name] == schema_name) & (columns[table_col_name] == table_name)]
            idx_df = pd.DataFrame(idx_map.get((schema_name, table_name), []))
            rowc = rc_map.get((schema_name, table_name))
            label = build_table_label(schema_name, table_name, cols_df, pk_set, fk_cols_map, idx_df, rowc, show_schema_prefix, max_cols)
            dot.node(f"{schema_name}.{table_name}", label=label, shape="plaintext")

    # Edges (child -> parent)
    if not fks.empty:
        fk_child_schema_col = next((col for col in fks.columns if 'child' in col.lower() and 'schema' in col.lower()), 'child_schema')
        fk_child_table_col = next((col for col in fks.columns if 'child' in col.lower() and 'table' in col.lower()), 'child_table')
        fk_child_column_col = next((col for col in fks.columns if 'child' in col.lower() and 'column' in col.lower()), 'child_column')
        fk_parent_schema_col = next((col for col in fks.columns if 'parent' in col.lower() and 'schema' in col.lower()), 'parent_schema')
        fk_parent_table_col = next((col for col in fks.columns if 'parent' in col.lower() and 'table' in col.lower()), 'parent_table')
        fk_parent_column_col = next((col for col in fks.columns if 'parent' in col.lower() and 'column' in col.lower()), 'parent_column')
        for _, r in fks.iterrows():
            child = f"{r[fk_child_schema_col]}.{r[fk_child_table_col]}"
            parent = f"{r[fk_parent_schema_col]}.{r[fk_parent_table_col]}"
            edge_label = f"{r[fk_child_column_col]} → {r[fk_parent_column_col]}"
            dot.edge(child, parent, label=edge_label, arrowsize="0.7")

    return dot

def build_table_label(schema, table, cols_df, pk_set, fk_cols_map, idx_df=None, row_count=None, show_schema=True, max_cols=80):
    title = f"{schema}.{table}" if show_schema else table
    
    rows_html = _build_column_rows(cols_df, schema, table, pk_set, fk_cols_map, max_cols)
    idx_html = _build_index_rows(idx_df)
    
    rc_html = []
    if row_count is not None:
        rc_html.append(f"<tr><td align='left'><font point-size='9'>~rows: {int(row_count):,}</font></td></tr>")

    return (
        "<\n<table border='0' cellborder='1' cellspacing='0'>"
        f"<tr><td bgcolor='lightblue'><b>{html_escape(title)}</b></td></tr>"
        + "".join(rc_html + rows_html + idx_html)
        + "</table>\n>"
    )

def _build_column_rows(cols_df, schema, table, pk_set, fk_cols_map, max_cols):
    col_name_col, data_type_col, nullable_col = _get_column_names(cols_df)
    rows_html = []
    
    for displayed, (_, r) in enumerate(cols_df.iterrows()):
        if displayed >= max_cols:
            rows_html.append(f"<tr><td align='left'><i>… {len(cols_df)-max_cols} more columns</i></td></tr>")
            break
            
        col = r[col_name_col]
        dtype = r[data_type_col]
        nullable = r[nullable_col]
        
        key_prefix = ""
        if (schema, table, col) in pk_set:
            key_prefix += "🔑 "
        if (schema, table, col) in fk_cols_map:
            key_prefix += "🔗 "
            
        nn = "NOT NULL" if str(nullable).upper() == "NO" else "NULL"
        detail = _format_column_detail(r, dtype)
        
        rows_html.append(
            f"<tr><td align='left'><font point-size='10'>{html_escape(key_prefix + col)} : {html_escape(detail)} <i>({nn})</i></font></td></tr>"
        )
    return rows_html

def _build_index_rows(idx_df):
    if idx_df is None or idx_df.empty:
        return []
        
    idx_html = ["<tr><td><b>Indexes</b></td></tr>"]
    index_name_col = next((col for col in idx_df.columns if 'index' in col.lower() and 'name' in col.lower()), 'index_name')
    
    for _, r in idx_df.iterrows():
        unique_col = next((col for col in idx_df.columns if 'unique' in col.lower()), 'non_unique')
        columns_col = next((col for col in idx_df.columns if 'column' in col.lower()), 'index_columns')
        unique = "UNIQUE " if (str(r.get(unique_col,"1")) == "0") else ""
        label = f"{unique}{r[index_name_col]} ({r[columns_col]})"
        idx_html.append(f"<tr><td align='left'><font point-size='9'>{html_escape(label)}</font></td></tr>")
    return idx_html

def _get_column_names(cols_df):
    col_name_col = next((col for col in cols_df.columns if 'column' in col.lower() and 'name' in col.lower()), 'column_name')
    data_type_col = next((col for col in cols_df.columns if 'data' in col.lower() and 'type' in col.lower()), 'data_type')
    nullable_col = next((col for col in cols_df.columns if 'nullable' in col.lower()), 'is_nullable')
    return col_name_col, data_type_col, nullable_col

def _format_column_detail(r, dtype):
    detail = dtype
    if r.get("char_len") not in (None, "", 0, "0"):
        detail += f"({r['char_len']})"
    elif r.get("num_precision") not in (None, "", 0, "0"):
        if r.get("num_scale") not in (None, "", 0, "0"):
            detail += f"({r['num_precision']},{r['num_scale']})"
        else:
            detail += f"({r['num_precision']})"
    return detail