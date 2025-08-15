"""Database connection and metadata service"""
import pandas as pd
from sqlalchemy import create_engine, text
import subprocess
import os
import time

def read_sql_df(conn, query, params=None):
    return pd.read_sql(text(query), conn, params=params or {})

def load_schema_metadata(schema, connection_params):
    """Load metadata for a single schema quickly"""
    try:
        engine = create_engine(f"mysql+mysqlconnector://{connection_params['username']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{schema}")
        with engine.connect() as conn:
            tables_query = f"""
            SELECT 
                t.TABLE_NAME,
                t.UPDATE_TIME,
                t.CREATE_TIME,
                t.TABLE_ROWS,
                t.DATA_LENGTH,
                t.INDEX_LENGTH
            FROM information_schema.tables t
            WHERE t.table_schema = '{schema}' AND t.table_type = 'BASE TABLE'
            ORDER BY t.TABLE_NAME
            """
            tables_df = read_sql_df(conn, tables_query)
            
            if not tables_df.empty:
                table_col = tables_df.columns[0]
                tables = tables_df[table_col].tolist()
                
                columns_query = f"""
                SELECT TABLE_NAME, COLUMN_NAME
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """
                columns_df = read_sql_df(conn, columns_query)
                
                schema_data = {'tables': tables, 'columns': {}, 'table_info': {}}
                
                if not columns_df.empty:
                    table_col_name = columns_df.columns[0]
                    column_col_name = columns_df.columns[1]
                    for table in tables:
                        table_cols = columns_df[columns_df[table_col_name] == table][column_col_name].tolist()
                        schema_data['columns'][table] = table_cols
                
                for _, row in tables_df.iterrows():
                    table = row[table_col]
                    schema_data['table_info'][table] = {
                        'last_update': row.get('UPDATE_TIME'),
                        'created': row.get('CREATE_TIME'),
                        'rows': row.get('TABLE_ROWS', 0) or 0,
                        'data_size': row.get('DATA_LENGTH', 0) or 0,
                        'index_size': row.get('INDEX_LENGTH', 0) or 0
                    }
                
                return schema_data
            else:
                try:
                    tables_df = read_sql_df(conn, "SHOW TABLES")
                    if not tables_df.empty:
                        table_col = tables_df.columns[0]
                        tables = tables_df[table_col].tolist()
                        return {'tables': tables, 'columns': {}, 'table_info': {}}
                except Exception:
                    pass
                return {'tables': [], 'columns': {}, 'table_info': {}}
                
    except Exception:
        return {'tables': [], 'columns': {}, 'table_info': {}}

def execute_reconnect_scripts(environment, environments_config, aws_creds=None):
    """Execute AWS SSM session for selected environment"""
    if aws_creds:
        access_key = aws_creds['access_key']
        secret_key = aws_creds['secret_key']
        session_token = aws_creds['session_token']
    else:
        missing_vars = [key for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN'] if not os.environ.get(key)]
        if missing_vars:
            return False, f"Missing AWS credentials: {', '.join(missing_vars)}"
        access_key = os.environ['AWS_ACCESS_KEY_ID']
        secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
        session_token = os.environ['AWS_SESSION_TOKEN']
    
    env_config = environments_config[environment]
    local_port = env_config["local_port"]
    target = env_config["target"]
    host = env_config["host"]
    region = env_config["region"]
    
    params = '{"host":["' + host + '"],"portNumber":["3306"], "localPortNumber":["' + local_port + '"]}'
    cmd = f'AWS_ACCESS_KEY_ID="{access_key}" AWS_SECRET_ACCESS_KEY="{secret_key}" AWS_SESSION_TOKEN="{session_token}" aws ssm start-session --target {target} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters \'{params}\' --region {region}'
    
    try:
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(5)
        
        if process.poll() is None:
            return True, local_port
        else:
            stdout, stderr = process.communicate()
            error_msg = stderr.decode()
            return False, error_msg
            
    except Exception as e:
        return False, str(e)