"""Code impact analysis service"""
import os
import re

def analyze_table_impact(repo_path, table_name, file_extensions):
    """Find all code references to a specific table"""
    results = {'files': [], 'total_references': 0}
    table_patterns = [
        rf'\b{table_name}\b',
        rf'FROM\s+{table_name}\b',
        rf'JOIN\s+{table_name}\b',
        rf'UPDATE\s+{table_name}\b',
        rf'INSERT\s+INTO\s+{table_name}\b',
        rf'DELETE\s+FROM\s+{table_name}\b',
        rf'@Table\s*\(\s*name\s*=\s*["\'{table_name}["\']\)',
        rf'table_name\s*=\s*["\'{table_name}["\']\)'
    ]
    
    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in {'.git', 'node_modules', 'target', 'build', '.idea', '__pycache__'}]
        
        for file in files:
            if any(file.endswith(ext) for ext in file_extensions):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        
                    matches = []
                    for pattern in table_patterns:
                        for match in re.finditer(pattern, content, re.IGNORECASE):
                            line_num = content[:match.start()].count('\n') + 1
                            line_content = content.split('\n')[line_num - 1].strip()
                            matches.append({'line': line_num, 'content': line_content, 'pattern': pattern})
                    
                    if matches:
                        rel_path = os.path.relpath(file_path, repo_path)
                        results['files'].append({
                            'path': rel_path,
                            'matches': matches,
                            'count': len(matches)
                        })
                        results['total_references'] += len(matches)
                        
                except Exception:
                    continue
    
    return results

def analyze_column_impact(repo_path, table_name, column_name, file_extensions):
    """Find all code references to a specific column"""
    results = {'files': [], 'total_references': 0}
    column_patterns = [
        rf'\b{column_name}\b',
        rf'SELECT.*{column_name}\b',
        rf'WHERE.*{column_name}\b',
        rf'ORDER\s+BY.*{column_name}\b',
        rf'GROUP\s+BY.*{column_name}\b',
        rf'@Column\s*\(\s*name\s*=\s*["\'{column_name}["\']\)',
        rf'column\s*=\s*["\'{column_name}["\']\)',
        rf'{table_name}\.{column_name}\b'
    ]
    
    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in {'.git', 'node_modules', 'target', 'build', '.idea', '__pycache__'}]
        
        for file in files:
            if any(file.endswith(ext) for ext in file_extensions):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        
                    matches = []
                    for pattern in column_patterns:
                        for match in re.finditer(pattern, content, re.IGNORECASE):
                            line_num = content[:match.start()].count('\n') + 1
                            line_content = content.split('\n')[line_num - 1].strip()
                            matches.append({'line': line_num, 'content': line_content, 'pattern': pattern})
                    
                    if matches:
                        rel_path = os.path.relpath(file_path, repo_path)
                        results['files'].append({
                            'path': rel_path,
                            'matches': matches,
                            'count': len(matches)
                        })
                        results['total_references'] += len(matches)
                        
                except Exception:
                    continue
    
    return results

def find_unused_objects(repo_path, all_tables, all_columns, file_extensions):
    """Find database objects not referenced in code"""
    all_code_content = ""
    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in {'.git', 'node_modules', 'target', 'build', '.idea', '__pycache__'}]
        
        for file in files:
            if any(file.endswith(ext) for ext in file_extensions):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        all_code_content += f.read().lower() + "\n"
                except Exception:
                    continue
    
    unused_tables = []
    for table in all_tables:
        table_name = table.split('.')[-1]
        if table_name.lower() not in all_code_content:
            unused_tables.append(table)
    
    unused_columns = []
    for column in all_columns:
        column_name = column.split('.')[-1]
        if column_name.lower() not in all_code_content:
            unused_columns.append(column)
    
    return {
        'unused_tables': unused_tables,
        'unused_columns': unused_columns[:100],
        'total_tables': len(all_tables),
        'total_columns': len(all_columns)
    }