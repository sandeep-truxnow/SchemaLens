"""Legacy code analysis service - DEPRECATED"""
# This file is deprecated. All functionality has been moved to git_analysis_service.py
# Keeping this file for backward compatibility during transition period.

import warnings
from services.git_analysis_service import CodeImpactAnalyzer

warnings.warn(
    "code_analysis_service is deprecated. Use git_analysis_service.CodeImpactAnalyzer instead.",
    DeprecationWarning,
    stacklevel=2
)

# Legacy wrapper functions for backward compatibility
def analyze_table_impact(repo_path, table_name, file_extensions):
    """DEPRECATED: Use CodeImpactAnalyzer.analyze_table_impact_local instead"""
    analyzer = CodeImpactAnalyzer()
    return analyzer.analyze_table_impact_local(repo_path, table_name, file_extensions)

def analyze_column_impact(repo_path, table_name, column_name, file_extensions):
    """DEPRECATED: Use CodeImpactAnalyzer.analyze_column_impact_local instead"""
    analyzer = CodeImpactAnalyzer()
    return analyzer.analyze_column_impact_local(repo_path, table_name, column_name, file_extensions)

def find_unused_objects(repo_path, all_tables, all_columns, file_extensions):
    """DEPRECATED: Use CodeImpactAnalyzer.find_unused_objects_local instead"""
    analyzer = CodeImpactAnalyzer()
    return analyzer.find_unused_objects_local(repo_path, all_tables, all_columns, file_extensions)