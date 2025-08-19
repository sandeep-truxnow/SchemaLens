"""Code Impact Analysis UI module"""

import streamlit as st
import pandas as pd
import os
import time
from services.database_service import load_schema_metadata
from services.git_analysis_service import GitAnalysisService, CodeImpactAnalyzer


def render_impact_analysis_tab():
    """Render Code Impact Analysis tab"""
    st.header("🔍 Code Impact Analysis")
    st.caption("Analyze which services reference database tables/columns and find unused database objects")
    
    # Analysis Configuration
    analysis_config = _render_analysis_configuration()
    
    # File extensions selection
    file_extensions = _render_file_extensions_selection()
    
    # Repository Configuration
    repo_config, repo_path = _render_repository_configuration()
    
    # Run Analysis
    if st.button("🔍 Run Analysis", type="primary", key="run_analysis_btn"):
        _handle_analysis_execution(analysis_config, file_extensions, repo_config, repo_path)


def _render_analysis_configuration():
    """Render analysis configuration section"""
    st.subheader("🎯 Analysis Configuration")
    col1, col2 = st.columns(2)
    
    with col1:
        analysis_type = st.radio(
            "Analysis Type",
            ["Table Impact Analysis", "Column Impact Analysis", "Unused Objects Detection"],
            key="analysis_type_selection"
        )
    
    with col2:
        target_config = _render_target_selection(analysis_type)
    
    return {
        'type': analysis_type,
        **target_config
    }


def _render_target_selection(analysis_type):
    """Render target selection based on analysis type"""
    if analysis_type in ["Table Impact Analysis", "Column Impact Analysis"]:
        if st.session_state.available_schemas:
            selected_schema = st.selectbox(
                "Schema", 
                st.session_state.available_schemas,
                key="selected_schema_analysis"
            )
            
            # Load schema metadata if not cached
            schema_data = _load_schema_metadata_for_analysis(selected_schema)
            tables = schema_data.get('tables', [])
            
            if analysis_type == "Table Impact Analysis":
                target_table = st.selectbox(
                    "Select Table", 
                    tables,
                    key="target_table_analysis"
                )
                return {'schema': selected_schema, 'table': target_table}
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
                    return {'schema': selected_schema, 'table': target_table, 'column': target_column}
                return {'schema': selected_schema, 'table': target_table, 'column': None}
    
    return {}


def _load_schema_metadata_for_analysis(selected_schema):
    """Load schema metadata for analysis"""
    cache_key = f"{st.session_state.connection_params.get('environment', 'QA')}_{selected_schema}"
    if cache_key not in st.session_state.get('schema_metadata', {}):
        with st.spinner(f"Loading {selected_schema} metadata..."):
            schema_data = load_schema_metadata(selected_schema, st.session_state.connection_params)
            st.session_state.schema_metadata[cache_key] = schema_data
    
    return st.session_state.schema_metadata[cache_key]


def _render_file_extensions_selection():
    """Render file extensions selection"""
    return st.multiselect(
        "File Extensions to Scan",
        options=[".java", ".py", ".js", ".ts", ".sql", ".xml", ".yml", ".yaml"],
        default=[".java", ".py", ".sql"],
        help="Select file types to search for database references",
        key="file_extensions_analysis"
    )


def _render_repository_configuration():
    """Render repository configuration section"""
    st.subheader("📂 Repository Configuration")
    
    repo_source = st.radio(
        "Repository Source",
        ["Git Repository (Remote)", "Local Directory"],
        help="Choose between remote Git repository or local directory"
    )
    
    if repo_source == "Git Repository (Remote)":
        return _render_remote_repo_config(), None
    else:
        return None, _render_local_repo_config()


def _render_remote_repo_config():
    """Render remote repository configuration"""
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
    
    # Repository URL and authentication
    repo_config = _handle_git_provider_config(git_provider, clone_method)
    
    return repo_config


def _handle_git_provider_config(git_provider, clone_method):
    """Handle git provider specific configuration"""
    if git_provider == "GitHub":
        return _handle_github_config(clone_method)
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
    
    # Handle authentication and branch
    git_token = _handle_authentication(clone_method, git_provider)
    git_branch = st.text_input("Branch", value="main", help="Git branch to clone (default: main)")
    
    if git_provider != "GitHub":
        if repo_url:
            st.success(f"✓ Ready to analyze repository: {repo_url}")
        else:
            st.info("📝 Enter repository URL above")
        
        return {'type': 'git_repo', 'url': repo_url, 'branch': git_branch, 'token': git_token} if repo_url else None
    
    return None


def _handle_github_config(clone_method):
    """Handle GitHub specific configuration"""
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
        git_token = _handle_authentication(clone_method, "GitHub", analysis_scope)
        git_branch = st.text_input("Branch", value="main", help="Git branch to clone (default: main)")
        
        if repo_url:
            st.success(f"✓ Ready to analyze repository: {repo_url}")
        else:
            st.info("📝 Enter repository URL above")
        
        return {'type': 'git_repo', 'url': repo_url, 'branch': git_branch, 'token': git_token} if repo_url else None
    else:
        org_name = st.text_input(
            "Organization Name",
            placeholder="company-name",
            help="GitHub organization name (without https://github.com/)"
        )
        st.warning("⚠️ Organization analysis requires authentication. Please provide your GitHub Personal Access Token below.")
        
        git_token = _handle_authentication(clone_method, "GitHub", analysis_scope)
        git_branch = st.text_input("Branch", value="main", help="Git branch to clone (default: main)")
        
        if org_name and git_token:
            st.success(f"✓ Ready to analyze organization: {org_name}")
        elif org_name and not git_token:
            st.warning("⚠️ GitHub token required for organization analysis")
        elif not org_name:
            st.info("📝 Enter organization name above")
        
        return {'type': 'github_org', 'org_name': org_name, 'branch': git_branch, 'token': git_token} if org_name and git_token else None


def _handle_authentication(clone_method, git_provider, analysis_scope=None):
    """Handle authentication configuration"""
    git_token = None
    
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
    
    return git_token


def _render_local_repo_config():
    """Render local repository configuration"""
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
    
    return repo_path


def _handle_analysis_execution(analysis_config, file_extensions, repo_config, repo_path):
    """Handle analysis execution"""
    # Validate inputs
    if repo_config is None and (not repo_path or not os.path.exists(repo_path)):
        st.error("Please configure repository settings above")
        return
    
    if not file_extensions:
        st.error("Please select at least one file extension")
        return
    
    try:
        analyzer = CodeImpactAnalyzer()
        
        # Get repository data if remote
        repo_data = None
        if repo_config:
            repo_data = _fetch_repository_data(repo_config)
        
        # Analyze code for database references
        _perform_analysis(analyzer, analysis_config, file_extensions, repo_data, repo_path)
        
    except Exception as e:
        st.error(f"Analysis failed: {str(e)}")


def _fetch_repository_data(repo_config):
    """Fetch repository data from remote source"""
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
    
    return repo_data


def _perform_analysis(analyzer, analysis_config, file_extensions, repo_data, repo_path):
    """Perform the actual code analysis"""
    with st.spinner("Analyzing database references..."):
        if analysis_config['type'] == "Table Impact Analysis":
            _perform_table_impact_analysis(analyzer, analysis_config, file_extensions, repo_data, repo_path)
        elif analysis_config['type'] == "Column Impact Analysis":
            _perform_column_impact_analysis(analyzer, analysis_config, file_extensions, repo_data, repo_path)
        else:  # Unused Objects Detection
            _perform_unused_objects_analysis(analyzer, file_extensions, repo_data, repo_path)


def _perform_table_impact_analysis(analyzer, analysis_config, file_extensions, repo_data, repo_path):
    """Perform table impact analysis"""
    target_table = analysis_config['table']
    
    if repo_data:
        results = analyzer.analyze_table_impact_api(repo_data, target_table, file_extensions)
    else:
        results = analyzer.analyze_table_impact_local(repo_path, target_table, file_extensions)
    
    _display_table_impact_results(results, target_table)


def _perform_column_impact_analysis(analyzer, analysis_config, file_extensions, repo_data, repo_path):
    """Perform column impact analysis"""
    target_table = analysis_config['table']
    target_column = analysis_config['column']
    
    if repo_data:
        results = analyzer.analyze_column_impact_api(repo_data, target_table, target_column, file_extensions)
    else:
        results = analyzer.analyze_column_impact_local(repo_path, target_table, target_column, file_extensions)
    
    _display_column_impact_results(results, target_table, target_column)


def _perform_unused_objects_analysis(analyzer, file_extensions, repo_data, repo_path):
    """Perform unused objects analysis"""
    if st.session_state.available_schemas:
        all_tables, all_columns = _collect_all_database_objects()
        
        if repo_data:
            results = analyzer.find_unused_objects_api(repo_data, all_tables, all_columns, file_extensions)
        else:
            results = analyzer.find_unused_objects_local(repo_path, all_tables, all_columns, file_extensions)
        
        _display_unused_objects_results(results)


def _collect_all_database_objects():
    """Collect all database tables and columns"""
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
    
    return all_tables, all_columns


def _display_table_impact_results(results, table_name):
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


def _display_column_impact_results(results, table_name, column_name):
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


def _display_unused_objects_results(results):
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