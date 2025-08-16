"""Git-based code impact analysis service"""
import os
import re
import time
import requests
import base64
import concurrent.futures
from datetime import datetime, timedelta, timezone


class GitAnalysisService:
    """Service for analyzing code repositories via Git APIs"""
    
    def __init__(self, token=None):
        """Initialize with optional Git token"""
        self.token = token or os.getenv('GIT_TOKEN') or os.getenv('GITHUB_TOKEN')
        self.headers = {}
        if self.token:
            self.headers['Authorization'] = f'token {self.token}'
    
    def analyze_repository(self, repo_url, branch='main'):
        """Analyze repository using Git provider APIs"""
        try:
            if "github.com" in repo_url:
                return self._analyze_github_repo(repo_url, branch)
            elif "gitlab.com" in repo_url:
                return self._analyze_gitlab_repo(repo_url, branch)
            else:
                raise Exception("API analysis only supports GitHub and GitLab")
        except Exception as e:
            raise Exception(f"Repository analysis failed: {str(e)}")
    
    def analyze_organization(self, org_name, branch='main'):
        """Analyze active repositories in a GitHub organization"""
        try:
            repos = self._get_active_repositories(org_name)
            return self._analyze_repositories_parallel(org_name, repos, branch)
        except Exception as e:
            raise Exception(f"Organization analysis failed: {str(e)}")
    
    def _get_active_repositories(self, org_name):
        """Get active repositories from organization"""
        repos_url = f"https://api.github.com/orgs/{org_name}/repos?sort=updated&per_page=50"
        response = requests.get(repos_url, headers=self.headers, timeout=10)
        
        if response.status_code != 200:
            raise Exception(f"GitHub API error: {response.status_code}")
        
        repositories = response.json()
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=180)
        active_repos = []
        
        for repo in repositories:
            updated_at = datetime.fromisoformat(repo['updated_at'].replace('Z', '+00:00'))
            if updated_at > cutoff_date and not repo.get('archived', False):
                active_repos.append({
                    'name': repo['name'],
                    'default_branch': repo['default_branch'],
                    'language': repo.get('language', 'Unknown')
                })
        
        return active_repos[:10]  # Limit to top 10
    
    def _analyze_repositories_parallel(self, org_name, repos, branch):
        """Analyze multiple repositories in parallel"""
        combined_results = {'files': [], 'total_repos': len(repos)}
        
        def analyze_single_repo(repo_info):
            try:
                repo_name = repo_info['name']
                repo_branch = repo_info['default_branch']
                repo_data = self._analyze_github_repo_fast(org_name, repo_name, repo_branch)
                
                for file_info in repo_data['files']:
                    file_info['repository'] = repo_name
                    file_info['path'] = f"{repo_name}/{file_info['path']}"
                
                return repo_data['files']
            except Exception:
                return []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_repo = {executor.submit(analyze_single_repo, repo): repo for repo in repos}
            
            try:
                for future in concurrent.futures.as_completed(future_to_repo, timeout=10):
                    try:
                        files = future.result(timeout=1)
                        combined_results['files'].extend(files)
                    except Exception:
                        continue
            except concurrent.futures.TimeoutError:
                pass
        
        return combined_results
    
    def _analyze_github_repo(self, repo_url, branch):
        """Analyze GitHub repository via API"""
        owner, repo = self._parse_github_url(repo_url)
        api_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
        response = requests.get(api_url, headers=self.headers)
        
        if response.status_code != 200:
            raise Exception(f"GitHub API error: {response.status_code}")
        
        tree_data = response.json()
        file_paths = [item['path'] for item in tree_data.get('tree', []) if item['type'] == 'blob']
        
        return self._analyze_files_via_api(owner, repo, branch, file_paths, 'github')
    
    def _analyze_github_repo_fast(self, owner, repo, branch):
        """Fast repository analysis - only key file types"""
        api_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
        response = requests.get(api_url, headers=self.headers, timeout=5)
        
        if response.status_code != 200:
            return {'files': []}
        
        tree_data = response.json()
        relevant_files = self._filter_relevant_files(tree_data.get('tree', []))
        
        files_data = []
        for file_path in relevant_files[:20]:  # Limit to 20 files
            try:
                content = self._get_file_content_github(owner, repo, file_path, branch)
                if content:
                    files_data.append({
                        'path': file_path,
                        'content': content[:5000],  # First 5KB only
                        'size': len(content)
                    })
            except Exception:
                continue
        
        return {'files': files_data}
    
    def _analyze_gitlab_repo(self, repo_url, branch):
        """Analyze GitLab repository via API"""
        project_path = self._parse_gitlab_url(repo_url)
        project_id = self._encode_project_path(project_path)
        
        api_url = f"https://gitlab.com/api/v4/projects/{project_id}/repository/tree?recursive=true&ref={branch}"
        response = requests.get(api_url, headers=self.headers)
        
        if response.status_code != 200:
            raise Exception(f"GitLab API error: {response.status_code}")
        
        tree_data = response.json()
        file_paths = [item['path'] for item in tree_data if item['type'] == 'blob']
        
        return self._analyze_files_via_api(project_id, None, branch, file_paths, 'gitlab')
    
    def _analyze_files_via_api(self, owner_or_id, repo, branch, file_paths, provider):
        """Analyze files using API without downloading"""
        results = {'files': [], 'total_references': 0}
        target_extensions = ['.java', '.py', '.sql', '.js', '.ts']
        filtered_files = [f for f in file_paths if any(f.endswith(ext) for ext in target_extensions)]
        
        for file_path in filtered_files[:50]:  # Limit to 50 files
            try:
                content = self._get_file_content(owner_or_id, repo, file_path, branch, provider)
                if content:
                    results['files'].append({
                        'path': file_path,
                        'content': content[:10000],  # First 10KB only
                        'size': len(content)
                    })
            except Exception:
                continue
        
        return results
    
    def _get_file_content(self, owner_or_id, repo, file_path, branch, provider):
        """Get file content from API"""
        if provider == 'github':
            return self._get_file_content_github(owner_or_id, repo, file_path, branch)
        else:  # gitlab
            return self._get_file_content_gitlab(owner_or_id, file_path, branch)
    
    def _get_file_content_github(self, owner, repo, file_path, branch):
        """Get file content from GitHub API"""
        file_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{file_path}?ref={branch}"
        response = requests.get(file_url, headers=self.headers, timeout=3)
        
        if response.status_code == 200:
            return base64.b64decode(response.json()['content']).decode('utf-8', errors='ignore')
        return None
    
    def _get_file_content_gitlab(self, project_id, file_path, branch):
        """Get file content from GitLab API"""
        encoded_path = file_path.replace('/', '%2F')
        file_url = f"https://gitlab.com/api/v4/projects/{project_id}/repository/files/{encoded_path}/raw?ref={branch}"
        response = requests.get(file_url, headers=self.headers, timeout=3)
        
        if response.status_code == 200:
            return response.text
        return None
    
    def _filter_relevant_files(self, tree_items):
        """Filter for key file types and directories"""
        key_extensions = ['.java', '.py', '.sql', '.js', '.ts']
        key_dirs = ['src/', 'main/', 'service/', 'controller/', 'repository/', 'dao/', 'model/']
        
        relevant_files = []
        for item in tree_items:
            if item['type'] == 'blob':
                path = item['path']
                if (any(path.endswith(ext) for ext in key_extensions) and 
                    (any(key_dir in path for key_dir in key_dirs) or len(path.split('/')) <= 2)):
                    relevant_files.append(path)
        
        return relevant_files
    
    def _parse_github_url(self, repo_url):
        """Parse GitHub URL to extract owner and repo"""
        match = re.search(r'github\.com[/:]([^/]+)/([^/.]+)', repo_url)
        if not match:
            raise Exception("Invalid GitHub URL format")
        return match.groups()
    
    def _parse_gitlab_url(self, repo_url):
        """Parse GitLab URL to extract project path"""
        match = re.search(r'gitlab\.com[/:](.+)\.git$', repo_url)
        if not match:
            match = re.search(r'gitlab\.com[/:](.+)$', repo_url)
        
        if not match:
            raise Exception("Invalid GitLab URL format")
        return match.group(1)
    
    def _encode_project_path(self, project_path):
        """Encode project path for GitLab API"""
        import urllib.parse
        return urllib.parse.quote(project_path, safe='')


class CodeImpactAnalyzer:
    """Service for analyzing code impact of database objects"""
    
    def __init__(self):
        self.table_patterns = [
            r'\b{}\b',
            r'FROM\s+{}\b',
            r'JOIN\s+{}\b',
            r'UPDATE\s+{}\b',
            r'INSERT\s+INTO\s+{}\b',
            r'DELETE\s+FROM\s+{}\b',
            r'@Table\s*\(\s*name\s*=\s*["\'{}\s*["\']\)',
            r'table_name\s*=\s*["\'{}\s*["\']\)'
        ]
        
        self.column_patterns = [
            r'\b{}\b',
            r'SELECT.*{}\b',
            r'WHERE.*{}\b',
            r'ORDER\s+BY.*{}\b',
            r'GROUP\s+BY.*{}\b',
            r'@Column\s*\(\s*name\s*=\s*["\'{}\s*["\']\)',
            r'column\s*=\s*["\'{}\s*["\']\)',
            r'{}\\.{}\b'
        ]
    
    def analyze_table_impact_local(self, repo_path, table_name, file_extensions):
        """Find all code references to a specific table in local repository"""
        results = {'files': [], 'total_references': 0}
        patterns = [pattern.format(table_name) for pattern in self.table_patterns]
        
        for root, dirs, files in os.walk(repo_path):
            dirs[:] = self._filter_directories(dirs)
            
            for file in files:
                if self._should_scan_file(file, file_extensions):
                    file_path = os.path.join(root, file)
                    matches = self._find_pattern_matches(file_path, patterns)
                    
                    if matches:
                        rel_path = os.path.relpath(file_path, repo_path)
                        results['files'].append({
                            'path': rel_path,
                            'matches': matches,
                            'count': len(matches)
                        })
                        results['total_references'] += len(matches)
        
        return results
    
    def analyze_table_impact_api(self, repo_data, table_name, file_extensions):
        """Find all code references to a specific table using API data"""
        results = {'files': [], 'total_references': 0}
        patterns = [pattern.format(table_name) for pattern in self.table_patterns]
        
        for file_info in repo_data['files']:
            if self._should_scan_file(file_info['path'], file_extensions):
                matches = self._find_pattern_matches_in_content(file_info['content'], patterns)
                
                if matches:
                    results['files'].append({
                        'path': file_info['path'],
                        'matches': matches,
                        'count': len(matches)
                    })
                    results['total_references'] += len(matches)
        
        return results
    
    def analyze_column_impact_local(self, repo_path, table_name, column_name, file_extensions):
        """Find all code references to a specific column in local repository"""
        results = {'files': [], 'total_references': 0}
        patterns = self._format_column_patterns(table_name, column_name)
        
        for root, dirs, files in os.walk(repo_path):
            dirs[:] = self._filter_directories(dirs)
            
            for file in files:
                if self._should_scan_file(file, file_extensions):
                    file_path = os.path.join(root, file)
                    matches = self._find_pattern_matches(file_path, patterns)
                    
                    if matches:
                        rel_path = os.path.relpath(file_path, repo_path)
                        results['files'].append({
                            'path': rel_path,
                            'matches': matches,
                            'count': len(matches)
                        })
                        results['total_references'] += len(matches)
        
        return results
    
    def analyze_column_impact_api(self, repo_data, table_name, column_name, file_extensions):
        """Find all code references to a specific column using API data"""
        results = {'files': [], 'total_references': 0}
        patterns = self._format_column_patterns(table_name, column_name)
        
        for file_info in repo_data['files']:
            if self._should_scan_file(file_info['path'], file_extensions):
                matches = self._find_pattern_matches_in_content(file_info['content'], patterns)
                
                if matches:
                    results['files'].append({
                        'path': file_info['path'],
                        'matches': matches,
                        'count': len(matches)
                    })
                    results['total_references'] += len(matches)
        
        return results
    
    def find_unused_objects_local(self, repo_path, all_tables, all_columns, file_extensions):
        """Find database objects not referenced in local code"""
        all_code_content = self._collect_all_code_content(repo_path, file_extensions)
        return self._identify_unused_objects(all_code_content, all_tables, all_columns)
    
    def find_unused_objects_api(self, repo_data, all_tables, all_columns, file_extensions):
        """Find database objects not referenced in API code"""
        all_code_content = ""
        for file_info in repo_data['files']:
            if self._should_scan_file(file_info['path'], file_extensions):
                all_code_content += file_info['content'].lower() + "\n"
        
        return self._identify_unused_objects(all_code_content, all_tables, all_columns)
    
    def _filter_directories(self, dirs):
        """Filter out directories to skip during scanning"""
        return [d for d in dirs if d not in {'.git', 'node_modules', 'target', 'build', '.idea', '__pycache__'}]
    
    def _should_scan_file(self, file_path, file_extensions):
        """Check if file should be scanned based on extension"""
        return any(file_path.endswith(ext) for ext in file_extensions)
    
    def _find_pattern_matches(self, file_path, patterns):
        """Find pattern matches in a file"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            return self._find_pattern_matches_in_content(content, patterns)
        except Exception:
            return []
    
    def _find_pattern_matches_in_content(self, content, patterns):
        """Find pattern matches in content string"""
        matches = []
        for pattern in patterns:
            for match in re.finditer(pattern, content, re.IGNORECASE):
                line_num = content[:match.start()].count('\n') + 1
                line_content = content.split('\n')[line_num - 1].strip()
                matches.append({
                    'line': line_num, 
                    'content': line_content, 
                    'pattern': pattern
                })
        return matches
    
    def _collect_all_code_content(self, repo_path, file_extensions):
        """Collect all code content from local repository"""
        all_code_content = ""
        for root, dirs, files in os.walk(repo_path):
            dirs[:] = self._filter_directories(dirs)
            
            for file in files:
                if self._should_scan_file(file, file_extensions):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            all_code_content += f.read().lower() + "\n"
                    except Exception:
                        continue
        
        return all_code_content
    
    def _format_column_patterns(self, table_name, column_name):
        """Format column patterns with proper substitution"""
        formatted_patterns = []
        for pattern in self.column_patterns:
            if pattern.count('{}') == 2:
                # Pattern expects both table and column name
                formatted_patterns.append(pattern.format(table_name, column_name))
            else:
                # Pattern expects only column name
                formatted_patterns.append(pattern.format(column_name))
        return formatted_patterns
    
    def _identify_unused_objects(self, all_code_content, all_tables, all_columns):
        """Identify unused database objects"""
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
            'unused_columns': unused_columns[:100],  # Limit for performance
            'total_tables': len(all_tables),
            'total_columns': len(all_columns)
        }