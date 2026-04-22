"""Full-history CVS to Git to Bitbucket migration for Python 2.7."""

from __future__ import print_function

import argparse
import datetime
import errno
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import traceback
from distutils.spawn import find_executable

try:
    import requests
except ImportError:
    requests = None

try:
    import urllib2
except ImportError:
    urllib2 = None

try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote

try:
    unicode
except NameError:  # pragma: no cover - Python 3 compatibility
    unicode = str


LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'
DEFAULT_ENGINE = 'cvs-fast-export'


class MigrationError(Exception):
    """Raised for controlled migration failures."""


class ApiError(MigrationError):
    """Raised for HTTP/API failures."""

    def __init__(self, message, status_code=None, body=None):
        MigrationError.__init__(self, message)
        self.status_code = status_code
        self.body = body


def safe_makedirs(path):
    """Create a directory if it does not already exist."""
    if not path:
        return
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise


def relpath(path, start):
    """Return a relative path in a Python 2.7-compatible way."""
    return os.path.relpath(path, start)


def sanitize_repo_name(value):
    """Convert a path-like value to a Bitbucket-friendly repository slug."""
    value = value.replace('\\', '/').strip('/')
    value = value.replace('/', '-')
    value = re.sub(r'[^A-Za-z0-9._-]+', '-', value)
    value = re.sub(r'-{2,}', '-', value).strip('-')
    if not value:
        value = 'cvs-repository'
    return value.lower()


def load_json(path):
    """Load JSON from disk."""
    handle = open(path, 'rb')
    try:
        return json.load(handle)
    finally:
        handle.close()


def dump_json(path, payload):
    """Write JSON to disk."""
    handle = open(path, 'wb')
    try:
        handle.write(json.dumps(payload, indent=2, sort_keys=True))
    finally:
        handle.close()


def write_text(path, content):
    """Write text content to disk."""
    handle = open(path, 'wb')
    try:
        if isinstance(content, unicode):
            handle.write(content.encode('utf-8'))
        else:
            handle.write(content)
    finally:
        handle.close()


def normalize_path(path_value):
    """Expand and normalize a configured path."""
    return os.path.abspath(os.path.expanduser(path_value))


def ensure_clean_directory(path):
    """Recreate a clean directory tree."""
    if os.path.isdir(path):
        shutil.rmtree(path)
    safe_makedirs(path)


def record_command(repo_report, command, cwd):
    """Store a command execution in the repository report."""
    repo_report['commands'].append({
        'command': command,
        'cwd': cwd,
    })


def add_step(repo_report, message, level='INFO'):
    """Append a step to the repository report and log it."""
    repo_report['steps'].append(message)
    logger = repo_report.get('logger')
    if logger:
        log_method = getattr(logger, level.lower(), logger.info)
        log_method('[%s] %s', repo_report['repository_name'], message)


def add_warning(repo_report, message):
    """Append a warning to the repository report."""
    repo_report['warnings'].append(message)
    add_step(repo_report, message, level='WARNING')


def add_api_call(container, method, path, status):
    """Record a Bitbucket API call in a report container."""
    container['api_calls'].append({
        'method': method,
        'path': path,
        'status': status,
    })


def run_command(command, cwd=None, env=None, logger=None, repo_report=None, stdin_handle=None):
    """Run a subprocess and raise on failure."""
    if repo_report is not None:
        record_command(repo_report, command, cwd)
    if logger:
        logger.debug('Running command: %s', ' '.join(command))
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdin=stdin_handle,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout_data, stderr_data = process.communicate()
    if logger and stdout_data:
        logger.debug('Command stdout: %s', stdout_data.strip())
    if logger and stderr_data:
        logger.debug('Command stderr: %s', stderr_data.strip())
    if process.returncode != 0:
        raise MigrationError(
            'Command failed with exit code {0}: {1}\nSTDOUT: {2}\nSTDERR: {3}'.format(
                process.returncode,
                ' '.join(command),
                stdout_data.strip(),
                stderr_data.strip(),
            )
        )
    return stdout_data, stderr_data


def discover_cvs_repositories(project_path):
    """Discover CVS repositories by locating directories that contain CVSROOT."""
    repositories = []
    for root, dirs, _files in os.walk(project_path):
        dirs.sort()
        if 'CVSROOT' in dirs:
            repositories.append(root)
            dirs[:] = [item for item in dirs if item != 'CVSROOT']
    repositories.sort()
    return repositories


def derive_repository_name(project_path, repository_path):
    """Derive a deterministic target repository name from the CVS path."""
    relative = relpath(repository_path, project_path)
    if relative == '.':
        relative = os.path.basename(project_path)
    return sanitize_repo_name(relative)


def iter_repository_rcs_files(repository_path):
    """Yield all RCS files for historical migration, excluding CVSROOT metadata."""
    for root, dirs, files in os.walk(repository_path):
        dirs.sort()
        if root == repository_path:
            dirs[:] = [item for item in dirs if item != 'CVSROOT']
        relative_root = relpath(root, repository_path)
        if relative_root == 'CVSROOT':
            continue
        for filename in sorted(files):
            if not filename.endswith(',v'):
                continue
            source_path = os.path.join(root, filename)
            yield source_path, relpath(source_path, repository_path)


def collect_cvs_authors(repository_path):
    """Collect all CVS authors seen in RCS files."""
    authors = set()
    for source_path, _relative_path in iter_repository_rcs_files(repository_path):
        handle = open(source_path, 'rb')
        try:
            content = handle.read()
        finally:
            handle.close()
        if not isinstance(content, unicode):
            content = content.decode('utf-8', 'replace')
        for author in re.findall(r'author\s+([^;]+);', content):
            authors.add(author.strip())
    return sorted(authors)


def format_identity(identity):
    """Render an identity dictionary in DVCS author-map format."""
    line = '{0} = {1} <{2}>'.format(
        identity['legacy_name'],
        identity['name'],
        identity['email']
    )
    if identity.get('timezone'):
        line += ' {0}'.format(identity['timezone'])
    return line


def build_effective_author_map(config):
    """Build the effective author map from LDAP and explicit overrides."""
    effective = {}
    ldap_config = config.get('ldap') or {}
    ldap_map = ldap_config.get('author_map') or ldap_config.get('users') or {}
    if isinstance(ldap_map, dict):
        effective.update(ldap_map)
    configured_map = config.get('author_map', {})
    if isinstance(configured_map, dict):
        effective.update(configured_map)
    return effective


def build_authormap(repository_path, config, repo_report, authormap_path):
    """Build the cvs-fast-export authormap with fallback identities."""
    configured_map = build_effective_author_map(config)
    default_committer = config['default_committer']
    authors = collect_cvs_authors(repository_path)
    fallback_authors = []
    authormap = []
    ldap_entries = len((config.get('ldap') or {}).get('author_map') or (config.get('ldap') or {}).get('users') or {})
    if ldap_entries:
        add_step(repo_report, 'Loaded {0} LDAP author mapping entries'.format(ldap_entries))
    for author in authors:
        mapped = configured_map.get(author)
        if mapped and mapped.get('name') and mapped.get('email'):
            entry = {
                'legacy_name': author,
                'name': mapped['name'],
                'email': mapped['email'],
                'timezone': mapped.get('timezone'),
                'source': 'author_map',
            }
        else:
            fallback_authors.append(author)
            entry = {
                'legacy_name': author,
                'name': default_committer['name'],
                'email': default_committer['email'],
                'timezone': default_committer.get('timezone'),
                'source': 'fallback',
            }
        authormap.append(entry)
    lines = ['# Generated author map for cvs-fast-export']
    for entry in authormap:
        lines.append(format_identity(entry))
    write_text(authormap_path, '\n'.join(lines) + '\n')
    add_step(repo_report, 'Collected {0} distinct CVS authors'.format(len(authors)))
    if fallback_authors:
        add_step(
            repo_report,
            'Mapped {0} unknown CVS authors to fallback {1} <{2}>'.format(
                len(fallback_authors),
                default_committer['name'],
                default_committer['email'],
            )
        )
    else:
        add_step(repo_report, 'No fallback author mapping was required')
    repo_report['author_mapping'] = {
        'authors': authors,
        'fallback_authors': fallback_authors,
        'authormap_path': authormap_path,
    }
    return authormap


def write_file_list(repository_path, file_list_path):
    """Write the RCS file list consumed by cvs-fast-export."""
    entries = [relative_path for _source_path, relative_path in iter_repository_rcs_files(repository_path)]
    entries.sort()
    write_text(file_list_path, '\n'.join(entries) + '\n')
    return entries


def run_cvs_fast_export(repository_path, file_list_path, authormap_path, revmap_path, stream_path, repo_report):
    """Run cvs-fast-export and capture its fast-import stream."""
    command = ['cvs-fast-export', '-A', authormap_path, '-R', revmap_path]
    record_command(repo_report, command, repository_path)
    stream_handle = open(stream_path, 'wb')
    stdin_handle = open(file_list_path, 'rb')
    try:
        process = subprocess.Popen(
            command,
            cwd=repository_path,
            stdin=stdin_handle,
            stdout=stream_handle,
            stderr=subprocess.PIPE
        )
        _stdout_data, stderr_data = process.communicate()
    finally:
        stdin_handle.close()
        stream_handle.close()
    if process.returncode != 0:
        raise MigrationError(
            'cvs-fast-export failed with exit code {0}: {1}'.format(
                process.returncode,
                stderr_data.strip()
            )
        )
    warnings = []
    for line in stderr_data.splitlines():
        if not line.strip():
            continue
        warnings.append(line)
    for warning in warnings:
        add_warning(repo_report, 'cvs-fast-export: {0}'.format(warning))
    add_step(repo_report, 'cvs-fast-export completed successfully')


def initialize_bare_git_repository(git_dir, repo_report):
    """Initialize the target bare Git repository."""
    ensure_clean_directory(git_dir)
    run_command(['git', 'init', '--bare'], cwd=git_dir, logger=repo_report['logger'], repo_report=repo_report)
    add_step(repo_report, 'Initialized bare Git repository at {0}'.format(git_dir))


def import_fast_stream(git_dir, stream_path, repo_report):
    """Import a fast-import stream into a bare Git repository."""
    stdin_handle = open(stream_path, 'rb')
    try:
        run_command(
            ['git', 'fast-import', '--quiet'],
            cwd=git_dir,
            logger=repo_report['logger'],
            repo_report=repo_report,
            stdin_handle=stdin_handle
        )
    finally:
        stdin_handle.close()
    add_step(repo_report, 'Imported cvs-fast-export stream into Git')


def list_refs(git_dir, ref_namespace, repo_report=None):
    """Return a sorted list of refs under a namespace."""
    output, _stderr = run_command(
        ['git', 'for-each-ref', '--format=%(refname:strip=2)', ref_namespace],
        cwd=git_dir,
        logger=repo_report and repo_report['logger'],
        repo_report=repo_report
    )
    refs = [line.strip() for line in output.splitlines() if line.strip()]
    refs.sort()
    return refs


def ref_exists(git_dir, full_ref, repo_report=None):
    """Check whether a ref exists."""
    process = subprocess.Popen(
        ['git', 'show-ref', '--verify', '--quiet', full_ref],
        cwd=git_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    process.communicate()
    return process.returncode == 0


def set_default_branch(git_dir, default_branch, repo_report):
    """Set the default branch and HEAD ref."""
    branches = list_refs(git_dir, 'refs/heads', repo_report=repo_report)
    if not branches:
        raise MigrationError('No branches were imported into the Git repository')
    if default_branch != 'master' and 'master' in branches and default_branch not in branches:
        run_command(
            ['git', 'branch', '-m', 'master', default_branch],
            cwd=git_dir,
            logger=repo_report['logger'],
            repo_report=repo_report
        )
        add_step(repo_report, 'Renamed master branch to {0}'.format(default_branch))
    target_branch = default_branch
    branches = list_refs(git_dir, 'refs/heads', repo_report=repo_report)
    if target_branch not in branches:
        target_branch = branches[0]
        add_warning(
            repo_report,
            'Configured default branch {0} was not found; using {1} as HEAD'.format(
                default_branch,
                target_branch,
            )
        )
    run_command(
        ['git', 'symbolic-ref', 'HEAD', 'refs/heads/{0}'.format(target_branch)],
        cwd=git_dir,
        logger=repo_report['logger'],
        repo_report=repo_report
    )
    add_step(repo_report, 'Set bare repository HEAD to {0}'.format(target_branch))
    return target_branch


def rename_tag(git_dir, old_tag, new_tag, repo_report):
    """Rename a tag by recreating it from the old tag object."""
    run_command(
        ['git', 'tag', new_tag, old_tag],
        cwd=git_dir,
        logger=repo_report['logger'],
        repo_report=repo_report
    )
    run_command(
        ['git', 'tag', '-d', old_tag],
        cwd=git_dir,
        logger=repo_report['logger'],
        repo_report=repo_report
    )


def apply_ref_mapping(git_dir, config, repo_report):
    """Apply optional branch and tag mappings after import."""
    branch_map = config.get('branch_map', {})
    tag_map = config.get('tag_map', {})
    for old_branch, new_branch in sorted(branch_map.items()):
        if ref_exists(git_dir, 'refs/heads/{0}'.format(old_branch), repo_report=repo_report):
            if ref_exists(git_dir, 'refs/heads/{0}'.format(new_branch), repo_report=repo_report):
                add_warning(
                    repo_report,
                    'Skipped branch rename from {0} to {1} because target branch already exists'.format(
                        old_branch,
                        new_branch,
                    )
                )
                continue
            run_command(
                ['git', 'branch', '-m', old_branch, new_branch],
                cwd=git_dir,
                logger=repo_report['logger'],
                repo_report=repo_report
            )
            add_step(repo_report, 'Renamed branch {0} to {1}'.format(old_branch, new_branch))
    for old_tag, new_tag in sorted(tag_map.items()):
        if ref_exists(git_dir, 'refs/tags/{0}'.format(old_tag), repo_report=repo_report):
            if ref_exists(git_dir, 'refs/tags/{0}'.format(new_tag), repo_report=repo_report):
                add_warning(
                    repo_report,
                    'Skipped tag rename from {0} to {1} because target tag already exists'.format(
                        old_tag,
                        new_tag,
                    )
                )
                continue
            rename_tag(git_dir, old_tag, new_tag, repo_report)
            add_step(repo_report, 'Renamed tag {0} to {1}'.format(old_tag, new_tag))


def validate_import(git_dir, repo_report):
    """Validate the imported repository before pushing it."""
    commit_count_output, _stderr = run_command(
        ['git', 'rev-list', '--all', '--count'],
        cwd=git_dir,
        logger=repo_report['logger'],
        repo_report=repo_report
    )
    commit_count = int(commit_count_output.strip() or '0')
    if commit_count <= 0:
        raise MigrationError('No commits were imported from CVS history')
    branches = list_refs(git_dir, 'refs/heads', repo_report=repo_report)
    tags = list_refs(git_dir, 'refs/tags', repo_report=repo_report)
    author_output, _stderr = run_command(
        ['git', 'log', '--all', '--format=%an <%ae>'],
        cwd=git_dir,
        logger=repo_report['logger'],
        repo_report=repo_report
    )
    imported_authors = sorted(set([line.strip() for line in author_output.splitlines() if line.strip()]))
    validation = {
        'commits_imported': commit_count,
        'branches_imported': branches,
        'tags_imported': tags,
        'authors_imported': imported_authors,
        'fallback_authors': repo_report['author_mapping']['fallback_authors'],
    }
    add_step(repo_report, 'Validated import: {0} commits, {1} branches, {2} tags'.format(
        commit_count,
        len(branches),
        len(tags),
    ))
    if repo_report['author_mapping']['fallback_authors']:
        add_step(
            repo_report,
            'Fallback author mapping used for: {0}'.format(
                ', '.join(repo_report['author_mapping']['fallback_authors'])
            )
        )
    repo_report['validation'] = validation
    return validation


def add_or_replace_remote(git_dir, remote_url, repo_report):
    """Configure the origin remote."""
    process = subprocess.Popen(
        ['git', 'remote', 'remove', 'origin'],
        cwd=git_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    process.communicate()
    run_command(
        ['git', 'remote', 'add', 'origin', remote_url],
        cwd=git_dir,
        logger=repo_report['logger'],
        repo_report=repo_report
    )
    add_step(repo_report, 'Configured origin remote as {0}'.format(remote_url))


def push_repository(git_dir, force, repo_report):
    """Push all branches and tags to Bitbucket."""
    branch_command = ['git', 'push', '--all', 'origin']
    tag_command = ['git', 'push', '--tags', 'origin']
    if force:
        branch_command.insert(2, '--force')
        tag_command.insert(2, '--force')
    run_command(branch_command, cwd=git_dir, logger=repo_report['logger'], repo_report=repo_report)
    add_step(repo_report, 'Pushed all branches to origin')
    run_command(tag_command, cwd=git_dir, logger=repo_report['logger'], repo_report=repo_report)
    add_step(repo_report, 'Pushed all tags to origin')


def verify_required_tools(config):
    """Ensure required migration tools are available."""
    engine = config.get('migration_tool', {}).get('engine', DEFAULT_ENGINE)
    required = ['git', engine]
    missing = []
    for item in required:
        if not find_executable(item):
            missing.append(item)
    if missing:
        raise MigrationError('Missing required external tools: {0}'.format(', '.join(missing)))


class BitbucketClient(object):
    """Minimal Bitbucket Server/Data Center API wrapper."""

    def __init__(self, config, logger):
        self.base_url = config['base_url'].rstrip('/')
        self.project_key = config['project_key']
        self.project_name = config.get('project_name', self.project_key)
        self.create_project_if_missing = bool(config.get('create_project_if_missing'))
        self.timeout = int(config.get('timeout_seconds', 30))
        self.token = config.get('token')
        self.username = config.get('username')
        self.password = config.get('password')
        self.logger = logger
        self._session = None
        if requests:
            self._session = requests.Session()

    def _build_headers(self):
        headers = {'Content-Type': 'application/json'}
        if self.token:
            headers['Authorization'] = 'Bearer {0}'.format(self.token)
        return headers

    def _request_requests(self, method, path, payload=None):
        url = self.base_url + path
        kwargs = {
            'headers': self._build_headers(),
            'timeout': self.timeout,
        }
        if self.username and self.password and not self.token:
            kwargs['auth'] = (self.username, self.password)
        if payload is not None:
            kwargs['data'] = json.dumps(payload)
        response = self._session.request(method, url, **kwargs)
        body = response.text or ''
        if response.status_code >= 400:
            raise ApiError(
                'Bitbucket API {0} {1} failed with status {2}'.format(method, url, response.status_code),
                status_code=response.status_code,
                body=body
            )
        if not body:
            return {}
        try:
            return response.json()
        except ValueError:
            raise ApiError('Bitbucket API returned invalid JSON for {0} {1}'.format(method, url))

    def _request_urllib2(self, method, path, payload=None):
        url = self.base_url + path
        headers = self._build_headers()
        data = None
        if payload is not None:
            data = json.dumps(payload)
        request = urllib2.Request(url, data=data, headers=headers)
        request.get_method = lambda: method
        if self.username and self.password and not self.token:
            import base64
            token = base64.b64encode('{0}:{1}'.format(self.username, self.password))
            request.add_header('Authorization', 'Basic {0}'.format(token))
        try:
            response = urllib2.urlopen(request, timeout=self.timeout)
            body = response.read()
        except urllib2.HTTPError as exc:
            body = exc.read()
            raise ApiError(
                'Bitbucket API {0} {1} failed with status {2}'.format(method, url, exc.code),
                status_code=exc.code,
                body=body
            )
        if not body:
            return {}
        try:
            return json.loads(body)
        except ValueError:
            raise ApiError('Bitbucket API returned invalid JSON for {0} {1}'.format(method, url))

    def request(self, method, path, payload=None, report_container=None):
        """Send a Bitbucket API request using requests or urllib2."""
        self.logger.debug('Bitbucket API %s %s', method, path)
        if self._session is not None:
            data = self._request_requests(method, path, payload=payload)
        else:
            data = self._request_urllib2(method, path, payload=payload)
        if report_container is not None:
            add_api_call(report_container, method, path, 'success')
        return data

    def project_exists(self, report_container=None):
        """Check whether the Bitbucket project exists."""
        path = '/rest/api/1.0/projects/{0}'.format(quote(self.project_key))
        try:
            self.request('GET', path, report_container=report_container)
            return True
        except ApiError as exc:
            if report_container is not None:
                add_api_call(report_container, 'GET', path, 'status-{0}'.format(exc.status_code))
            if exc.status_code == 404:
                return False
            raise

    def ensure_project(self, report_container=None):
        """Ensure that the Bitbucket project exists."""
        if self.project_exists(report_container=report_container):
            return 'existing'
        if not self.create_project_if_missing:
            raise MigrationError(
                'Bitbucket project {0} does not exist and create_project_if_missing is false'.format(
                    self.project_key
                )
            )
        payload = {'key': self.project_key, 'name': self.project_name}
        self.request('POST', '/rest/api/1.0/projects', payload=payload, report_container=report_container)
        return 'created'

    def repository_exists(self, repo_slug, report_container=None):
        """Check whether a repository exists."""
        path = '/rest/api/1.0/projects/{0}/repos/{1}'.format(
            quote(self.project_key),
            quote(repo_slug)
        )
        try:
            self.request('GET', path, report_container=report_container)
            return True
        except ApiError as exc:
            if report_container is not None:
                add_api_call(report_container, 'GET', path, 'status-{0}'.format(exc.status_code))
            if exc.status_code == 404:
                return False
            raise

    def ensure_repository(self, repo_name, report_container=None):
        """Ensure that the Bitbucket repository exists."""
        if self.repository_exists(repo_name, report_container=report_container):
            return 'existing'
        payload = {'name': repo_name, 'scmId': 'git', 'forkable': True}
        path = '/rest/api/1.0/projects/{0}/repos'.format(quote(self.project_key))
        self.request('POST', path, payload=payload, report_container=report_container)
        return 'created'


class MigrationRunner(object):
    """Drive the end-to-end history migration workflow."""

    def __init__(self, config, options, logger):
        self.config = config
        self.options = options
        self.logger = logger
        self.start_time = datetime.datetime.utcnow()
        self.project_path = normalize_path(config['cvs_project_path'])
        self.workspace_root = normalize_path(config['workspace_root'])
        self.report_root = normalize_path(config['report_root'])
        self.branch_name = config['git']['default_branch']
        self.engine = config.get('migration_tool', {}).get('engine', DEFAULT_ENGINE)
        self.bitbucket = BitbucketClient(config['bitbucket'], logger)
        safe_makedirs(self.workspace_root)
        safe_makedirs(self.report_root)
        self.report = {
            'started_at_utc': self.start_time.isoformat() + 'Z',
            'config_path': options.config,
            'migration_tool': self.engine,
            'dry_run': bool(options.dry_run),
            'force': bool(options.force),
            'skip_existing': bool(options.skip_existing),
            'global_steps': [],
            'api_calls': [],
            'repositories': [],
            'summary': {},
            'limitations': [
                'CVS branch and tag semantics cannot always be represented 1:1 in Git.',
                'cvs-fast-export may emit warnings or discard ambiguous CVS tags/branches when no faithful Git equivalent exists.',
                'Repository quality depends on the correctness of the local CVS metadata and author mappings.',
            ],
        }

    def add_global_step(self, message):
        """Record a global workflow step."""
        self.report['global_steps'].append(message)
        self.logger.info(message)

    def run(self):
        """Execute the migration across all discovered repositories."""
        verify_required_tools(self.config)
        repositories = discover_cvs_repositories(self.project_path)
        self.add_global_step('Detected {0} CVS repositories under {1}'.format(len(repositories), self.project_path))
        selected = self._filter_repositories(repositories)
        self.add_global_step('Selected {0} CVS repositories for processing'.format(len(selected)))
        if not self.options.dry_run:
            project_status = self.bitbucket.ensure_project(report_container=self.report)
            self.add_global_step(
                'Bitbucket project {0} status: {1}'.format(
                    self.config['bitbucket']['project_key'],
                    project_status,
                )
            )
        else:
            self.add_global_step('Dry-run enabled: skipping Bitbucket project creation')
        for repository_path in selected:
            repo_report = self._process_repository(repository_path)
            self.report['repositories'].append(repo_report)
        self._finalize_report()
        return self.report

    def _filter_repositories(self, repositories):
        """Filter repositories based on command line selection."""
        if not self.options.only:
            return repositories
        allowed = set([sanitize_repo_name(item) for item in self.options.only.split(',') if item.strip()])
        selected = []
        for repository_path in repositories:
            repo_name = derive_repository_name(self.project_path, repository_path)
            if repo_name in allowed:
                selected.append(repository_path)
        return selected

    def _new_repo_report(self, repository_path):
        """Create the per-repository report structure."""
        return {
            'repository_name': derive_repository_name(self.project_path, repository_path),
            'repository_path': repository_path,
            'status': 'pending',
            'error': None,
            'steps': [],
            'warnings': [],
            'commands': [],
            'api_calls': [],
            'validation': {},
            'author_mapping': {},
            'logger': self.logger,
        }

    def _process_repository(self, repository_path):
        """Process one CVS repository end-to-end."""
        repo_report = self._new_repo_report(repository_path)
        repo_name = repo_report['repository_name']
        add_step(repo_report, 'Detected CVS repository via CVSROOT at {0}'.format(repository_path))
        temp_root = tempfile.mkdtemp(prefix=repo_name + '-', dir=self.workspace_root)
        git_dir = os.path.join(temp_root, 'git.git')
        authormap_path = os.path.join(temp_root, 'authors.map')
        revmap_path = os.path.join(temp_root, 'revision.map')
        stream_path = os.path.join(temp_root, 'history.fi')
        file_list_path = os.path.join(temp_root, 'rcs-files.txt')
        repo_report['workspace'] = temp_root
        try:
            write_file_list(repository_path, file_list_path)
            handle = open(file_list_path, 'rb')
            try:
                file_count = len([line for line in handle.read().splitlines() if line.strip()])
            finally:
                handle.close()
            add_step(repo_report, 'Prepared RCS file list with {0} master files'.format(file_count))
            authormap = build_authormap(repository_path, self.config, repo_report, authormap_path)
            add_step(repo_report, 'Author map written to {0}'.format(authormap_path))
            if self.options.dry_run:
                add_step(repo_report, 'Dry-run would run {0} with revision map output at {1}'.format(self.engine, revmap_path))
                add_step(repo_report, 'Dry-run would create Bitbucket repository and push all branches and tags')
                repo_report['status'] = 'dry-run'
                return repo_report
            repo_exists = self.bitbucket.repository_exists(repo_name, report_container=repo_report)
            if repo_exists and self.options.skip_existing:
                repo_report['status'] = 'skipped'
                add_step(repo_report, 'Skipped because Bitbucket repository already exists and skip_existing is enabled')
                return repo_report
            repository_status = self.bitbucket.ensure_repository(repo_name, report_container=repo_report)
            add_step(repo_report, 'Bitbucket repository status: {0}'.format(repository_status))
            initialize_bare_git_repository(git_dir, repo_report)
            if self.engine != 'cvs-fast-export':
                raise MigrationError('Configured migration tool "{0}" is not implemented'.format(self.engine))
            run_cvs_fast_export(repository_path, file_list_path, authormap_path, revmap_path, stream_path, repo_report)
            import_fast_stream(git_dir, stream_path, repo_report)
            apply_ref_mapping(git_dir, self.config, repo_report)
            active_branch = set_default_branch(git_dir, self.branch_name, repo_report)
            validation = validate_import(git_dir, repo_report)
            validation['active_head_branch'] = active_branch
            validation['authormap_entries'] = len(authormap)
            validation['bitbucket_repository_status'] = repository_status
            ssh_template = self.config['git']['ssh_url_template']
            remote_url = ssh_template.format(
                project_key=self.config['bitbucket']['project_key'],
                repo_slug=repo_name,
                repo_name=repo_name,
            )
            add_or_replace_remote(git_dir, remote_url, repo_report)
            push_repository(git_dir, self.options.force, repo_report)
            repo_report['validation']['branches_pushed'] = True
            repo_report['validation']['tags_pushed'] = True
            repo_report['status'] = 'success'
            add_step(repo_report, 'Repository migration completed successfully')
        except Exception as exc:
            repo_report['status'] = 'failed'
            repo_report['error'] = str(exc)
            add_step(repo_report, 'Repository migration failed: {0}'.format(exc), level='ERROR')
            self.logger.debug(traceback.format_exc())
        finally:
            if self.options.keep_workdirs:
                add_step(repo_report, 'Keeping temporary workspace at {0}'.format(temp_root))
            else:
                shutil.rmtree(temp_root, ignore_errors=True)
                add_step(repo_report, 'Removed temporary workspace {0}'.format(temp_root))
        return repo_report

    def _finalize_report(self):
        """Write JSON and Markdown reports to disk."""
        repositories = self.report['repositories']
        success_count = len([item for item in repositories if item['status'] == 'success'])
        failure_count = len([item for item in repositories if item['status'] == 'failed'])
        skipped_count = len([item for item in repositories if item['status'] == 'skipped'])
        dry_run_count = len([item for item in repositories if item['status'] == 'dry-run'])
        finished_at = datetime.datetime.utcnow()
        self.report['finished_at_utc'] = finished_at.isoformat() + 'Z'
        self.report['summary'] = {
            'total': len(repositories),
            'success': success_count,
            'failed': failure_count,
            'skipped': skipped_count,
            'dry_run': dry_run_count,
        }
        json_report_path = os.path.join(self.report_root, 'migration-report.json')
        markdown_report_path = os.path.join(self.report_root, 'migration-report.md')
        serializable_repositories = []
        for repo_report in repositories:
            clone = dict(repo_report)
            if 'logger' in clone:
                del clone['logger']
            serializable_repositories.append(clone)
        payload = dict(self.report)
        payload['repositories'] = serializable_repositories
        dump_json(json_report_path, payload)
        write_text(markdown_report_path, self._build_markdown_report(payload))
        self.logger.info('Wrote JSON report to %s', json_report_path)
        self.logger.info('Wrote Markdown report to %s', markdown_report_path)

    def _build_markdown_report(self, payload):
        """Render a Markdown migration report."""
        lines = []
        lines.append('# CVS to Git Full-History Migration Report')
        lines.append('')
        lines.append('## Summary')
        lines.append('')
        lines.append('* Started: {0}'.format(payload['started_at_utc']))
        lines.append('* Finished: {0}'.format(payload['finished_at_utc']))
        lines.append('* Migration tool: `{0}`'.format(payload['migration_tool']))
        lines.append('* Dry-run: `{0}`'.format(payload['dry_run']))
        lines.append('* Force: `{0}`'.format(payload['force']))
        lines.append('* Skip existing: `{0}`'.format(payload['skip_existing']))
        lines.append('* Total repositories: `{0}`'.format(payload['summary']['total']))
        lines.append('* Successful: `{0}`'.format(payload['summary']['success']))
        lines.append('* Failed: `{0}`'.format(payload['summary']['failed']))
        lines.append('* Skipped: `{0}`'.format(payload['summary']['skipped']))
        lines.append('* Dry-run only: `{0}`'.format(payload['summary']['dry_run']))
        lines.append('')
        lines.append('## Global Steps')
        lines.append('')
        for step in payload['global_steps']:
            lines.append('1. {0}'.format(step))
        lines.append('')
        lines.append('## Technical Limits')
        lines.append('')
        for limitation in payload['limitations']:
            lines.append('* {0}'.format(limitation))
        lines.append('')
        for repo_report in payload['repositories']:
            lines.append('## {0}'.format(repo_report['repository_name']))
            lines.append('')
            lines.append('* Source path: `{0}`'.format(repo_report['repository_path']))
            lines.append('* Status: `{0}`'.format(repo_report['status']))
            if repo_report.get('error'):
                lines.append('* Error: `{0}`'.format(repo_report['error']))
            if repo_report.get('validation'):
                validation = repo_report['validation']
                lines.append('* Commits imported: `{0}`'.format(validation.get('commits_imported', 0)))
                lines.append('* Branches imported: `{0}`'.format(', '.join(validation.get('branches_imported', []))))
                lines.append('* Tags imported: `{0}`'.format(', '.join(validation.get('tags_imported', []))))
                lines.append('* Authors imported: `{0}`'.format(', '.join(validation.get('authors_imported', []))))
                lines.append('* Fallback authors: `{0}`'.format(
                    ', '.join(validation.get('fallback_authors', []))
                ))
            lines.append('')
            lines.append('### Steps')
            lines.append('')
            for step in repo_report['steps']:
                lines.append('1. {0}'.format(step))
            lines.append('')
            lines.append('### API Calls')
            lines.append('')
            for call in repo_report['api_calls']:
                lines.append('* `{0} {1}` -> `{2}`'.format(call['method'], call['path'], call['status']))
            lines.append('')
            lines.append('### Commands')
            lines.append('')
            for command in repo_report['commands']:
                lines.append('* `{0}` (cwd: `{1}`)'.format(' '.join(command['command']), command['cwd']))
            lines.append('')
        return '\n'.join(lines) + '\n'


def validate_config(config):
    """Validate the mandatory configuration structure."""
    required_top_level = [
        'cvs_project_path',
        'workspace_root',
        'report_root',
        'migration_tool',
        'bitbucket',
        'git',
        'default_committer',
    ]
    for key in required_top_level:
        if key not in config:
            raise MigrationError('Missing required config key: {0}'.format(key))
    for key in ['engine']:
        if key not in config['migration_tool']:
            raise MigrationError('Missing required migration_tool config key: {0}'.format(key))
    for key in ['base_url', 'project_key']:
        if key not in config['bitbucket']:
            raise MigrationError('Missing required bitbucket config key: {0}'.format(key))
    for key in ['default_branch', 'ssh_url_template']:
        if key not in config['git']:
            raise MigrationError('Missing required git config key: {0}'.format(key))
    for key in ['name', 'email']:
        if key not in config['default_committer']:
            raise MigrationError('Missing required default_committer key: {0}'.format(key))


def configure_logging(report_root, verbose):
    """Configure console and file logging."""
    safe_makedirs(report_root)
    logger = logging.getLogger('cvstogitmigration')
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    formatter = logging.Formatter(LOG_FORMAT)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    file_handler = logging.FileHandler(os.path.join(report_root, 'migration.log'))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def build_argument_parser():
    """Create the command line parser."""
    parser = argparse.ArgumentParser(
        description='Migrate local CVS repositories with full history to Git and Bitbucket.'
    )
    parser.add_argument('--config', required=True, help='Path to the JSON configuration file')
    parser.add_argument('--dry-run', action='store_true', help='Plan the migration without modifying Git or Bitbucket')
    parser.add_argument('--force', action='store_true', help='Force-push branches and tags to Bitbucket')
    parser.add_argument('--skip-existing', action='store_true', help='Skip repositories that already exist in Bitbucket')
    parser.add_argument('--keep-workdirs', action='store_true', help='Keep temporary work directories after processing')
    parser.add_argument('--only', help='Comma-separated list of repository names to process')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose console logging')
    return parser


def main(argv=None):
    """CLI entry point."""
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    config = load_json(args.config)
    validate_config(config)
    report_root = normalize_path(config['report_root'])
    logger = configure_logging(report_root, args.verbose)
    logger.info('Loading configuration from %s', normalize_path(args.config))
    runner = MigrationRunner(config, args, logger)
    report = runner.run()
    if report['summary']['failed']:
        return 1
    return 0
