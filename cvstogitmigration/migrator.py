"""Snapshot-based CVS to Git to Bitbucket migration for Python 2.7."""

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


LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'
DEFAULT_COMMIT_MESSAGE = 'Initial snapshot import from CVS repository'


class MigrationError(Exception):
    """Raised for controlled migration failures."""


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
    """Return a relative path in Python 2.7-compatible form."""
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


def iter_snapshot_files(repository_path):
    """Yield live RCS files that belong to the current repository snapshot."""
    for root, dirs, files in os.walk(repository_path):
        dirs.sort()
        if root == repository_path:
            dirs[:] = [item for item in dirs if item != 'CVSROOT']
        if 'Attic' in dirs:
            dirs[:] = [item for item in dirs if item != 'Attic']
        relative_root = relpath(root, repository_path)
        if relative_root == 'CVSROOT':
            continue
        if '/Attic' in relative_root or relative_root.startswith('Attic'):
            continue
        files = sorted(files)
        for filename in files:
            if not filename.endswith(',v'):
                continue
            source_path = os.path.join(root, filename)
            relative_path = relpath(source_path, repository_path)
            relative_path = relative_path[:-2]
            yield source_path, relative_path


def parse_head_author(rcs_path):
    """Parse the head author from an RCS file."""
    handle = open(rcs_path, 'rb')
    try:
        content = handle.read()
    finally:
        handle.close()
    head_match = re.search(r'head\s+([^;]+);', content)
    if not head_match:
        return None
    revision = head_match.group(1).strip()
    pattern = r'(^|\n)%s\s*\n(?:[^\n]*\n)*?date\s+[^;]+;\s+author\s+([^;]+);\s+state\s+([^;]+);'
    match = re.search(pattern % re.escape(revision), content, re.MULTILINE)
    if not match:
        return None
    return {
        'revision': revision,
        'author': match.group(2).strip(),
        'state': match.group(3).strip(),
    }


def ensure_clean_directory(path):
    """Recreate a clean directory tree."""
    if os.path.isdir(path):
        shutil.rmtree(path)
    safe_makedirs(path)


def run_command(command, cwd=None, env=None, logger=None):
    """Run a subprocess and raise on failure."""
    if logger:
        logger.debug('Running command: %s', ' '.join(command))
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
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


def export_rcs_file(rcs_path, target_path, logger):
    """Export the head revision of an RCS file to the target path."""
    parent = os.path.dirname(target_path)
    safe_makedirs(parent)
    process = subprocess.Popen(
        ['co', '-q', '-p', rcs_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout_data, stderr_data = process.communicate()
    if process.returncode != 0:
        raise MigrationError(
            'Failed to export RCS file {0}: {1}'.format(
                rcs_path,
                stderr_data.strip()
            )
        )
    handle = open(target_path, 'wb')
    try:
        handle.write(stdout_data)
    finally:
        handle.close()
    source_mode = os.stat(rcs_path).st_mode
    if source_mode & 0111:
        os.chmod(target_path, 0755)
    logger.debug('Exported %s to %s', rcs_path, target_path)


def resolve_commit_identity(repository_path, snapshot_files, config, repo_report):
    """Resolve the single Git identity for the snapshot commit."""
    default_committer = config['default_committer']
    author_map = config.get('author_map', {})
    unique_authors = set()
    fallback_reason = 'No CVS author information could be resolved'
    for source_path, _relative_path in snapshot_files:
        parsed = parse_head_author(source_path)
        if not parsed:
            continue
        if parsed.get('state') == 'dead':
            continue
        unique_authors.add(parsed.get('author'))
        if len(unique_authors) > 1:
            fallback_reason = 'Multiple CVS authors found in repository snapshot'
            break
    if len(unique_authors) == 1:
        cvs_author = list(unique_authors)[0]
        mapped = author_map.get(cvs_author)
        if mapped and mapped.get('name') and mapped.get('email'):
            add_step(
                repo_report,
                'Resolved single CVS author "{0}" to Git identity {1} <{2}>'.format(
                    cvs_author,
                    mapped['name'],
                    mapped['email'],
                )
            )
            return {
                'name': mapped['name'],
                'email': mapped['email'],
                'source': 'author_map',
                'cvs_author': cvs_author,
            }
        fallback_reason = 'Single CVS author "{0}" has no configured author_map entry'.format(cvs_author)
    elif len(unique_authors) == 0:
        fallback_reason = 'No live CVS author found in snapshot files'
    add_step(
        repo_report,
        'Using fallback Git committer {0} <{1}> because: {2}'.format(
            default_committer['name'],
            default_committer['email'],
            fallback_reason,
        )
    )
    return {
        'name': default_committer['name'],
        'email': default_committer['email'],
        'source': 'fallback',
        'cvs_author': None,
    }


def add_step(repo_report, message, level='INFO'):
    """Append a step to the repository report and log it."""
    repo_report['steps'].append(message)
    logger = repo_report.get('logger')
    if logger:
        log_method = getattr(logger, level.lower(), logger.info)
        log_method('[%s] %s', repo_report['repository_name'], message)


def build_snapshot(repository_path, snapshot_path, repo_report):
    """Create a filesystem snapshot from a CVS repository."""
    add_step(repo_report, 'Preparing snapshot workspace at {0}'.format(snapshot_path))
    ensure_clean_directory(snapshot_path)
    snapshot_files = list(iter_snapshot_files(repository_path))
    add_step(
        repo_report,
        'Discovered {0} live RCS files after excluding CVSROOT and Attic'.format(len(snapshot_files))
    )
    for source_path, relative_path in snapshot_files:
        export_rcs_file(source_path, os.path.join(snapshot_path, relative_path), repo_report['logger'])
    add_step(repo_report, 'Snapshot export completed')
    return snapshot_files


def init_git_repository(snapshot_path, branch_name, repo_report):
    """Initialize a Git repository for the exported snapshot."""
    add_step(repo_report, 'Initializing Git repository in {0}'.format(snapshot_path))
    run_command(['git', 'init'], cwd=snapshot_path, logger=repo_report['logger'])
    if branch_name != 'master':
        run_command(['git', 'checkout', '--orphan', branch_name], cwd=snapshot_path, logger=repo_report['logger'])
        git_dir = os.path.join(snapshot_path, '.git')
        refs_path = os.path.join(git_dir, 'refs', 'heads', 'master')
        if os.path.exists(refs_path):
            os.unlink(refs_path)
    add_step(repo_report, 'Git repository initialized with target branch {0}'.format(branch_name))


def create_git_commit(snapshot_path, branch_name, identity, repo_name, repo_report, config):
    """Create the single Git snapshot commit."""
    commit_message = config.get('git', {}).get(
        'commit_message',
        '{0} "{1}"'.format(DEFAULT_COMMIT_MESSAGE, repo_name)
    )
    env = os.environ.copy()
    env['GIT_AUTHOR_NAME'] = identity['name']
    env['GIT_AUTHOR_EMAIL'] = identity['email']
    env['GIT_COMMITTER_NAME'] = identity['name']
    env['GIT_COMMITTER_EMAIL'] = identity['email']
    run_command(['git', 'add', '-A'], cwd=snapshot_path, env=env, logger=repo_report['logger'])
    run_command(['git', 'status', '--short'], cwd=snapshot_path, env=env, logger=repo_report['logger'])
    run_command(
        ['git', 'commit', '--allow-empty', '-m', commit_message],
        cwd=snapshot_path,
        env=env,
        logger=repo_report['logger']
    )
    if branch_name == 'master':
        add_step(repo_report, 'Created single Git commit on master')
    else:
        run_command(['git', 'branch', '-M', branch_name], cwd=snapshot_path, env=env, logger=repo_report['logger'])
        add_step(repo_report, 'Created single Git commit and renamed branch to {0}'.format(branch_name))


def add_or_replace_remote(snapshot_path, remote_url, repo_report):
    """Add or replace the Git origin remote."""
    try:
        run_command(['git', 'remote', 'remove', 'origin'], cwd=snapshot_path, logger=repo_report['logger'])
    except Exception:
        pass
    run_command(['git', 'remote', 'add', 'origin', remote_url], cwd=snapshot_path, logger=repo_report['logger'])
    add_step(repo_report, 'Configured Git remote origin as {0}'.format(remote_url))


def push_repository(snapshot_path, branch_name, force, repo_report):
    """Push the Git repository to Bitbucket over SSH."""
    command = ['git', 'push', '-u', 'origin', branch_name]
    if force:
        command.insert(2, '--force')
    run_command(command, cwd=snapshot_path, logger=repo_report['logger'])
    add_step(repo_report, 'Pushed branch {0} to origin'.format(branch_name))


class BitbucketClient(object):
    """Minimal Bitbucket Server/Data Center API wrapper."""

    def __init__(self, config, logger):
        self.base_url = config['base_url'].rstrip('/')
        self.project_key = config['project_key']
        self.project_name = config.get('project_name', self.project_key)
        self.create_project_if_missing = bool(config.get('create_project_if_missing'))
        self.token = config.get('token')
        self.username = config.get('username')
        self.password = config.get('password')
        self.timeout = int(config.get('timeout_seconds', 30))
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
            raise MigrationError(
                'Bitbucket API {0} {1} failed with status {2}: {3}'.format(
                    method,
                    url,
                    response.status_code,
                    body[:1000],
                )
            )
        if not body:
            return {}
        try:
            return response.json()
        except ValueError:
            raise MigrationError('Bitbucket API returned invalid JSON for {0} {1}'.format(method, url))

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
            raise MigrationError(
                'Bitbucket API {0} {1} failed with status {2}: {3}'.format(
                    method,
                    url,
                    exc.code,
                    body[:1000],
                )
            )
        if not body:
            return {}
        try:
            return json.loads(body)
        except ValueError:
            raise MigrationError('Bitbucket API returned invalid JSON for {0} {1}'.format(method, url))

    def request(self, method, path, payload=None):
        """Send a Bitbucket API request using requests or urllib2."""
        self.logger.debug('Bitbucket API %s %s', method, path)
        if self._session is not None:
            return self._request_requests(method, path, payload=payload)
        return self._request_urllib2(method, path, payload=payload)

    def project_exists(self):
        """Check whether the Bitbucket project already exists."""
        path = '/rest/api/1.0/projects/{0}'.format(quote(self.project_key))
        try:
            self.request('GET', path)
            return True
        except MigrationError as exc:
            if 'status 404' in str(exc):
                return False
            raise

    def ensure_project(self):
        """Ensure that the Bitbucket project exists."""
        if self.project_exists():
            return 'existing'
        if not self.create_project_if_missing:
            raise MigrationError(
                'Bitbucket project {0} does not exist and create_project_if_missing is false'.format(
                    self.project_key
                )
            )
        payload = {'key': self.project_key, 'name': self.project_name}
        self.request('POST', '/rest/api/1.0/projects', payload=payload)
        return 'created'

    def repository_exists(self, repo_slug):
        """Check whether a repository exists."""
        path = '/rest/api/1.0/projects/{0}/repos/{1}'.format(
            quote(self.project_key),
            quote(repo_slug)
        )
        try:
            self.request('GET', path)
            return True
        except MigrationError as exc:
            if 'status 404' in str(exc):
                return False
            raise

    def ensure_repository(self, repo_name):
        """Ensure that the Bitbucket repository exists."""
        if self.repository_exists(repo_name):
            return 'existing'
        payload = {'name': repo_name, 'scmId': 'git', 'forkable': True}
        path = '/rest/api/1.0/projects/{0}/repos'.format(quote(self.project_key))
        self.request('POST', path, payload=payload)
        return 'created'


class MigrationRunner(object):
    """Drive the end-to-end migration workflow."""

    def __init__(self, config, options, logger):
        self.config = config
        self.options = options
        self.logger = logger
        self.start_time = datetime.datetime.utcnow()
        self.report = {
            'started_at_utc': self.start_time.isoformat() + 'Z',
            'config_path': options.config,
            'dry_run': bool(options.dry_run),
            'force': bool(options.force),
            'skip_existing': bool(options.skip_existing),
            'repositories': [],
            'summary': {},
        }
        self.project_path = normalize_path(config['cvs_project_path'])
        self.workspace_root = normalize_path(config['workspace_root'])
        self.report_root = normalize_path(config['report_root'])
        self.branch_name = config.get('git', {}).get('default_branch', 'main')
        self.bitbucket = BitbucketClient(config['bitbucket'], logger)
        safe_makedirs(self.workspace_root)
        safe_makedirs(self.report_root)

    def run(self):
        """Execute the migration across all discovered repositories."""
        repositories = discover_cvs_repositories(self.project_path)
        self.logger.info('Discovered %d CVS repositories under %s', len(repositories), self.project_path)
        selected = self._filter_repositories(repositories)
        self.logger.info('Selected %d CVS repositories for processing', len(selected))
        if not self.options.dry_run:
            project_status = self.bitbucket.ensure_project()
            self.logger.info('Bitbucket project %s status: %s', self.bitbucket.project_key, project_status)
        else:
            self.logger.info('Dry-run enabled: skipping Bitbucket project creation check')
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
        """Create a repository report structure."""
        repo_name = derive_repository_name(self.project_path, repository_path)
        return {
            'repository_path': repository_path,
            'repository_name': repo_name,
            'status': 'pending',
            'error': None,
            'steps': [],
            'logger': self.logger,
        }

    def _process_repository(self, repository_path):
        """Process one CVS repository from discovery to push."""
        repo_report = self._new_repo_report(repository_path)
        repo_name = repo_report['repository_name']
        add_step(repo_report, 'Detected CVS repository via CVSROOT at {0}'.format(repository_path))
        temp_root = tempfile.mkdtemp(prefix=repo_name + '-', dir=self.workspace_root)
        snapshot_path = os.path.join(temp_root, 'snapshot')
        repo_report['workspace'] = temp_root
        try:
            repo_exists = False
            if not self.options.dry_run:
                repo_exists = self.bitbucket.repository_exists(repo_name)
                if repo_exists and self.options.skip_existing:
                    repo_report['status'] = 'skipped'
                    add_step(repo_report, 'Skipped because Bitbucket repository already exists and skip_existing is enabled')
                    return repo_report
                ensure_status = self.bitbucket.ensure_repository(repo_name)
                add_step(repo_report, 'Bitbucket repository status: {0}'.format(ensure_status))
            else:
                add_step(repo_report, 'Dry-run enabled: skipping Bitbucket repository API calls')
            if self.options.dry_run:
                snapshot_files = list(iter_snapshot_files(repository_path))
                add_step(
                    repo_report,
                    'Dry-run detected {0} live RCS files that would be exported'.format(len(snapshot_files))
                )
                identity = resolve_commit_identity(repository_path, snapshot_files, self.config, repo_report)
                add_step(
                    repo_report,
                    'Dry-run would create exactly one Git commit as {0} <{1}>'.format(
                        identity['name'],
                        identity['email'],
                    )
                )
                repo_report['status'] = 'dry-run'
                return repo_report
            snapshot_files = build_snapshot(repository_path, snapshot_path, repo_report)
            identity = resolve_commit_identity(repository_path, snapshot_files, self.config, repo_report)
            init_git_repository(snapshot_path, self.branch_name, repo_report)
            create_git_commit(snapshot_path, self.branch_name, identity, repo_name, repo_report, self.config)
            ssh_template = self.config['git']['ssh_url_template']
            remote_url = ssh_template.format(
                project_key=self.config['bitbucket']['project_key'],
                repo_slug=repo_name,
                repo_name=repo_name,
            )
            add_or_replace_remote(snapshot_path, remote_url, repo_report)
            push_repository(snapshot_path, self.branch_name, self.options.force, repo_report)
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
        serializable = []
        for repo_report in repositories:
            clone = dict(repo_report)
            if 'logger' in clone:
                del clone['logger']
            serializable.append(clone)
        report_payload = dict(self.report)
        report_payload['repositories'] = serializable
        dump_json(json_report_path, report_payload)
        write_text(markdown_report_path, self._build_markdown_report(report_payload))
        self.logger.info('Wrote JSON report to %s', json_report_path)
        self.logger.info('Wrote Markdown report to %s', markdown_report_path)

    def _build_markdown_report(self, payload):
        """Render a Markdown migration report."""
        lines = []
        lines.append('# CVS to Git Snapshot Migration Report')
        lines.append('')
        lines.append('## Summary')
        lines.append('')
        lines.append('* Started: {0}'.format(payload['started_at_utc']))
        lines.append('* Finished: {0}'.format(payload['finished_at_utc']))
        lines.append('* Dry-run: {0}'.format(payload['dry_run']))
        lines.append('* Force push: {0}'.format(payload['force']))
        lines.append('* Skip existing: {0}'.format(payload['skip_existing']))
        lines.append('* Total repositories: {0}'.format(payload['summary']['total']))
        lines.append('* Successful: {0}'.format(payload['summary']['success']))
        lines.append('* Failed: {0}'.format(payload['summary']['failed']))
        lines.append('* Skipped: {0}'.format(payload['summary']['skipped']))
        lines.append('* Dry-run only: {0}'.format(payload['summary']['dry_run']))
        lines.append('')
        for repo_report in payload['repositories']:
            lines.append('## {0}'.format(repo_report['repository_name']))
            lines.append('')
            lines.append('* Source path: `{0}`'.format(repo_report['repository_path']))
            lines.append('* Status: `{0}`'.format(repo_report['status']))
            if repo_report.get('error'):
                lines.append('* Error: `{0}`'.format(repo_report['error']))
            lines.append('')
            lines.append('### Steps')
            lines.append('')
            for step in repo_report['steps']:
                lines.append('1. {0}'.format(step))
            lines.append('')
        return '\n'.join(lines) + '\n'


def validate_config(config):
    """Validate the mandatory configuration structure."""
    required_top_level = [
        'cvs_project_path',
        'workspace_root',
        'report_root',
        'bitbucket',
        'git',
        'default_committer',
    ]
    for key in required_top_level:
        if key not in config:
            raise MigrationError('Missing required config key: {0}'.format(key))
    required_bitbucket = ['base_url', 'project_key']
    for key in required_bitbucket:
        if key not in config['bitbucket']:
            raise MigrationError('Missing required bitbucket config key: {0}'.format(key))
    required_git = ['ssh_url_template', 'default_branch']
    for key in required_git:
        if key not in config['git']:
            raise MigrationError('Missing required git config key: {0}'.format(key))
    required_committer = ['name', 'email']
    for key in required_committer:
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
        description='Migrate local CVS repositories to Git and Bitbucket as single-commit snapshots.'
    )
    parser.add_argument('--config', required=True, help='Path to the JSON configuration file')
    parser.add_argument('--dry-run', action='store_true', help='Plan the migration without changing Git or Bitbucket')
    parser.add_argument('--force', action='store_true', help='Force-push to Bitbucket if the destination already has commits')
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
