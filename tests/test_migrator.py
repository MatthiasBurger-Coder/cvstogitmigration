import os
import shutil
import tempfile
import unittest

from cvstogitmigration import migrator


RCS_SAMPLE = """head 1.3;
access;
symbols;
locks; strict;
comment @# @;

1.3
date 2024.01.02.10.11.12;  author jdoe;  state Exp;
branches;
next 1.2;

1.2
date 2024.01.01.09.00.00;  author unknown;  state Exp;
branches;
next 1.1;
"""


class MigratorTestCase(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='cvstogitmigration-tests-')

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_discover_cvs_repositories_finds_root_and_nested_repositories(self):
        root_repo = os.path.join(self.tempdir, 'root')
        nested_repo = os.path.join(root_repo, 'nested', 'repo2')
        os.makedirs(os.path.join(root_repo, 'CVSROOT'))
        os.makedirs(os.path.join(nested_repo, 'CVSROOT'))
        result = migrator.discover_cvs_repositories(self.tempdir)
        self.assertEqual([root_repo, nested_repo], result)

    def test_collect_cvs_authors_reads_all_authors_from_rcs_files(self):
        repository_path = os.path.join(self.tempdir, 'repo')
        os.makedirs(os.path.join(repository_path, 'CVSROOT'))
        os.makedirs(os.path.join(repository_path, 'src'))
        handle = open(os.path.join(repository_path, 'src', 'main.py,v'), 'wb')
        try:
            handle.write(RCS_SAMPLE)
        finally:
            handle.close()
        result = migrator.collect_cvs_authors(repository_path)
        self.assertEqual(['jdoe', 'unknown'], result)

    def test_build_authormap_uses_fallback_for_unknown_authors(self):
        repository_path = os.path.join(self.tempdir, 'repo')
        os.makedirs(os.path.join(repository_path, 'CVSROOT'))
        os.makedirs(os.path.join(repository_path, 'src'))
        handle = open(os.path.join(repository_path, 'src', 'main.py,v'), 'wb')
        try:
            handle.write(RCS_SAMPLE)
        finally:
            handle.close()
        report = {
            'repository_name': 'repo',
            'steps': [],
            'warnings': [],
            'commands': [],
            'api_calls': [],
            'logger': None,
        }
        authormap_path = os.path.join(self.tempdir, 'authors.map')
        config = {
            'default_committer': {
                'name': 'John Doe',
                'email': 'john.doe@example.com',
            },
            'author_map': {
                'jdoe': {
                    'name': 'Jane Doe',
                    'email': 'jane.doe@example.com',
                }
            }
        }
        result = migrator.build_authormap(repository_path, config, report, authormap_path)
        self.assertEqual(2, len(result))
        self.assertEqual(['unknown'], report['author_mapping']['fallback_authors'])
        content = open(authormap_path, 'rb').read()
        self.assertTrue('jdoe = Jane Doe <jane.doe@example.com>' in content)
        self.assertTrue('unknown = John Doe <john.doe@example.com>' in content)


if __name__ == '__main__':
    unittest.main()
