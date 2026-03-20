import os
import shutil
import tempfile
import unittest

from cvstogitmigration import migrator


class MigratorTestCase(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='cvstogitmigration-tests-')

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_discover_cvs_repositories_finds_root_and_nested_repositories(self):
        root_repo = os.path.join(self.tempdir, 'root')
        nested_parent = os.path.join(root_repo, 'nested')
        nested_repo = os.path.join(nested_parent, 'repo2')
        os.makedirs(os.path.join(root_repo, 'CVSROOT'))
        os.makedirs(os.path.join(nested_repo, 'CVSROOT'))
        result = migrator.discover_cvs_repositories(self.tempdir)
        self.assertEqual([root_repo, nested_repo], result)

    def test_derive_repository_name_uses_relative_path(self):
        project_path = os.path.join(self.tempdir, 'project')
        repository_path = os.path.join(project_path, 'Team Repo', 'Backend')
        os.makedirs(repository_path)
        result = migrator.derive_repository_name(project_path, repository_path)
        self.assertEqual('team-repo-backend', result)

    def test_iter_snapshot_files_skips_cvsroot_and_attic(self):
        repository_path = os.path.join(self.tempdir, 'repo')
        os.makedirs(os.path.join(repository_path, 'CVSROOT'))
        os.makedirs(os.path.join(repository_path, 'src'))
        os.makedirs(os.path.join(repository_path, 'src', 'Attic'))
        open(os.path.join(repository_path, 'src', 'live.txt,v'), 'wb').close()
        open(os.path.join(repository_path, 'src', 'Attic', 'deleted.txt,v'), 'wb').close()
        open(os.path.join(repository_path, 'CVSROOT', 'modules,v'), 'wb').close()
        result = list(migrator.iter_snapshot_files(repository_path))
        self.assertEqual(
            [(os.path.join(repository_path, 'src', 'live.txt,v'), os.path.join('src', 'live.txt'))],
            result
        )


if __name__ == '__main__':
    unittest.main()
