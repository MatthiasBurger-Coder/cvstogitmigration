import unittest

from cvstogitmigration import cli


class CliTestCase(unittest.TestCase):

    def test_main_returns_success(self):
        self.assertEqual(cli.main(), 0)


if __name__ == '__main__':
    unittest.main()
