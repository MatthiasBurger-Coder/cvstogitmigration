from setuptools import find_packages
from setuptools import setup


setup(
    name='cvstogitmigration',
    version='0.1.0',
    description='Helper to migrate CVS repositories to Git',
    author='',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=[],
    entry_points={
        'console_scripts': [
            'cvstogitmigration=cvstogitmigration.cli:main',
        ],
    },
)
