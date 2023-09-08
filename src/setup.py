from setuptools import setup, find_packages

setup(
    name='two_one',
    version='0.0.1',
    packages=find_packages(where='src'),
    include_package_data=True,
    install_requires=[
        'arrow==1.2.3',
        'ray==2.6.2',
        'neomodel==5.1.0',
        'pandas==2.0.3',
        'python-on-whales~=0.64.2',
        'click==8.1.7',
        'tqdm',
    ],
    entry_points={
        'console_scripts': [
            'two_one = two_one.cli:cli',
        ],
    },
)
