#setup.py

from setuptools import setup
setup(
    name='census-parquet',
    version='0.0.1',
    entry_points={
        'console_scripts': [
            'mysupercoolscript=mysupercoolscript:mysupercoolscript'
        ]
    }
)
