# setup.py

from setuptools import setup, find_packages

setup(
    name='mcutimeline', 
    version='1.0', 
    packages=find_packages(),
    install_requires = [
        'wikitextparser',
        'requests'
    ]
)