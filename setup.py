# setup.py

from setuptools import setup, find_packages

setup(
    name='mcutimeline', 
    version='1.0', 
    packages=find_packages(),
    install_requires = [
        'pip-autoremove',
        'wikitextparser',
        'requests',
        'bs4',
        'django',
        'django_extensions',
        'django-webpack-loader',
        'djangorestframework',
        'pygments',
        'pylint',
        'psycopg2-binary',
    ]
)