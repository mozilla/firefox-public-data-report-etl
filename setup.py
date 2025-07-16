#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='firefox-public-data-report-etl',
    version='0.1',
    description='Python ETL jobs for the Firefox Public Data Report',
    author='Firefox Data Platform',
    author_email='fx-public-data@mozilla.com',
    url='https://github.com/mozilla/firefox-public-data-report-etl.git',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=[
        "click == 8.2.1",
        "google-cloud-bigquery == 3.34.0",
        "google-cloud-storage == 3.2.0",
        "requests == 2.32.4",
    ],
)
