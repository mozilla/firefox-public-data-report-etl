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
        "boto3 == 1.12.33",
        "click == 7.1.1",
        "google-cloud-bigquery == 1.24.0",
        "google-cloud-storage==2.7.0",
        "pyspark >= 2.4.0",
        "requests == 2.23.0",
    ],
)
