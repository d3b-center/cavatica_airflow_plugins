#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


def parse_requirements():
    """ load requirements from a pip requirements file """
    lineiter = (line.strip() for line in open('requirements.txt'))
    return [line for line in lineiter if line and not line.startswith("#")]


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()


setup_requirements = []

test_requirements = []

setup(
    author="Mark Welsh, D3b Center, CHOP",
    author_email='welshm3@chop.edu',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
    ],
    description="Plugins for Airflow that wrap commonly used Cavatica API endpoints",
    install_requires=parse_requirements(),
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='cavatica_airflow_plugins',
    name='cavatica_airflow_plugins',
    packages=find_packages(include=['cavatica_airflow_plugins']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/d3b-center/cavatica_airflow_plugins',
    version='0.1.1',
    zip_safe=False,
)
