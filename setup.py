#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

version = "0.0.1"

setup(
    name="psql-replication",
    version=version,
    url="https://github.com/jiamo/python-psql-replication",
    author="jiamo",
    author_email="life.130815@gmail.com",
    description=("Pure Python Implementation of PSQL Replication Only data change not DDL"
                 "build on top of psycopg2."),
    license="Apache 2",
    zip_safe=False,
    include_package_data=True,
    packages=find_packages(include=['ppsqlreplication']),
    install_requires=['psycopg2'],
)
