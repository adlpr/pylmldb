#!/usr/bin/python3
# -*- coding: UTF-8 -*-

version = '0.1'

import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.md'),'r') as inf:
    long_description = inf.read()

setup(
    name = 'pylmldb',
    version = version,
    # url = '',
    author = 'Alex DelPriore',
    author_email = 'delpriore@stanford.edu',
    license = 'Copyright © 2019 The Board of Trustees of The Leland Stanford Junior University, All Rights Reserved',
    packages = ['pylmldb'],
    python_requires='>=3.7',
    install_requires = ['pymarc==3.1.L',
                        'requests==2.22.0',
                        'regex==2019.8.19',
                        'loguru==0.3.2',
                        'SQLAlchemy==1.3.9',
                        'tqdm==4.36.1'],
    dependency_links = ['git+git://github.com/adlpr/pymarc.git@master#egg=pymarc-3.1.L'],
    description = 'Package for pulling LMLDB MARC data from Voyager and reading/storing it locally',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    # classifiers = ...,
)
