#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

import carrot

setup(
    name='carrot',
    version=carrot.__version__,
    description=carrot.__doc__,
    author=carrot.__author__,
    author_email=carrot.__contact__,
    url=carrot.__homepage__,
    platforms=["any"],
    packages=find_packages(exclude=['ez_setup']),
    install_requires=[
        'amqplib',
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: Django",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    long_description=codecs.open('README', "r", "utf-8").read(),
)
