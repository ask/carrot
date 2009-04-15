#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

carrot = __import__("carrot", {}, {}, [""])

setup(
    name='carrot',
    version=carrot.__version__,
    description=carrot.__doc__,
    author=carrot.__author__,
    author_email=carrot.__contact__,
    url=carrot.__homepage__,
    packages=find_packages(exclude=['ez_setup']),
    install_requires=[
        'django>=1.0',
        'amqplib',
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: Django",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    long_description=codecs.open('README.rst', "r", "utf-8").read(),
)
