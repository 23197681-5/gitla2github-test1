# -*- coding: utf-8 -*-

# DO NOT EDIT THIS FILE!
# This file has been autogenerated by dephell <3
# https://github.com/dephell/dephell

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = ''

setup(
    long_description=readme,
    name='violet',
    version='0.1.0',
    description='An asyncio background job library',
    python_requires='==3.*,>=3.7.0',
    author='elixi.re',
    author_email='python@elixi.re',
    license='LGPL-3.0',
    packages=['violet'],
    package_dir={"": "."},
    package_data={},
    install_requires=['asyncpg==0.*,>=0.19.0'],
)