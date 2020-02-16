# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = ""

setup(
    long_description=readme,
    name="violet",
    version="0.2.2",
    description="An asyncio background job library",
    python_requires="==3.*,>=3.7.0",
    author="elixi.re",
    author_email="python@elixi.re",
    license="LGPL-3.0",
    packages=["violet"],
    package_dir={"": "."},
    package_data={"violet": ["py.typed"]},
    install_requires=[
        "asyncpg==0.20.1",
        "hail @ git+https://gitlab.com/elixire/hail.git@master#egg=hail",
    ],
)
