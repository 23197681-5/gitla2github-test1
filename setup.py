# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = ""

setup(
    long_description=readme,
    name="violet",
    version="0.3.0",
    description="An asyncio background job library",
    python_requires="==3.*,>=3.7.0",
    author="elixi.re",
    author_email="python@elixi.re",
    license="LGPL-3.0",
    packages=["violet"],
    package_dir={"": "."},
    package_data={"violet": ["py.typed"]},
    install_requires=[
        "hail @ git+https://gitlab.com/elixire/hail.git@1133e3c25f8eb6b8ff6c65c7ebac57071c3275d9#egg=hail",
    ],
    extras_require={"asyncpg": ["asyncpg > 0.20"]},
)
