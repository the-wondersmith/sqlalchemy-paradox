import os
import re

from setuptools import setup, find_packages

v = open(
    os.path.join(os.path.dirname(__file__), "sqlalchemy_paradox", "__init__.py")
)
VERSION = re.compile(r'.*__version__ = "(.*?)"', re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), "README.md")


setup(
    name="sqlalchemy-paradox",
    version=VERSION,
    description="Paradox DB support for SQLAlchemy",
    long_description=open(readme).read(),
    url="https://github.com/the-wondersmith/sqlalchemy-paradox",
    author="Mark S.",
    author_email="developers@pawn-pay.com",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
    ],
    keywords=["SQLAlchemy", "Paradox", "Borland"],
    project_urls={
        "Source": "https://github.com/the-wondersmith/sqlalchemy-paradox",
    },
    packages=find_packages(include=["sqlalchemy_paradox"]),
    include_package_data=True,
    install_requires=["SQLAlchemy", "pyodbc>=4.0.27"],
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": [
            "paradox = sqlalchemy_paradox.pyodbc:ParadoxDialect_pyodbc",
            "paradox.pyodbc = sqlalchemy_paradox.pyodbc:ParadoxDialect_pyodbc",
        ]
    },
)
