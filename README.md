# SQLAlchemy-Paradox

A SQLAlchemy dialect for the Microsoft Paradox DB ODBC Driver

## Objectives

This dialect is mainly intended to offer an easy way to access the
Paradox DB flat-file databases of older or EOL'd application-specific
softwares. It is designed for use with the ODBC driver included with
most versions of Microsoft Windows, `Microsoft Paradox Driver (*.db)` /s
**ODBCJT32.DLL**.

## Pre-requisites

- A System or User DSN configured to use the Microsoft Paradox driver

- 32-bit Python. The Microsoft Paradox driver *may* come in a 64-bit
  version, but using it might run into the same "bittedness" issue
  experienced with other JET-based ODBC drivers.

## Co-requisites

This dialect requires SQLAlchemy and pyodbc. They are both specified as
requirements so `pip` will install them if they are not already in
place. To install separately, just:

> `pip install -U SQLAlchemy pyodbc`

## Installation

At the time of this writing, I've not yet deemed this library suitable
(read: worthy) of publication on PyPI. For now, you'll have to install
it with:

> `pip install -U
> git+https://github.com/the-wondersmith/sqlalchemy-paradox`

## Getting Started

Create an `ODBC DSN (Data Source Name)` that points to the directory
containing your Paradox table files.

Then, in your Python app, you can connect to the database via:

```python
import sqlalchemy_paradox
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


db = create_engine("paradox+pyodbc://@your_dsn", echo=False)
super_session = sessionmaker(bind=db)
super_session.configure(autoflush=True, autocommit=True, expire_on_commit=True)
session = super_session()
```

## The SQLAlchemy Project

SQLAlchemy-Paradox is based on SQLAlchemy-access, which is part of the
[SQLAlchemy Project](https://www.sqlalchemy.org) and *tries* to adhere
to the same standards and conventions as the core project.

At the time of this writing, it's unlikely that SQLAlchemy-Paradox
actually *does* comply with the aforementioned standards and
conventions. That will be rectified (if and when) in a future release.

## Development / Bug reporting / Pull requests

SQLAlchemy maintains a
[Community Guide](https://www.sqlalchemy.org/develop.html) detailing
guidelines on coding and participating in that project.

While I'm aware that this project could desperately use the
participation of anyone else who actually knows what they're doing,
Paradox DB may be so esoteric and obscure (at the time of this writing)
that I wouldn't reasonably expect anyone to actually want to. If I am
mistaken in that belief, *please God* submit a pull request.

This library technically *works*, but it's *far* from feature-complete.

## License

This library is derived almost in its entirety from the
SQLAlchemy-Access library written by
[Gord Thompson](https://github.com/gordthompson). As such, and given
that SQLAlchemy-access is distributed under the
[MIT license](https://opensource.org/licenses/MIT), this library is
subject to the same licensure and grant of rights as its parent works
[SQLALchemy](https://www.sqlalchemy.org/) and
[SQLAlchemy-Access](https://github.com/sqlalchemy/sqlalchemy-access).
