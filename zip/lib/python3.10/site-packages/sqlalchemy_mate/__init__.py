# -*- coding: utf-8 -*-

from ._version import __version__

__short_description__ = "A library extend sqlalchemy module, makes CRUD easier."
__license__ = "MIT"
__author__ = "Sanhe Hu"
__author_email__ = "husanhe@gmail.com"
__github_username__ = "MacHu-GWU"


try:
    from . import engine_creator, io, pt, types
    from .utils import test_connection
    from .engine_creator import EngineCreator
    from .crud import selecting, inserting, updating, deleting
    from .orm.extended_declarative_base import ExtendedBase
    from .pkg.timeout_decorator import TimeoutError
except ImportError as e:  # pragma: no cover
    print(e)
except Exception as e:  # pragma: no cover
    raise e
