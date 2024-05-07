# -*- coding: utf-8 -*-

"""
Utilities function.
"""

from typing import Type, Union, Tuple, Dict, Iterable

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session


def ensure_exact_one_arg_is_not_none(*args):
    if sum([bool(arg is not None) for arg in args]) != 1:
        raise ValueError


def ensure_list(item) -> list:
    if not isinstance(item, (list, tuple)):
        return [item, ]
    else:
        return item


def grouper_list(l: Iterable, n: int) -> Iterable[list]:
    """Evenly divide list into fixed-length piece, no filled value if chunk
    size smaller than fixed-length.

    Example::

        >>> list(grouper(range(10), n=3)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    **中文文档**

    将一个列表按照尺寸n, 依次打包输出, 有多少输出多少, 并不强制填充包的大小到n。

    下列实现是按照性能从高到低进行排列的:

    - 方法1: 建立一个counter, 在向chunk中添加元素时, 同时将counter与n比较, 如果一致
      则yield。然后在最后将剩余的item视情况yield。
    - 方法2: 建立一个list, 每次添加一个元素, 并检查size。
    - 方法3: 调用grouper()函数, 然后对里面的None元素进行清理。
    """
    chunk = list()
    counter = 0
    for item in l:
        counter += 1
        chunk.append(item)
        if counter == n:
            yield chunk
            chunk = list()
            counter = 0
    if len(chunk) > 0:
        yield chunk


session_klass_cache = dict()  # type: Dict[int, Type[Session]]


def ensure_session(
    engine_or_session: Union[Engine, Session]
) -> Tuple[Session, bool]:
    """
    If it is an engine, then create a session from it. And indicate that
    this session should be closed after the job done.

    **中文文档**

    在 ORM 中对数据进行操作主要是通过 Session. 如果传入的参数是 Engine, 则创建一个
    Session, 用完之后是要 close 的, 所以 ``auto_close = True`` 因为这个
    Session 反正是新创建的. 如果传入的参数是 Session, 用完之后是否 close 取决于业务,
    所以 ``auto_close = False``.
    """
    if isinstance(engine_or_session, Engine):
        engine_id = id(engine_or_session)
        if engine_id not in session_klass_cache:  # pragma: no cover
            session_klass_cache[engine_id] = sessionmaker(bind=engine_or_session)
        SessionClass = session_klass_cache[engine_id]
        session = SessionClass()
        auto_close = True
        return session, auto_close
    elif isinstance(engine_or_session, Session):
        session = engine_or_session
        auto_close = False
        return session, auto_close


def clean_session(
    session: Session,
    auto_close: bool,
):
    """
    Close session if necessary. Just a syntax sugar.
    """
    if auto_close:
        session.close()


from .pkg import timeout_decorator


def test_connection(engine, timeout=3):
    @timeout_decorator.timeout(timeout)
    def _test_connection(engine):
        v = engine.execute(sa.text("SELECT 1;")).fetchall()[0][0]
        assert v == 1

    try:
        _test_connection(engine)
        return True
    except timeout_decorator.TimeoutError:
        raise timeout_decorator.TimeoutError(
            "time out in %s seconds!" % timeout)
    except AssertionError:  # pragma: no cover
        raise ValueError
    except Exception as e:
        raise e
