import logging
import sqlalchemy as sa
from contextlib import contextmanager
from recap.analyzers.abstract import AbstractAnalyzer
from typing import Generator


log = logging.getLogger(__name__)


class AbstractDatabaseAnalyzer(AbstractAnalyzer):
    def __init__(
        self,
        engine: sa.engine.Engine,
        **_,
    ):
        self.engine = engine
