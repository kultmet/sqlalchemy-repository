from abc import ABC, abstractmethod
import typing

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession


class Criteries:
    def __init__(self, **criteries) -> None:
        self._criteries = criteries

    def add_model(self, model):
        self._model = model
        return self

    def _get_critery(self, model, key, value):
        return model.__table__.c.get(key) == value

    def _build_equality_criteria(self, model, **kwargs):
        for key, value in kwargs.items():
            yield self._get_critery(model, key, value)

    def get_criteries(self):
        return self._build_equality_criteria(self._model, **self._criteries)


class Repository(ABC):
    @abstractmethod
    async def create(self, **data):
        raise NotImplementedError

    @abstractmethod
    async def get(self, id):
        raise NotImplementedError

    @abstractmethod
    async def list(self, *creteries):
        raise NotImplementedError

    @abstractmethod
    async def update(self, *creteries, **data):
        raise NotImplementedError

    @abstractmethod
    async def delete(self, *creteries):
        raise NotImplementedError

    @abstractmethod
    async def one(self, *creteries):
        raise NotImplementedError


class SQLAlchemyRepository(Repository):
    def __init__(self, model, session: AsyncSession) -> None:
        self._model = model
        self._session: AsyncSession = session

    def _check_empty_criteries(self, criteries: typing.Optional[Criteries]) -> Criteries:
        if criteries is None:
            return Criteries()
        return criteries

    def _check_type(self, criteries):
        if not isinstance(criteries, (Criteries, type(None))):
            raise TypeError(
                f"Required type 'Criteries' For 'criteries' argument, not '{type(criteries)}'"
            )
        return criteries

    async def create(self, **data):
        await self._session.execute(insert(self._model).values(**data))

    async def get(self, id):
        return await self._session.get(self._model, id)

    async def list(self, criteries: typing.Optional[Criteries] = None):
        self._check_type(criteries)
        criteries = (
            self._check_empty_criteries(criteries).add_model(self._model).get_criteries()
        )
        return (
            (await self._session.execute(select(self._model).where(*criteries)))
            .scalars()
            .all()
        )

    async def update(self, criteries: typing.Optional[Criteries] = None, **data):
        self._check_type(criteries)
        criteries = (
            self._check_empty_criteries(criteries).add_model(self._model).get_criteries()
        )
        await self._session.execute(update(self._model).values(**data).where(*criteries))

    async def one(self, criteries: typing.Optional[Criteries] = None):
        self._check_type(criteries)
        criteries = (
            self._check_empty_criteries(criteries).add_model(self._model).get_criteries()
        )
        return (
            await self._session.execute(select(self._model).where(*criteries))
        ).scalar()

    async def delete(self, criteries: typing.Optional[Criteries] = None):
        self._check_type(criteries)
        criteries = (
            self._check_empty_criteries(criteries).add_model(self._model).get_criteries()
        )
        return await self._session.execute(delete(self._model).where(*criteries))
