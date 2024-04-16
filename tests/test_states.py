import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import BinaryExpression

from src.signals import models
from src.states import schemas
from src.utils import Criteries, SQLAlchemyRepository
from tests.conftest import async_session_maker


class TestStates:
    def test_create_criteries_object(self):
        criteries = Criteries(id=1, wagon_number="1234")
        assert criteries._criteries == {"id": 1, "wagon_number": "1234"}
        with pytest.raises(AttributeError) as e:
            criteries._model
            criteries.get_criteries()
        criteries.add_model(models.Carriage)
        assert criteries._model == models.Carriage
        creteries_result = criteries.get_criteries()
        id_cretery = creteries_result.__next__()
        wagon_number_cretery = creteries_result.__next__()
        assert isinstance(id_cretery, BinaryExpression)
        assert id_cretery.__str__() == (models.Carriage.id == 1).__str__()
        assert (
            wagon_number_cretery.__str__()
            == (models.Carriage.wagon_number == "1234").__str__()
        )
        creteries_result = criteries.get_criteries()
        result = [*creteries_result]
        assert len(result) == 2

    def test_create_empty_criteries_object(self):
        empty_criteries = Criteries()
        empty_criteries.add_model(models.Carriage)
        creteries_result = empty_criteries.get_criteries()
        result = [*creteries_result]
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_sqllalchemy_repository_create(self):
        session: AsyncSession = None
        async with async_session_maker() as session:
            print(session)
            repository = SQLAlchemyRepository(models.Chemistry, session=session)
            chemistry = schemas.ChemistryBase(wt=1, fe=2, cao=3, sio2=4)
            await repository.create(**chemistry.model_dump())
            await session.commit()
            db_chemistry = (
                (await session.execute(select(models.Chemistry))).scalars().all()
            )
            assert len(db_chemistry) == 1
            await repository.create(**chemistry.model_dump())
            await session.commit()
            db_chemistry = (
                (await session.execute(select(models.Chemistry))).scalars().all()
            )
            assert len(db_chemistry) == 2
            await session.execute(delete(models.Chemistry))
            await session.commit()

    @pytest.mark.asyncio
    async def test_sqllalchemy_repository_list(self):
        session: AsyncSession = None
        async with async_session_maker() as session:
            chemistry = schemas.ChemistryBase(wt=1, fe=2, cao=3, sio2=4)
            chemistry2 = schemas.ChemistryBase(wt=2, fe=2, cao=3, sio2=4)
            await session.execute(
                insert(models.Chemistry).values(**chemistry.model_dump())
            )
            await session.execute(
                insert(models.Chemistry).values(**chemistry.model_dump())
            )
            await session.execute(
                insert(models.Chemistry).values(**chemistry2.model_dump())
            )
            await session.commit()
            db_chemistry = (
                (await session.execute(select(models.Chemistry))).scalars().all()
            )
            assert len(db_chemistry) == 3
            repository = SQLAlchemyRepository(models.Chemistry, session=session)
            with pytest.raises(TypeError):
                await repository.list(1)
            db_result = await repository.list(Criteries(wt=1))
            assert len(db_result) == 2
            await session.execute(delete(models.Chemistry))
            await session.commit()

    @pytest.mark.asyncio
    async def test_sqllalchemy_repository_get(self):
        session: AsyncSession = None
        async with async_session_maker() as session:
            chemistry = schemas.ChemistryBase(wt=1, fe=2, cao=3, sio2=4)
            chemistry2 = schemas.ChemistryBase(wt=2, fe=2, cao=3, sio2=4)
            await session.execute(
                insert(models.Chemistry).values(**chemistry.model_dump())
            )
            await session.execute(
                insert(models.Chemistry).values(**chemistry.model_dump())
            )
            await session.execute(
                insert(models.Chemistry).values(**chemistry2.model_dump())
            )
            await session.commit()
            db_chemistry = (
                (await session.execute(select(models.Chemistry))).scalars().all()
            )
            assert len(db_chemistry) == 3
            repository = SQLAlchemyRepository(models.Chemistry, session=session)
            chemistry_id = (
                await session.execute(
                    select(models.Chemistry.id).order_by(models.Chemistry.id)
                )
            ).scalar()
            single_db_chemistry = await repository.get(id=chemistry_id)
            assert single_db_chemistry is not None
            assert single_db_chemistry.wt == chemistry.wt
            await session.execute(delete(models.Chemistry))
            await session.commit()

    @pytest.mark.asyncio
    async def test_sqllalchemy_repository_one(self):
        session: AsyncSession = None
        async with async_session_maker() as session:
            chemistry = schemas.ChemistryBase(wt=1, fe=2, cao=3, sio2=4)
            chemistry2 = schemas.ChemistryBase(wt=2, fe=2, cao=3, sio2=4)
            await session.execute(
                insert(models.Chemistry).values(**chemistry.model_dump())
            )
            await session.execute(
                insert(models.Chemistry).values(**chemistry.model_dump())
            )
            await session.execute(
                insert(models.Chemistry).values(**chemistry2.model_dump())
            )
            await session.commit()
            db_chemistry = (
                (await session.execute(select(models.Chemistry))).scalars().all()
            )
            assert len(db_chemistry) == 3
            repository = SQLAlchemyRepository(models.Chemistry, session=session)
            with pytest.raises(TypeError):
                await repository.one(1)
            db_chemistry_one = await repository.one(Criteries(wt=2))
            assert db_chemistry_one is not None
            # assert db_chemistry_one.id == 11
            db_chemistry_within_criteries = await repository.one()
            assert db_chemistry_within_criteries is not None
            await session.execute(delete(models.Chemistry))
            await session.commit()

    @pytest.mark.asyncio
    async def test_sqllalchemy_repository_delete(self):
        session: AsyncSession = None
        async with async_session_maker() as session:
            chemistry = schemas.ChemistryBase(wt=1, fe=2, cao=3, sio2=4)
            chemistry2 = schemas.ChemistryBase(wt=2, fe=2, cao=3, sio2=4)
            await session.execute(
                insert(models.Chemistry).values(**chemistry.model_dump())
            )
            await session.execute(
                insert(models.Chemistry).values(**chemistry.model_dump())
            )
            await session.execute(
                insert(models.Chemistry).values(**chemistry2.model_dump())
            )
            await session.commit()
            db_chemistry = (
                (await session.execute(select(models.Chemistry))).scalars().all()
            )
            assert len(db_chemistry) == 3
            repository = SQLAlchemyRepository(models.Chemistry, session=session)
            with pytest.raises(TypeError):
                await repository.delete(1)
            await repository.delete(Criteries(**chemistry.model_dump()))
            db_chemistry = (
                (await session.execute(select(models.Chemistry))).scalars().all()
            )
            assert len(db_chemistry) == 1
            await repository.delete()
            await session.commit()
            db_chemistry = (
                (await session.execute(select(models.Chemistry))).scalars().all()
            )
            assert len(db_chemistry) == 0
            await session.execute(delete(models.Chemistry))
            await session.commit()
