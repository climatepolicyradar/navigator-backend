import datetime
from contextlib import contextmanager

import pytest
from freezegun import freeze_time
from sqlalchemy import event
from surrogate import surrogate

from app.db.models.law_policy.family import FamilyDocument


@pytest.fixture()
def patch_current_time():
    return patch_time


@contextmanager
def patch_time(time_to_freeze, tick=True):
    with freeze_time(time_to_freeze, tick=tick) as frozen_time:

        def set_initial_timestamp(mapper, connection, target):
            now = datetime.datetime.now()
            if hasattr(target, "created"):
                target.created = now

        event.listen(
            FamilyDocument, "before_insert", set_initial_timestamp, propagate=True
        )
        yield frozen_time
        event.remove(FamilyDocument, "before_insert", set_initial_timestamp)


@pytest.fixture(scope="session", autouse=True)
def stub_freezegun_dynamic_imports():
    with surrogate("transformers.models.deprecated.open_llama.tokenization_open_llama"):
        with surrogate(
            "transformers.models.deprecated.open_llama.tokenization_open_llama_fast"
        ):
            with surrogate("transformers.models.open_llama.tokenization_open_llama"):
                with surrogate(
                    "transformers.models.open_llama.tokenization_open_llama_fast"
                ):
                    yield
