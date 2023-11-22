import datetime
from contextlib import contextmanager

import pytest
from freezegun import freeze_time
from sqlalchemy import event
from surrogate import surrogate


@pytest.fixture()
def patch_current_time():
    return patch_time


@contextmanager
def patch_time(time_to_freeze, entity, tick=False):
    with freeze_time(time_to_freeze, tick=tick) as frozen_time:

        def set_initial_timestamp(mapper, connection, target):
            now = datetime.datetime.now(datetime.timezone.utc)

            if hasattr(target, "created"):
                target.created = now
            if hasattr(target, "last_modified"):
                target.last_modified = now

        event.listen(entity, "before_insert", set_initial_timestamp, propagate=True)
        yield frozen_time
        event.remove(entity, "before_insert", set_initial_timestamp)


@pytest.fixture(scope="session", autouse=True)
def stub_freezegun_dynamic_imports():
    """Needed for patch_time to work due to freezegun imports."""
    with surrogate("transformers.models.deprecated.open_llama.tokenization_open_llama"):
        with surrogate(
            "transformers.models.deprecated.open_llama.tokenization_open_llama_fast"
        ):
            with surrogate("transformers.models.open_llama.tokenization_open_llama"):
                with surrogate(
                    "transformers.models.open_llama.tokenization_open_llama_fast"
                ):
                    yield
