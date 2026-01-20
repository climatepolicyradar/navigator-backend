import pytest
from pytest_alembic import tests as test_alembic


@pytest.mark.alembic
def test_model_definitions_match_ddl(alembic_runner):
    test_alembic.test_model_definitions_match_ddl(alembic_runner)


@pytest.mark.alembic
@pytest.mark.xfail(
    reason="This test is failing because the first revision has not been created"
)
def test_single_head_revision(alembic_runner):
    test_alembic.test_single_head_revision(alembic_runner)


@pytest.mark.alembic
def test_upgrade(alembic_runner):
    test_alembic.test_upgrade(alembic_runner)


@pytest.mark.alembic
def test_up_down_consistency(alembic_runner):
    test_alembic.test_up_down_consistency(alembic_runner)


# Experimental pytest-alembic tests
# https://pytest-alembic.readthedocs.io/en/latest/experimental_tests.html
@pytest.mark.alembic
def test_downgrade_leaves_no_trace(alembic_runner):
    test_alembic.experimental.test_downgrade_leaves_no_trace(alembic_runner)  # type: ignore
