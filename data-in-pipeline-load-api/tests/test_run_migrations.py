from pytest_alembic import tests as test_alembic


def test_model_definitions_match_ddl(alembic_runner):
    test_alembic.test_model_definitions_match_ddl(alembic_runner)


# def test_single_head_revision(alembic_runner):
#     test_alembic.test_single_head_revision(alembic_runner)


# def test_upgrade(alembic_runner):
#     test_alembic.test_upgrade(alembic_runner)


# def test_up_down_consistency(alembic_runner):
#     test_alembic.test_up_down_consistency(alembic_runner)


# # Experimental pytest-alembic tests
# # https://pytest-alembic.readthedocs.io/en/latest/experimental_tests.html
# def test_all_models_register_on_metadata(alembic_runner):
#     test_alembic.all_models_register_on_metadata(alembic_runner)


# def test_downgrade_leaves_no_trace(alembic_runner):
#     test_alembic.test_downgrade_leaves_no_trace(alembic_runner)
