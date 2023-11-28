"""
Updated document triggers and last modified server default

Revision ID: 0021
Revises: 0020
Create Date: 2023-11-22 18:49:04.057129

"""
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_trigger import PGTrigger

from alembic import op

# revision identifiers, used by Alembic.
revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade():
    public_update_1_last_modified = PGFunction(
        schema="public",
        signature="update_1_last_modified()",
        definition="""
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.last_modified = NOW();
        RETURN NEW;
    END;
    $$ language 'plpgsql'""",
    )
    op.create_entity(public_update_1_last_modified)  # type: ignore

    public_family_document_update_last_modified = PGTrigger(
        schema="public",
        signature="update_last_modified",
        on_entity="public.family_document",
        is_constraint=False,
        definition="""
    BEFORE UPDATE ON public.family_document
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_1_last_modified()""",
    )
    op.replace_entity(public_family_document_update_last_modified)  # type: ignore

    public_update_last_modified = PGFunction(
        schema="public",
        signature="update_last_modified()",
        definition="""
    returns trigger
    LANGUAGE plpgsql
    AS $function$
    BEGIN
        NEW.last_modified = NOW();
        RETURN NEW;
    END;
    $function$""",
    )
    op.drop_entity(public_update_last_modified)  # type: ignore


def downgrade():
    public_update_last_modified = PGFunction(
        schema="public",
        signature="update_last_modified()",
        definition="""
    returns trigger
    LANGUAGE plpgsql
    AS $function$
    BEGIN
        NEW.last_modified = NOW();
        RETURN NEW;
    END;
    $function$""",
    )
    op.create_entity(public_update_last_modified)  # type: ignore

    public_family_document_update_last_modified = PGTrigger(
        schema="public",
        signature="update_last_modified",
        on_entity="public.family_document",
        is_constraint=False,
        definition="""
    BEFORE UPDATE ON public.family_document
    FOR EACH ROW
    EXECUTE PROCEDURE public.update_last_modified()""",
    )
    op.replace_entity(public_family_document_update_last_modified)  # type: ignore

    public_update_1_last_modified = PGFunction(
        schema="public",
        signature="update_1_last_modified()",
        definition="""
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.last_modified = NOW();
        RETURN NEW;
    END;
    $$ language 'plpgsql'""",
    )
    op.drop_entity(public_update_1_last_modified)  # type: ignore
