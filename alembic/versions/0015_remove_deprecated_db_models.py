"""remove deprecated db models

Revision ID: 0015
Revises: 0014
Create Date: 2023-05-07 10:59:09.807015

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0015'
down_revision = '0014'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_user_email', table_name='user')
    op.drop_index('ix_document_import_id', table_name='document')
    op.drop_index('ix_document_slug', table_name='document')
    op.drop_table('document_framework')
    op.drop_table('document_response')
    op.drop_table('document_keyword')
    op.drop_table('document_instrument')
    op.drop_table('document_hazard')
    op.drop_table('document_relationship')
    op.drop_table('document_sector')
    op.drop_table('document_language')
    op.drop_table('keyword')
    op.drop_table('association')
    op.drop_table('framework')
    op.drop_table('response')
    op.drop_table('passage')
    op.drop_table('passage_type')
    op.drop_table('event')
    op.drop_table('document')
    op.drop_table('document_type')
    op.drop_table('relationship')
    op.drop_table('sector')
    op.drop_table('hazard')
    op.drop_table('instrument')
    op.drop_table('category')
    op.drop_table('source')
    op.drop_table('password_reset_token')
    op.drop_table('user')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(op.f('uq_app_user__email'), 'app_user', type_='unique')
    op.create_table('document_instrument',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('instrument_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_document_instrument__document_id__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['instrument_id'], ['instrument.id'], name='fk_document_instrument__instrument_id__instrument'),
    sa.PrimaryKeyConstraint('id', name='pk_document_instrument')
    )
    op.create_table('document_keyword',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('keyword_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_document_keyword__document_id__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['keyword_id'], ['keyword.id'], name='fk_document_keyword__keyword_id__keyword'),
    sa.PrimaryKeyConstraint('id', name='pk_document_keyword')
    )
    op.create_table('document_response',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('response_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_document_response__document_id__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['response_id'], ['response.id'], name='fk_document_response__response_id__response'),
    sa.PrimaryKeyConstraint('id', name='pk_document_response')
    )
    op.create_table('document_framework',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('framework_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_document_framework__document_id__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['framework_id'], ['framework.id'], name='fk_document_framework__framework_id__framework'),
    sa.PrimaryKeyConstraint('id', name='pk_document_framework')
    )
    op.create_table('password_reset_token',
    sa.Column('created_ts', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('now()'), autoincrement=False, nullable=True),
    sa.Column('updated_ts', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=True),
    sa.Column('id', sa.BIGINT(), autoincrement=True, nullable=False),
    sa.Column('token', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('expiry_ts', postgresql.TIMESTAMP(), autoincrement=False, nullable=False),
    sa.Column('is_redeemed', sa.BOOLEAN(), autoincrement=False, nullable=False),
    sa.Column('is_cancelled', sa.BOOLEAN(), autoincrement=False, nullable=False),
    sa.Column('user_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['user.id'], name='fk_password_reset_token__user_id__user'),
    sa.PrimaryKeyConstraint('id', name='pk_password_reset_token'),
    sa.UniqueConstraint('token', name='uq_password_reset_token__token')
    )
    op.create_table('category',
    sa.Column('id', sa.INTEGER(), server_default=sa.text("nextval('category_id_seq'::regclass)"), autoincrement=True, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='pk_category'),
    sa.UniqueConstraint('name', name='uq_category__name'),
    postgresql_ignore_search_path=False
    )
    op.create_table('instrument',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('parent_id', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('source_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['parent_id'], ['instrument.id'], name='fk_instrument__parent_id__instrument'),
    sa.ForeignKeyConstraint(['source_id'], ['source.id'], name='fk_instrument__source_id__source'),
    sa.PrimaryKeyConstraint('id', name='pk_instrument'),
    sa.UniqueConstraint('name', 'source_id', 'parent_id', name='uq_instrument__name')
    )
    op.create_table('hazard',
    sa.Column('id', sa.INTEGER(), server_default=sa.text("nextval('hazard_id_seq'::regclass)"), autoincrement=True, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='pk_hazard'),
    sa.UniqueConstraint('name', name='uq_hazard__name'),
    postgresql_ignore_search_path=False
    )
    op.create_table('document_language',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('language_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_document_language__document_id__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['language_id'], ['language.id'], name='fk_document_language__language_id__language'),
    sa.PrimaryKeyConstraint('id', name='pk_document_language')
    )
    op.create_table('event',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('created_ts', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_event__document_id__document', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id', name='pk_event')
    )
    op.create_table('document_relationship',
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('relationship_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_document_relationship__document_id__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['relationship_id'], ['relationship.id'], name='fk_document_relationship__relationship_id__relationship', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('document_id', 'relationship_id', name='pk_document_relationship')
    )
    op.create_table('document_hazard',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('hazard_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_document_hazard__document_id__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['hazard_id'], ['hazard.id'], name='fk_document_hazard__hazard_id__hazard'),
    sa.PrimaryKeyConstraint('id', name='pk_document_hazard')
    )
    op.create_table('source',
    sa.Column('id', sa.INTEGER(), server_default=sa.text("nextval('source_id_seq'::regclass)"), autoincrement=True, nullable=False),
    sa.Column('name', sa.VARCHAR(length=128), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='pk_source'),
    sa.UniqueConstraint('name', name='uq_source__name'),
    postgresql_ignore_search_path=False
    )
    op.create_table('sector',
    sa.Column('id', sa.INTEGER(), server_default=sa.text("nextval('sector_id_seq'::regclass)"), autoincrement=True, nullable=False),
    sa.Column('parent_id', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('source_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['parent_id'], ['sector.id'], name='fk_sector__parent_id__sector'),
    sa.ForeignKeyConstraint(['source_id'], ['source.id'], name='fk_sector__source_id__source'),
    sa.PrimaryKeyConstraint('id', name='pk_sector'),
    sa.UniqueConstraint('name', 'source_id', 'parent_id', name='uq_sector__name'),
    postgresql_ignore_search_path=False
    )
    op.create_table('document_sector',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('sector_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_document_sector__document_id__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['sector_id'], ['sector.id'], name='fk_document_sector__sector_id__sector'),
    sa.PrimaryKeyConstraint('id', name='pk_document_sector')
    )
    op.create_table('passage',
    sa.Column('id', sa.BIGINT(), autoincrement=True, nullable=False),
    sa.Column('document_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('page_id', sa.BIGINT(), autoincrement=False, nullable=False),
    sa.Column('passage_type_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('parent_passage_id', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('language_id', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('text', sa.TEXT(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['document.id'], name='fk_passage__document_id__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['language_id'], ['language.id'], name='fk_passage__language_id__language'),
    sa.ForeignKeyConstraint(['parent_passage_id'], ['passage.id'], name='fk_passage__parent_passage_id__passage'),
    sa.ForeignKeyConstraint(['passage_type_id'], ['passage_type.id'], name='fk_passage__passage_type_id__passage_type'),
    sa.PrimaryKeyConstraint('id', name='pk_passage')
    )
    op.create_table('relationship',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('type', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='pk_relationship')
    )
    op.create_table('passage_type',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='pk_passage_type')
    )
    op.create_table('document',
    sa.Column('created_ts', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('now()'), autoincrement=False, nullable=True),
    sa.Column('updated_ts', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=True),
    sa.Column('id', sa.INTEGER(), server_default=sa.text("nextval('document_id_seq'::regclass)"), autoincrement=True, nullable=False),
    sa.Column('publication_ts', postgresql.TIMESTAMP(), autoincrement=False, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('source_url', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('source_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('url', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('md5_sum', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('geography_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('type_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('category_id', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('slug', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('import_id', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('cdn_object', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('content_type', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('postfix', sa.TEXT(), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['category_id'], ['category.id'], name='fk_document__category_id__category'),
    sa.ForeignKeyConstraint(['geography_id'], ['geography.id'], name='fk_document__geography_id__geography'),
    sa.ForeignKeyConstraint(['source_id'], ['source.id'], name='fk_document__source_id__source'),
    sa.ForeignKeyConstraint(['type_id'], ['document_type.id'], name='fk_document__type_id__document_type'),
    sa.PrimaryKeyConstraint('id', name='pk_document'),
    sa.UniqueConstraint('source_id', 'import_id', name='uq_document__source_id'),
    postgresql_ignore_search_path=False
    )
    op.create_index('ix_document_slug', 'document', ['slug'], unique=False)
    op.create_index('ix_document_import_id', 'document', ['import_id'], unique=False)
    op.create_table('document_type',
    sa.Column('id', sa.INTEGER(), server_default=sa.text("nextval('document_type_id_seq'::regclass)"), autoincrement=True, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='pk_document_type'),
    sa.UniqueConstraint('name', name='uq_document_type__name'),
    postgresql_ignore_search_path=False
    )
    op.create_table('response',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='pk_response'),
    sa.UniqueConstraint('name', name='uq_response__name')
    )
    op.create_table('framework',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='pk_framework'),
    sa.UniqueConstraint('name', name='uq_framework__name')
    )
    op.create_table('association',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('document_id_from', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('document_id_to', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('type', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.ForeignKeyConstraint(['document_id_from'], ['document.id'], name='fk_association__document_id_from__document', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['document_id_to'], ['document.id'], name='fk_association__document_id_to__document', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id', name='pk_association')
    )
    op.create_table('keyword',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('name', sa.TEXT(), autoincrement=False, nullable=False),
    sa.Column('description', sa.TEXT(), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('id', name='pk_keyword')
    )
    op.create_table('user',
    sa.Column('created_ts', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('now()'), autoincrement=False, nullable=True),
    sa.Column('updated_ts', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=True),
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('email', sa.VARCHAR(), autoincrement=False, nullable=False),
    sa.Column('names', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('hashed_password', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('is_active', sa.BOOLEAN(), autoincrement=False, nullable=False),
    sa.Column('is_superuser', sa.BOOLEAN(), autoincrement=False, nullable=False),
    sa.Column('job_role', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('location', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('affiliation_organisation', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('affiliation_type', postgresql.ARRAY(sa.TEXT()), autoincrement=False, nullable=True),
    sa.Column('policy_type_of_interest', postgresql.ARRAY(sa.TEXT()), autoincrement=False, nullable=True),
    sa.Column('geographies_of_interest', postgresql.ARRAY(sa.TEXT()), autoincrement=False, nullable=True),
    sa.Column('data_focus_of_interest', postgresql.ARRAY(sa.TEXT()), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('id', name='pk_user')
    )
    op.create_index('ix_user_email', 'user', ['email'], unique=False)
    # ### end Alembic commands ###