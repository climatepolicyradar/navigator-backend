"""
Schema for counters.

The following section includes the necessary schema for maintaining the counts
of different entity types. These are scoped per "data source" - however the 
concept of "data source" is not yet implemented, see PDCT-431.
"""
import logging
from enum import Enum
import sqlalchemy as sa
from sqlalchemy.sql import text
from app.db.session import Base
from sqlalchemy.orm.session import object_session


_LOGGER = logging.getLogger(__name__)

#
# DO NOT ADD TO THIS LIST BELOW
#
# NOTE: These need to change when we introduce "Data source" (PDCT-431)
ORGANISATION_CCLW = "CCLW"
ORGANISATION_UNFCCC = "UNFCCC"


class CountedEntity(str, Enum):
    """Entities that are to be counted."""

    Collection = "collection"
    Family = "family"
    Document = "document"
    Event = "event"


class EntityCounters(Base):
    """
    A list of entity counters per organisation name.

    NOTE: There is no foreign key, as this is expected to change
    when we introduce data sources (PDCT-431). So at this time a
    FK to the new datasource table should be introduced.

    This is used for generating import_ids in the following format:

        <organisation.name>.<entity>.<counter>.<n>

    """

    __tablename__ = "entity_counters"
    __table_args__ = (
        sa.CheckConstraint(
            "prefix IN ('CCLW','UNFCCC')",
            name="prefix_allowed_orgs",
        ),
    )

    _get_and_increment = text(
        """
        WITH updated AS (
        UPDATE entity_counters SET counter = counter + 1 
        WHERE id = :id RETURNING counter
        )
        SELECT counter FROM updated;
        """
    )

    id = sa.Column(sa.Integer, primary_key=True)
    description = sa.Column(sa.String, nullable=False, default="")
    prefix = sa.Column(sa.String, unique=True, nullable=False)  # Organisation.name
    counter = sa.Column(sa.Integer, default=0)

    def get_import_id(self, entity: CountedEntity, n: int = 0) -> str:
        """gets an import id"""
        # Validation
        prefix_ok = (
            self.prefix == ORGANISATION_CCLW or self.prefix == ORGANISATION_UNFCCC
        )
        if not prefix_ok:
            raise RuntimeError("Prefix is not a known organisation!")

        try:
            db = object_session(self)
            cmd = self._get_and_increment.bindparams(id=self.id)
            i_value = str(db.execute(cmd).scalar()).zfill(8)
            db.commit()
            n_value = str(n).zfill(4)
            return f"{self.prefix}.{entity.value}.i{i_value}.n{n_value}"
        except:
            _LOGGER.exception(f"When generating counter for {self.prefix} / {entity}")
            raise
