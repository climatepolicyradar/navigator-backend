"""
Schema for counters.

The following section includes the necessary schema for maintaining the counts
of different entity types. These are scoped per "data source" - however the 
concept of "data source" is not yet implemented, see PDCT-431.
"""
from enum import Enum
import sqlalchemy as sa

from app.db.session import Base

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

    id = sa.Column(sa.Integer, primary_key=True)
    description = sa.Column(sa.String)
    prefix = sa.Column(sa.String, unique=True)  # Organisation.name
    counter = sa.Column(sa.Integer, default=1)

    def get_import_id(self, entity: CountedEntity, n: int = 0) -> str:
        """gets an import id"""
        # Validation
        prefix_ok = (
            self.prefix == ORGANISATION_CCLW or self.prefix == ORGANISATION_UNFCCC
        )
        if not prefix_ok:
            raise RuntimeError("Prefix is not a know organisation!")

        # TODO Read and increment counter

        return f"{self.prefix}.{entity.value}.i{self.counter}.{n}"
