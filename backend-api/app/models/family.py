from pydantic import BaseModel


class HomepageCountsResponse(BaseModel):
    """Response for the count of families by category per corpus for the homepage."""

    family_category_counts: dict[str, int]
