from pydantic import BaseModel


class FamilyCategoryCount(BaseModel):
    family_category: str
    count: int


class HomepageCountsResponse(BaseModel):
    """Response for the count of families by category per corpus for the homepage."""

    family_category_counts: list[FamilyCategoryCount]
    # total_families: int
