from ..models import RegionType

regions = [
    {
        "name": "North America",
        "type": RegionType.WORLD_BANK_REGION,
        "slug": "north-america",
    },
    {
        "name": "East Asia & Pacific",
        "type": RegionType.WORLD_BANK_REGION,
        "slug": "east-asia-pacific",
    },
    {
        "name": "Latin America & Caribbean",
        "type": RegionType.WORLD_BANK_REGION,
        "slug": "latin-america-caribbean",
    },
    {
        "name": "Sub-Saharan Africa",
        "type": RegionType.WORLD_BANK_REGION,
        "slug": "sub-saharan-africa",
    },
    {
        "name": "Middle East & North Africa",
        "type": RegionType.WORLD_BANK_REGION,
        "slug": "middle-east-north-africa",
    },
    {
        "name": "Europe & Central Asia",
        "type": RegionType.WORLD_BANK_REGION,
        "slug": "europe-central-asia",
    },
    {
        "name": "South Asia",
        "type": RegionType.WORLD_BANK_REGION,
        "slug": "south-asia",
    },
    {
        "name": "Other",
        "type": RegionType.CPR_CUSTOM_REGION,
        "slug": "other",
    },
]
