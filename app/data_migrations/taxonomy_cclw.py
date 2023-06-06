from app.data_migrations.taxonomy_utils import read_taxonomy_values


TAXONOMY_DATA = [
    {
        "key": "topic",
        "filename": "app/data_migrations/data/cclw/topic_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
        "allow_any": False,
    },
    {
        "key": "sector",
        "filename": "app/data_migrations/data/cclw/sector_data.json",
        "file_key_path": "node.name",
        "allow_blanks": True,
        "allow_any": False,
    },
    {
        "key": "keyword",
        "filename": "app/data_migrations/data/cclw/keyword_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
        "allow_any": False,
    },
    {
        "key": "instrument",
        "filename": "app/data_migrations/data/cclw/instrument_data.json",
        "file_key_path": "node.name",
        "allow_blanks": True,
        "allow_any": False,
    },
    {
        "key": "hazard",
        "filename": "app/data_migrations/data/cclw/hazard_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
        "allow_any": False,
    },
    {
        "key": "framework",
        "filename": "app/data_migrations/data/cclw/framework_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
        "allow_any": False,
    },
]


def get_cclw_taxonomy():
    taxonomy = read_taxonomy_values(TAXONOMY_DATA)

    # Remove unwanted values for new taxonomy
    if "sector" in taxonomy:
        sectors = taxonomy["sector"]["allowed_values"]
        if "Transportation" in sectors:
            taxonomy["sector"]["allowed_values"] = [
                s for s in sectors if s != "Transportation"
            ]

    return taxonomy
