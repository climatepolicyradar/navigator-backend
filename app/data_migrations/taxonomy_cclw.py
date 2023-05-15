from app.data_migrations.taxonomy_utils import load_metadata_type


TAXONOMY_DATA = [
    {
        "key": "topic",
        "filename": "app/data_migrations/data/cclw/topic_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "sector",
        "filename": "app/data_migrations/data/cclw/sector_data.json",
        "file_key_path": "node.name",
        "allow_blanks": True,
    },
    {
        "key": "keyword",
        "filename": "app/data_migrations/data/cclw/keyword_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "instrument",
        "filename": "app/data_migrations/data/cclw/instrument_data.json",
        "file_key_path": "node.name",
        "allow_blanks": True,
    },
    {
        "key": "hazard",
        "filename": "app/data_migrations/data/cclw/hazard_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
    {
        "key": "framework",
        "filename": "app/data_migrations/data/cclw/framework_data.json",
        "file_key_path": "name",
        "allow_blanks": True,
    },
]


def get_cclw_taxonomy():
    taxonomy = {}
    for data in TAXONOMY_DATA:
        taxonomy.update(
            {
                data["key"]: {
                    "allowed_values": load_metadata_type(
                        data["filename"], data["file_key_path"]
                    ),
                    "allow_blanks": data["allow_blanks"],
                },
            }
        )

    # Remove unwanted values for new taxonomy
    if (
        "sector" in taxonomy
        and "Transportation" in taxonomy["sector"]["allowed_values"]
    ):
        taxonomy["sector"]["allowed_values"].remove("Transportation")

    return taxonomy
