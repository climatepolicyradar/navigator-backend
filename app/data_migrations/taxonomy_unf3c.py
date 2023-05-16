from app.data_migrations.taxonomy_utils import read_taxonomy_values


TAXONOMY_DATA = [
    {
        "key": "submission_type",
        "filename": "app/data_migrations/data/unf3c/submission_type_data.json",
        "file_key_path": "name",
        "allow_blanks": False,
    },
    {
        "key": "author_type",
        "allow_blanks": False,
        "allowed_values": ["Party", "Non-Party"],
    },
]


def get_unf3c_taxonomy():
    return read_taxonomy_values(TAXONOMY_DATA)
