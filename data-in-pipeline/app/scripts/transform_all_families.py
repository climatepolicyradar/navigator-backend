import json

from returns.result import Failure, Success

from app.extract.connectors import NavigatorDocument, NavigatorFamily
from app.models import Identified
from app.transform.navigator_family import transform_navigator_family

if __name__ == "__main__":
    # You will need to run cache_all_navigator_families.py first if you have not previously.
    with open(".data_cache/navigator_families.json", "r") as f:
        families = json.load(f)

    results = []
    successes = []
    failures = []
    for family in families:
        result = transform_navigator_family(
            Identified(
                data=NavigatorFamily(
                    import_id=family["import_id"],
                    title=family["title"],
                    documents=[
                        NavigatorDocument(
                            import_id=doc["import_id"], title=doc["title"]
                        )
                        for doc in family["documents"]
                    ],
                ),
                id=family["import_id"],
                source="navigator_family",
            )
        )
        match result:
            case Success(document):
                successes.append(document)
            case Failure(error):
                failures.append(error)

    print(f"Successes: {len(successes)}")
    print(f"Failures: {len(failures)}")
