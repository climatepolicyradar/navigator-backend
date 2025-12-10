import json
import os

from returns.result import Failure, Success

from app.extract.connectors import (
    NavigatorCorpus,
    NavigatorCorpusType,
    NavigatorDocument,
    NavigatorEvent,
    NavigatorFamily,
)
from app.models import Identified
from app.transform.data import (
    unfccc_copenhagen_accord_document,
    unfccc_document,
    unfccc_kyoto_protocol_document,
    unfccc_kyoto_protocol_doha_amendment_document,
    unfccc_paris_agreement_document,
)
from app.transform.navigator_family import transform_navigator_family

if __name__ == "__main__":
    """
    This script is used to assess how many families will be trasnformed when adding new trasnformations.
    This helps speed up the development process by making the data available locally.

    You will need to run `uv run python -m app.scripts.cache_all_navigator_families` at least once to make the data available locally.

    We don't run that command as part of this one, as that defeats the point of caching.
    """
    with open(".data_cache/navigator_families.json", "r") as f:
        families = json.load(f)

    results = []
    successes = [
        unfccc_document,
        unfccc_kyoto_protocol_document,
        unfccc_copenhagen_accord_document,
        unfccc_kyoto_protocol_doha_amendment_document,
        unfccc_paris_agreement_document,
    ]
    failures = []
    for family in families:
        result = transform_navigator_family(
            Identified(
                data=NavigatorFamily(
                    import_id=family["import_id"],
                    title=family["title"],
                    corpus=NavigatorCorpus(
                        import_id=family["corpus"]["import_id"],
                        corpus_type=NavigatorCorpusType(
                            name=family["corpus"]["corpus_type"]["name"],
                        ),
                    ),
                    documents=[
                        NavigatorDocument(
                            import_id=doc["import_id"],
                            title=doc["title"],
                            valid_metadata=doc["valid_metadata"],
                            events=[],
                        )
                        for doc in family["documents"]
                    ],
                    events=[
                        NavigatorEvent(
                            import_id=event["import_id"],
                            event_type=event["event_type"],
                            date=event["date"],
                        )
                        for event in family["events"]
                    ],
                ),
                id=family["import_id"],
                source="navigator_family",
            )
        )
        match result:
            case Success(document):
                successes.extend(document)
            case Failure(error):
                failures.append(error)

    os.makedirs(".data_cache/transformed_navigator_families", exist_ok=True)
    for file in os.listdir(".data_cache/transformed_navigator_families"):
        os.remove(f".data_cache/transformed_navigator_families/{file}")

    for document in successes:
        model_dump = document.model_dump_json(indent=4)
        with open(
            f".data_cache/transformed_navigator_families/{document.id}.json", "w"
        ) as f:
            f.write(model_dump)

    print(f"Successes: {len(successes)}")
    print(f"Failures: {len(failures)}")
