import glob
import json
import os

import duckdb
import requests
import yaml


def fetch_classifier_specs_file() -> str:
    """
    Fetch classifier specs file from GitHub repository.

    :return: Classifier specs file as text
    :rtype: str
    :raises ValueError: If unable to fetch the file
    """
    # GitHub raw content URL for the classifier specs
    owner = "climatepolicyradar"
    repo = "knowledge-graph"
    branch = "main"

    classifier_file_name = "production.yaml"
    if os.getenv("ENV") == "staging":
        classifier_file_name = "staging.yaml"

    path = f"flows/classifier_specs/v2/{classifier_file_name}"
    github_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"

    try:
        # Attempt to fetch the file
        response = requests.get(github_url, timeout=10)  # seconds
        response.raise_for_status()  # Raise an error for bad HTTP responses

        return response.text

    except requests.RequestException as req_err:
        print(f"GitHub fetch failed: {req_err}.")

    raise ValueError("Failed to fetch classifiers") from None


def parse_classifier_specs(classifier_specs_file: str) -> list[str]:
    """
    Parse classifier specs file.

    :param classifier_specs_file: Classifier specs file as text
    :type classifier_specs_file: str
    :return: list of concept IDs with classifiers
    :rtype: list[str]
    :raises ValueError: If unable to parse the YAML file
    """
    try:
        # Parse the YAML content
        classifier_specs = yaml.safe_load(classifier_specs_file)

        # Extract concept IDs, removing version numbers
        concepts_with_classifiers = [
            concept["wikibase_id"] for concept in classifier_specs if concept
        ]

        if not concepts_with_classifiers:
            raise ValueError("No concepts with classifiers found in the YAML file")

        return concepts_with_classifiers

    except yaml.YAMLError as yaml_err:
        raise ValueError(f"Failed to parse YAML file: {yaml_err}") from yaml_err


def get_classifier_specs() -> list[str]:
    """
    Get concepts with classifiers from classifier specs file.

    :return: list of concept IDs with classifiers
    :rtype: list[str]
    """
    classifier_specs_file = fetch_classifier_specs_file()
    classifier_specs = parse_classifier_specs(classifier_specs_file)
    return classifier_specs


def main():
    con = duckdb.connect("initial-data/concepts.db")
    con.execute(
        """
    DROP TABLE IF EXISTS concept_related_relations;
    DROP TABLE IF EXISTS concept_subconcept_relations;
    DROP TABLE IF EXISTS concepts;
    """
    )

    con.execute(
        """
    -- Main concepts table
    CREATE TABLE IF NOT EXISTS concepts (
        wikibase_id VARCHAR PRIMARY KEY,
        preferred_label VARCHAR,
        alternative_labels VARCHAR[],
        negative_labels VARCHAR[],
        description VARCHAR,
        definition VARCHAR,
        labelled_passages JSON,
        has_classifier BOOLEAN,
    );

    -- Relationship tables with unique constraints
    CREATE TABLE IF NOT EXISTS concept_subconcept_relations (
        concept_id VARCHAR,
        subconcept_id VARCHAR,
        FOREIGN KEY (concept_id) REFERENCES concepts(wikibase_id),
        FOREIGN KEY (subconcept_id) REFERENCES concepts(wikibase_id),
        UNIQUE(concept_id, subconcept_id)  -- Prevents duplicate relationships
    );

    CREATE TABLE IF NOT EXISTS concept_related_relations (
        concept_id1 VARCHAR,
        concept_id2 VARCHAR,
        FOREIGN KEY (concept_id1) REFERENCES concepts(wikibase_id),
        FOREIGN KEY (concept_id2) REFERENCES concepts(wikibase_id),
        UNIQUE(concept_id1, concept_id2)  -- Prevents duplicate relationships
    );
    """
    )

    # Get concepts with classifiers
    try:
        classifiers = get_classifier_specs()
        print("Concepts with classifiers:", classifiers)
    except ValueError as e:
        print(f"Error: {e}")
        # Optionally, you might want to exit or handle this differently
        raise

    # First pass: Insert all concepts into the database
    json_files = glob.glob("initial-data/*.json")
    for file_path in json_files:
        with open(file_path, "r") as f:
            data = json.load(f)

        has_classifier = data["wikibase_id"] in classifiers

        # Insert main concept data with ON CONFLICT DO NOTHING for deduplication
        con.execute(
            """
            INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                data["wikibase_id"],
                data["preferred_label"],
                data["alternative_labels"],
                data["negative_labels"],
                data["description"],
                data["definition"],
                data["labelled_passages"],
                has_classifier,
            ),
        )

    # Second pass: Insert all relationships
    missing_concepts = set()

    for file_path in json_files:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Insert sub-concept relationships
        for subconcept_id in data["subconcept_of"]:
            try:
                con.execute(
                    """
                    INSERT INTO concept_subconcept_relations (concept_id, subconcept_id)
                    VALUES (?, ?)
                """,
                    (data["wikibase_id"], subconcept_id),
                )
            except duckdb.ConstraintException as e:
                print(f"Error: {e}")
                missing_concepts.add(subconcept_id)

        # Insert related concept relationships
        for related_id in data["related_concepts"]:
            # Only insert if concept_id1 < concept_id2 to avoid duplicates
            if data["wikibase_id"] < related_id:
                try:

                    con.execute(
                        """
                        INSERT INTO concept_related_relations (concept_id1, concept_id2)
                        VALUES (?, ?)
                    """,
                        (data["wikibase_id"], related_id),
                    )
                except duckdb.ConstraintException as e:
                    print(f"Error: {e}")
                    missing_concepts.add(related_id)

    if missing_concepts:
        print(
            "Done. Found {} missing concept IDs:\n- {}".format(
                len(missing_concepts), "\n- ".join(sorted(missing_concepts))
            )
        )
    print("Finished creating DuckDB database")


if __name__ == "__main__":
    main()
