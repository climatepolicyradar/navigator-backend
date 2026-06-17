from data_in_models.models import Label

from app.extract.connectors import (
    CorpusId,
    NavigatorConnector,
    NavigatorConnectorConfig,
)
from app.extract.enums import CheckPointStorageType
from app.transform.models import NoMatchingTransformations, TransformWarning

mcf_project_corpus_import_id_to_multilateral_climate_fund: dict[CorpusId, str] = {
    "MCF.corpus.AF.n0000": "Adaptation Fund",
    "MCF.corpus.CIF.n0000": "The Climate Investment Funds",
    "MCF.corpus.GCF.n0000": "Green Climate Fund",
    "MCF.corpus.GEF.n0000": "Global Environment Facility",
}


corpus_to_provider_map: dict[CorpusId, str] = {
    "CCLW.corpus.i00000001.n0000": "Grantham Research Institute",
    "Academic.corpus.Litigation.n0000": "Sabin Center for Climate Change Law",
    "CPR.corpus.Goldstandard.n0000": "Gold Standard",
    "CPR.corpus.i00000589.n0000": "Naturebase",
    "CPR.corpus.i00000001.n0000": "NewClimate Institute",
    "CPR.corpus.i00000002.n0000": "Climate Policy Radar",
    "CPR.corpus.i00000591.n0000": "Laws Africa",
    "CPR.corpus.i00000592.n0000": "UNDRR",
    "MCF.corpus.AF.Guidance": "Adaptation Fund",
    "MCF.corpus.CIF.Guidance": "The Climate Investment Funds",
    "MCF.corpus.GCF.Guidance": "Green Climate Fund",
    "MCF.corpus.GEF.Guidance": "Global Environment Facility",
    "OEP.corpus.i00000001.n0000": "Ocean Energy Pathways",
    "UNFCCC.corpus.i00000001.n0000": "UNFCCC",
    "UN.corpus.UNCCD.n0000": "UNCCD",
    "UN.corpus.UNCBD.n0000": "UNCBD",
    "ICCN.corpus.i00000001.n0000": "International Climate Councils Network",
    **mcf_project_corpus_import_id_to_multilateral_climate_fund,
}


def create_provider_labels() -> (
    tuple[dict[str, Label], list[Exception | TransformWarning]]
):
    connector_config = NavigatorConnectorConfig(
        source_id="navigator_family",
        checkpoint_storage=CheckPointStorageType.S3,
        checkpoint_key_prefix="navigator/families/corpora",  # TODO : Implement convention for checkpoint keys APP-1409
    )

    connector = NavigatorConnector(connector_config)
    corpora_envelopes = connector.fetch_all_corpora().envelopes

    corpora_by_id = {
        corpus.import_id: corpus
        for envelope in corpora_envelopes
        for corpus in envelope.data
    }

    provider_labels = {}
    warnings = []

    fetched_corpora_ids = set(corpora_by_id.keys())
    provider_ids = set(corpus_to_provider_map.keys())
    diff = fetched_corpora_ids - provider_ids

    if diff:
        warnings.append(
            NoMatchingTransformations(f"Missing transformation for corpus: {diff}")
        )

    for corpus_import_id, provider_name in corpus_to_provider_map.items():
        if corpus_import_id in corpora_by_id:
            provider_labels[corpus_import_id] = Label(
                type="agent",
                id=f"agent::{provider_name}",
                value=provider_name,
                attributes={
                    "attribution_url": corpora_by_id[corpus_import_id].attribution_url,
                    "corpus_text": corpora_by_id[corpus_import_id].corpus_text,
                    "corpus_image_url": (
                        f"https://cdn.climatepolicyradar.org/{corpora_by_id[corpus_import_id].corpus_image_url}"
                        if corpora_by_id[corpus_import_id].corpus_image_url
                        else ""
                    ),
                },
            )

    return provider_labels, warnings
