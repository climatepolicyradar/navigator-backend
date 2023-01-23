import requests
from cloudpathlib import S3Path

from tests.integration.bulk_import.config import PIPELINE_BUCKET, API_HOST_LOCAL

# TODO should these be a pytest fixture
EXPECTED_S3_FILES = [
    "embeddings_input/CCLW.executive.10992.6270.json",
    "embeddings_input/CCLW.executive.10992.6271.json",
    "embeddings_input/CCLW.executive.10993.6272.json",
    "embeddings_input/CCLW.executive.10994.6273.json",
    "embeddings_input/CCLW.executive.10995.6274.json",
    "indexer_input/CCLW.executive.10992.6270.json",
    "indexer_input/CCLW.executive.10992.6271.json",
    "indexer_input/CCLW.executive.10993.6272.json",
    "indexer_input/CCLW.executive.10994.6273.json",
    "indexer_input/CCLW.executive.10995.6274.json",
    "parser_input/CCLW.executive.10992.6270.json",
    "parser_input/CCLW.executive.10992.6271.json",
    "parser_input/CCLW.executive.10993.6272.json",
    "parser_input/CCLW.executive.10994.6273.json",
    "parser_input/CCLW.executive.10995.6274.json",
]

EXPECTED_RDS_DOCUMENTS = [
    {
        "document_id": 4,
        "import_id": "CCLW.executive.10992.6271",
        "slug": "equatorial-guinea_2021_information-on-decree-69-2021-approving-the-national"
        "-strategy-for-sustainable-development-agenda-guinea-ecuatorial-2035_10992_6271",
        "name": 'Information on Decree 69/2021 approving the National Strategy for Sustainable Development "Agenda '
        'Guinea Ecuatorial 2035"',
        "postfix": "",
        "description": "This decree approves the National Strategy for Sustainable Development. The strategy contains "
        "the final act of the Third National Economic Conference; the PNDES 2020; the Equatorial "
        "Guinea Agenda 2035; the Strategic Plan for Sustainable Development 2020-2025; and the "
        "Supporting Programme to Economic Diversification (PRODECO).",
        "country_code": "GNQ",
        "country_name": "Equatorial Guinea",
        "publication_ts": "2021-01-01T00:00:00",
    },
    {
        "document_id": 5,
        "import_id": "CCLW.executive.10992.6270",
        "slug": "equatorial-guinea_2021_decree-69-2021-approving-the-national-strategy-for-sustainable-development"
        "-agenda-guinea-ecuatorial-2035_10992_6270",
        "name": 'Decree 69/2021 approving the National Strategy for Sustainable Development "Agenda Guinea '
        'Ecuatorial 2035"',
        "postfix": "",
        "description": "This decree approves the National Strategy for Sustainable Development. The strategy contains "
        "the final act of the Third National Economic Conference; the PNDES 2020; the Equatorial "
        "Guinea Agenda 2035; the Strategic Plan for Sustainable Development 2020-2025; and the "
        "Supporting Programme to Economic Diversification (PRODECO).",
        "country_code": "GNQ",
        "country_name": "Equatorial Guinea",
        "publication_ts": "2021-01-01T00:00:00",
    },
    {
        "document_id": 1,
        "import_id": "CCLW.executive.10995.6274",
        "slug": "equatorial-guinea_2020_national-redd-investment-plan-pni-redd_10995_6274",
        "name": "DOCUMENT EDIT National REDD+ Investment Plan (PNI-REDD+)",
        "postfix": "",
        "description": "The National REDD+ Investment Plan (PNI-REDD+) presents the EN-REDD+ implementation "
        "priorities for the next ten years (2020-2030). The PNI-REDD+ establishes orderly and "
        "sustainable economic growth as a principle, which safeguards the country's valuable natural "
        "capital, fosters participation and social inclusion, and improves the living conditions of "
        "the population.The PNI-REDD+ proposes to make the vision of REDD+ a reality through two "
        "impacts that combine environmental and socioeconomic benefits:• reduction of country "
        "emissions from agriculture, forestry and other land uses;• Improvement of the living "
        "conditions of the population thanks to economic diversification, with a sustainable approach "
        "and integrated management of the territory.Specifically, the PNI-REDD+ aspires to reduce the "
        "country's emissions linked to agriculture, forestry and other land uses by 40 million tCO2eq "
        "in the year 2040, including the implementation periods (2020-2030) and capitalization (2030- "
        "2040). Achieving such an impact requires a total budget of US$185 million.",
        "country_code": "GNQ",
        "country_name": "Equatorial Guinea",
        "publication_ts": "2020-01-01T00:00:00",
    },
    {
        "document_id": 3,
        "import_id": "CCLW.executive.10993.6272",
        "slug": "equatorial-guinea_2019_national-strategy-redd-for-equatorial-guinea_10993_6272",
        "name": "National Strategy REDD+ for Equatorial Guinea",
        "postfix": "",
        "description": 'EN-REDD+ has as a long-term objective "to contribute to the global fight against climate '
        "change and to the country's sectoral development to achieve the well-being of the Equatorial "
        "Guinean people through REDD+, with an approach based on competitiveness, sustainability, "
        "integrated land management, food security, and social and gender equity”. EN-REDD+ will "
        "contributefor the country to achieve the climate objectives stipulated in its planned "
        "contributions determined at the national level (CPDN) and the development objectives "
        "established in the National Economic and Social Development Plan (PNDES) Horizon 2020 (RGE, "
        "2007).The EN-REDD+ sets ambitious goals to meet its objectives: 1) reduce GHG emissions "
        "linked to agriculture, forestry and other land uses (AFOLU) by 20% by 2030, and in 50% by the "
        "year 2050; 2) maintain the forest area around 93% of the national territory; 3) reduce the "
        "rate of forest degradation to 0.45% per year; 4) strengthen the National System of Protected "
        "Areas (SNAP); 5) increase the area of productive forests with sustainable management plans up "
        "to 80% by the year 2030; 6) achieve sustainability and improve the efficiency of the forestry "
        "and agricultural sectors; and 7) mitigate and compensate the possible negative consequences "
        "for forests of future productive activities.REDD+ is part of the national structure of fight "
        "against climate change, which includes both adaptation to climate change and mitigation of "
        "its effects. The implementation of the REDD+ process will be guided by the principles of good "
        "governance: accountability, effectiveness, efficiency, equity, participation and "
        "transparency, as well as the application of a multisectoral and territorial development "
        "approach.Said implementation will be the responsibility of a REDD+ Steering Committee ("
        "CP-REDD+) and a REDD+ National Coordination (CN-REDD+), which will act as the entity "
        "responsible for governance and as the technical executive body, respectively.To achieve its "
        "objectives, the country plans to develop policies and measures for REDD+ structured in eight "
        "strategic axes. Four of them are sectoral axes, whose objective is to achieve sustainable "
        "productive development, which in turn makes it possible to mitigate the direct causes of "
        "deforestation and forest degradation, and four are cross-cutting axes, which will address the "
        "underlying causes.",
        "country_code": "GNQ",
        "country_name": "Equatorial Guinea",
        "publication_ts": "2019-01-01T00:00:00",
    },
    {
        "document_id": 2,
        "import_id": "CCLW.executive.10994.6273",
        "slug": "equatorial-guinea_2018_action-plan-for-the-development-of-renewable-energies-in-equatorial-guinea"
        "-2018-2025-paer_10994_6273",
        "name": "Action Plan for the development of Renewable Energies in Equatorial Guinea 2018 – 2025 (PAER)",
        "postfix": "",
        "description": "The National Action Plan for the Development of Renewable Energies in Equatorial Guinea "
        "establishes the following general objectives:1. Promote the study, research, "
        "use and comprehensive development of electricity generation with renewable sources in the "
        "country, through a model of sustainable energy development, which ensures a positive "
        "contribution from the environment, and with significant impacts on the economy and the "
        "backbone social of the territories.2. Reduce dependence on electricity generation through "
        "fossil fuels, implementing generation projects with renewable sources, achieving climate "
        "change mitigation, and also contributing a differential value to energy security and the "
        "sustainability of the country's electrical system.",
        "country_code": "GNQ",
        "country_name": "Equatorial Guinea",
        "publication_ts": "2018-01-01T00:00:00",
    },
]


def test_final_bulk_import_state_s3():
    """
    Assert that the final state of the test infrastructure is correct post the bulk import with updates.

    - The relevant json objects should be in the s3 bucket with correct object keys.
    """
    s3_files = list(S3Path(f"s3://{PIPELINE_BUCKET}").glob("*/*.json"))

    assert s3_files == EXPECTED_S3_FILES


def test_final_bulk_import_state_rds():
    """
    Assert that the final state of the test infrastructure is correct post the bulk import with updates.

    - The relevant rows should be in the rds database.
    """

    # TODO send a request to the backend and assert the response is correct
    rds_documents = requests.get(f"http://{API_HOST_LOCAL}/api/v1/documents")

    assert rds_documents == EXPECTED_RDS_DOCUMENTS
