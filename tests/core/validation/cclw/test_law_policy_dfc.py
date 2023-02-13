from app.core.import_row import (
    ID,
    DOCUMENT_ID,
    CCLW_DESCRIPTION,
    PART_OF_COLLECTION,
    CREATE_NEW_FAMILYIES,
    COLLECTION_ID,
    COLLECTION_NAME,
    COLLECTION_SUMMARY,
    DOCUMENT_TITLE,
    FAMILY_NAME,
    FAMILY_SUMMARY,
    FAMILY_ID,
    DOCUMENT_ROLE,
    APPLIES_TO_ID,
    GEOG_ISO,
    DOCUMENTS,
    CATEGORY,
    EVENTS,
    SECTORS,
    INSTRUMENTS,
    FRAMEWORKS,
    RESPONSES,
    NATURAL_HAZARDS,
    DOCUMENT_TYPE,
    YEAR,
    LANGUAGE,
    KEYWORDS,
    GEOG,
    PARENT_LEGISLATION,
    COMMENT,
    CPR_DOCUMENT_ID,
    CPR_FAMILY_ID,
    CPR_COLLECTION_IF,
    CPR_FAMILY_SLUG,
    CPR_DOCUMENT_SLUG,
    CPR_DOCUMENT_STATUS,
)


CCLW_FIELDNAMES = [
    ID,
    DOCUMENT_ID,
    CCLW_DESCRIPTION,
    PART_OF_COLLECTION,
    CREATE_NEW_FAMILYIES,
    COLLECTION_ID,
    COLLECTION_NAME,
    COLLECTION_SUMMARY,
    DOCUMENT_TITLE,
    FAMILY_NAME,
    FAMILY_SUMMARY,
    FAMILY_ID,
    DOCUMENT_ROLE,
    APPLIES_TO_ID,
    GEOG_ISO,
    DOCUMENTS,
    CATEGORY,
    EVENTS,
    SECTORS,
    INSTRUMENTS,
    FRAMEWORKS,
    RESPONSES,
    NATURAL_HAZARDS,
    DOCUMENT_TYPE,
    YEAR,
    LANGUAGE,
    KEYWORDS,
    GEOG,
    PARENT_LEGISLATION,
    COMMENT,
    CPR_DOCUMENT_ID,
    CPR_FAMILY_ID,
    CPR_COLLECTION_IF,
    CPR_FAMILY_SLUG,
    CPR_DOCUMENT_SLUG,
    CPR_DOCUMENT_STATUS,
]


INVALID_CSV_MISSING_HEADERS = (
    f"{ID},{DOCUMENT_ID},{CCLW_DESCRIPTION},{PART_OF_COLLECTION},"
    f"{CREATE_NEW_FAMILYIES},{COLLECTION_ID},{COLLECTION_NAME},"
    f"{COLLECTION_SUMMARY},{DOCUMENT_TITLE},{FAMILY_NAME},"
    f"{FAMILY_SUMMARY},{FAMILY_ID},{DOCUMENT_ROLE},"
    f"{APPLIES_TO_ID},{GEOG_ISO},{DOCUMENTS},"
    f"{CATEGORY},{EVENTS},{SECTORS},"
    f"{INSTRUMENTS},{FRAMEWORKS},{RESPONSES},"
    f"{NATURAL_HAZARDS},{DOCUMENT_TYPE},{YEAR},"
    f"{LANGUAGE},{KEYWORDS},{GEOG},"
    f"{PARENT_LEGISLATION},{COMMENT},{CPR_DOCUMENT_ID},"
    f"{CPR_FAMILY_ID},{CPR_COLLECTION_IF},{CPR_FAMILY_SLUG},"
    f"{CPR_DOCUMENT_SLUG}\n"
)
MISSING_HEADERS = [CPR_DOCUMENT_STATUS]
INVALID_CSV_EXTRA_HEADERS = (
    f"{ID},{DOCUMENT_ID},{CCLW_DESCRIPTION},{PART_OF_COLLECTION},"
    f"{CREATE_NEW_FAMILYIES},{COLLECTION_ID},{COLLECTION_NAME},"
    f"{COLLECTION_SUMMARY},{DOCUMENT_TITLE},{FAMILY_NAME},"
    f"{FAMILY_SUMMARY},{FAMILY_ID},{DOCUMENT_ROLE},"
    f"{APPLIES_TO_ID},{GEOG_ISO},{DOCUMENTS},"
    f"{CATEGORY},{EVENTS},{SECTORS},"
    f"{INSTRUMENTS},{FRAMEWORKS},{RESPONSES},"
    f"{NATURAL_HAZARDS},{DOCUMENT_TYPE},{YEAR},"
    f"{LANGUAGE},{KEYWORDS},{GEOG},"
    f"{PARENT_LEGISLATION},{COMMENT},{CPR_DOCUMENT_ID},"
    f"{CPR_FAMILY_ID},{CPR_COLLECTION_IF},{CPR_FAMILY_SLUG},"
    f"{CPR_DOCUMENT_SLUG},extra,{CPR_DOCUMENT_STATUS}\n"
)
EXTRA_HEADERS = ["extra"]
INVALID_CSV_MIXED_ERRORS = (
    f"{ID},{DOCUMENT_ID},{CCLW_DESCRIPTION},{PART_OF_COLLECTION},"
    f"{CREATE_NEW_FAMILYIES},{COLLECTION_ID},{COLLECTION_NAME},"
    f"{COLLECTION_SUMMARY},{DOCUMENT_TITLE},{FAMILY_NAME},"
    f"{FAMILY_SUMMARY},{FAMILY_ID},{DOCUMENT_ROLE},"
    f"{APPLIES_TO_ID},{GEOG_ISO},{DOCUMENTS},"
    f"{CATEGORY},{EVENTS},{SECTORS},"
    f"{INSTRUMENTS},{FRAMEWORKS},{RESPONSES},"
    f"{NATURAL_HAZARDS},{DOCUMENT_TYPE},{YEAR},"
    f"{LANGUAGE},{KEYWORDS},{GEOG},"
    f"{PARENT_LEGISLATION},{COMMENT},{CPR_DOCUMENT_ID},"
    f"{CPR_FAMILY_ID},{CPR_COLLECTION_IF},{CPR_FAMILY_SLUG},"
    f"{CPR_DOCUMENT_SLUG},extra\n"
)
VALID_CSV_HEADER = (
    f"{ID},{DOCUMENT_ID},{CCLW_DESCRIPTION},{PART_OF_COLLECTION},"
    f"{CREATE_NEW_FAMILYIES},{COLLECTION_ID},{COLLECTION_NAME},"
    f"{COLLECTION_SUMMARY},{DOCUMENT_TITLE},{FAMILY_NAME},"
    f"{FAMILY_SUMMARY},{FAMILY_ID},{DOCUMENT_ROLE},"
    f"{APPLIES_TO_ID},{GEOG_ISO},{DOCUMENTS},"
    f"{CATEGORY},{EVENTS},{SECTORS},"
    f"{INSTRUMENTS},{FRAMEWORKS},{RESPONSES},"
    f"{NATURAL_HAZARDS},{DOCUMENT_TYPE},{YEAR},"
    f"{LANGUAGE},{KEYWORDS},{GEOG},"
    f"{PARENT_LEGISLATION},{COMMENT},{CPR_DOCUMENT_ID},"
    f"{CPR_FAMILY_ID},{CPR_COLLECTION_IF},{CPR_FAMILY_SLUG},"
    f"{CPR_DOCUMENT_SLUG},{CPR_DOCUMENT_STATUS}\n"
)

INVALID_LINE_1 = (
    ",1,Example Description,FALSE,FALSE,N/A,N/A,N/A,Example doc title,Example family name,Example family summary.,,"
    "MAIN,,DZA,https://marksdummyurl/bhoyz,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Mitigation,,"
    "Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1.1,CCLW.family.1.1,N/A,"
    "executive-decree-no-2011-423-fixing-the-operating-procedures-of-the-trust-account-no-302-1221-national-fund-for"
    "-renewable-energy-and-cogeneration_39d9,"
    "executive-decree-no-2011-423-fixing-the-operating-procedures-of-the-trust-account-no-302-1221-national-fund-for"
    "-renewable-energy-and-cogeneration_56b3,Published\n"
)
# INVALID_LINE_1_ERRORS = {
#     "sectors": ["unknown_sector"],
#     "keywords": ["unknown_keyword"],
# } TODO need to build out errors (no ID present)
INVALID_FILE_1 = VALID_CSV_HEADER + INVALID_LINE_1


VALID_LINE_1 = (
    "1,1,Example Description,FALSE,FALSE,N/A,N/A,N/A,Example doc title,Example family name,Example family summary.,,"
    "MAIN,,DZA,https://marksdummyurl/bhoyz,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,Mitigation,,"
    "Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1.1,CCLW.family.1.1,N/A,"
    "executive-decree-no-2011-423-fixing-the-operating-procedures-of-the-trust-account-no-302-1221-national-fund-for"
    "-renewable-energy-and-cogeneration_39d9,"
    "executive-decree-no-2011-423-fixing-the-operating-procedures-of-the-trust-account-no-302-1221-national-fund-for"
    "-renewable-energy-and-cogeneration_56b3,Deleted\n"
)
VALID_LINE_2 = (
    "1,2,Example Description 2,FALSE,FALSE,N/A,N/A,N/A,Example doc title 2,Example family name 2,Example family "
    "summary 2.,,MAIN,,DZA,https://marksdummyurl/bhoyz,executive,08/12/2011|Law passed,Energy,Subsidies|Economic,,"
    "Mitigation,,Decree,,,Research And Development;Energy Supply,Algeria,,,CCLW.executive.1.2,CCLW.family.1.2,N/A,"
    "executive-decree-no-2011-423-fixing-the-operating-procedures-of-the-trust-account-no-302-1221-national-fund-for"
    "-renewable-energy-and-cogeneration_39d9,"
    "executive-decree-no-2011-423-fixing-the-operating-procedures-of-the-trust-account-no-302-1221-national-fund-for"
    "-renewable-energy-and-cogeneration_56b3,Published\n"
)
DFC_VALID_FILE_1 = VALID_CSV_HEADER + VALID_LINE_1 + VALID_LINE_2


# TODO update the validation function to provide the correct error messages

# @pytest.mark.parametrize(
#     "csv_header_line,expected_missing,expected_extra",
#     [
#         (INVALID_CSV_MISSING_HEADERS, MISSING_HEADERS, []),
#         (INVALID_CSV_EXTRA_HEADERS, [], EXTRA_HEADERS),
#         (INVALID_CSV_MIXED_ERRORS, MISSING_HEADERS, EXTRA_HEADERS),
#     ],
# )
# def test_validated_input__invalid(csv_header_line, expected_missing, expected_extra):
#     with pytest.raises(ImportSchemaMismatchError) as e:
#         csv_file = TextIOWrapper(BytesIO(csv_header_line.encode("utf8")))
#         validated_input(csv_file)
#
#     assert e.value.details == {
#         "unexpected_fields": expected_extra,
#         "missing_fields": expected_missing,
#     }


# def test_validated_input__valid():
#     csv_file = TextIOWrapper(BytesIO(VALID_CSV_HEADER.encode("utf8")))
#     validated_csv_file = validated_input(csv_file)
#     assert isinstance(validated_csv_file, DictReader)
#     assert validated_csv_file.fieldnames is not None
#     assert set(validated_csv_file.fieldnames) == set(CCLW_FIELDNAMES)


# def test_extract_documents_invalid(test_db):
#     # ensure metadata exists
#     test_db.add(Source(name="CCLW"))
#     test_db.add(
#         Geography(
#             display_value="geography", slug="geography", value="GEO", type="country"
#         )
#     )
#     test_db.add(DocumentType(name="doctype", description="doctype"))
#     test_db.add(Language(language_code="LAN", name="language"))
#     test_db.add(Category(name="executive", description="executive"))
#     test_db.add(Keyword(name="keyword", description="keyword"))
#     test_db.add(Hazard(name="hazard", description="hazard"))
#     test_db.add(Response(name="topic", description="topic"))
#     test_db.add(Framework(name="framework", description="framework"))
#
#     test_db.commit()
#
#     test_db.add(Instrument(name="instrument", description="instrument", source_id=1))
#     test_db.add(Sector(name="sector", description="sector", source_id=1))
#
#     test_db.commit()
#
#     csv_file = TextIOWrapper(BytesIO(INVALID_FILE_1.encode("utf8")))
#     validated_files = list(
#         extract_documents(
#             csv_reader=validated_input(csv_file),
#             valid_metadata=get_valid_metadata(test_db),
#         )
#     )
#     assert len(validated_files) == 1
#     assert validated_files[0].errors == INVALID_LINE_1_ERRORS


# def test_extract_documents_valid(test_db):
#     # ensure metadata exists
#     test_db.add(Source(name="CCLW"))
#     test_db.add(
#         Geography(
#             display_value="geography", slug="geography", value="GEO", type="country"
#         )
#     )
#     test_db.add(DocumentType(name="doctype", description="doctype"))
#     test_db.add(Language(language_code="LAN", name="language"))
#     test_db.add(Category(name="executive", description="executive"))
#     test_db.add(Keyword(name="keyword1", description="keyword1"))
#     test_db.add(Keyword(name="keyword2", description="keyword2"))
#     test_db.add(Hazard(name="hazard1", description="hazard1"))
#     test_db.add(Hazard(name="hazard2", description="hazard2"))
#     test_db.add(Response(name="topic", description="topic"))
#     test_db.add(Framework(name="framework", description="framework"))
#
#     test_db.commit()
#
#     test_db.add(Instrument(name="instrument", description="instrument", source_id=1))
#     test_db.add(Sector(name="sector", description="sector", source_id=1))
#
#     test_db.commit()
#
#     csv_file = TextIOWrapper(BytesIO(VALID_FILE_1.encode("utf8")))
#     validated_files = list(
#         extract_documents(
#             csv_reader=validated_input(csv_file),
#             valid_metadata=get_valid_metadata(test_db),
#         )
#     )
#     assert len(validated_files) == 2
#     assert validated_files[0].create_request.postfix == "pf1"
#     assert validated_files[0].create_request.source_url == "https://dave"
#     assert validated_files[0].create_request.keywords == ["keyword1"]
#     assert validated_files[0].create_request.hazards == ["hazard1"]
#     assert validated_files[1].create_request.postfix == "pf2"
#     assert validated_files[1].create_request.source_url == "https://steve"
#     assert validated_files[1].create_request.keywords == ["keyword1", "keyword2"]
#     assert validated_files[1].create_request.hazards == ["hazard1", "hazard2"]
