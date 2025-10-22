# Search API Specification

> **_NOTE:_** Last updated October 2023

---

## **VERSION HISTORY**

|     |                                                                                                                             |           |              |
| --- | --------------------------------------------------------------------------------------------------------------------------- | --------- | ------------ |
| 1.0 | Initial version based on [v1.3.15-beta.](https://github.com/climatepolicyradar/navigator-backend/releases/tag/v1.3.15-beta) | 3/10/2023 | Peter Hooper |
|     |                                                                                                                             |           |              |

## **PURPOSE**

This document is intended to explain the use of our search API for external
developers and integrators.

## **BACKGROUND**

The API  is a typical REST API where the requests and responses are encoded as
`application/json`

## **SEARCH ENDPOINT**

|          |                  |
| :------: | ---------------- |
| **POST** | /api/v1/searches |

There is **_no_** authentication required for using this interface.

❗ We ask that users be respectful of its use and remind users that data is
available to download on request.

The search endpoint behaves in two distinct ways:

1. In “Browse” mode - this is when an empty`query_string`is provided. This mode
   does not use Vespa, rather queries the structured data (postgresql) directly,
   using the other supplied filter fields.
2. In “Search” mode - when a `query_string`is provided. A query is constructed
   and sent to Vespa and the response is augmented with the structured data
   before being returned in the same response scheme.

### **Request Payload**

The payload is a JSON object representing the search to be performed. This can
be seen in
[code here](https://github.com/climatepolicyradar/navigator-backend/blob/ddebbd17f6b62cf7909e6e4c575285b8b00a41b2/app/api/api_v1/schemas/search.py#L62)
and is described in further detail below.

```json
{
  "query_string": "string",
  "exact_match": false,
  "max_passages_per_doc": 10,
  "keyword_filters": {
    "additionalProp1": ["string"],
    "additionalProp2": ["string"],
    "additionalProp3": ["string"]
  },
  "year_range": ["string", "string"],
  "sort_field": "date",
  "sort_order": "desc",
  "limit": 10,
  "offset": 0
}
```

#### Properties of Request Payload

##### query_string

A string representation of the search to be performed, for example, “Adaptation
strategy”

##### exact_match

Boolean value to indicate if the `query_string`should be treated as an exact
match when the search is performed.

##### max_passages_per_doc (optional, default is 10)

The maximum number of matched passages to be returned for a single document.

##### keyword_filters (optional)

This is an object containing a map of fields and their values to filter on. The
allowed fields can be found in
[code here](https://github.com/climatepolicyradar/navigator-backend/blob/ddebbd17f6b62cf7909e6e4c575285b8b00a41b2/app/api/api_v1/schemas/search.py#L34).

##### year_range

This is an array containing exactly two values, which can be null or an integer
representing the years to search between. Examples:

`[2000, 2023]` - Would search between 2000 and 2023 inclusive.

`[null, 2023]` - Would search from 1947 up to 2023 inclusive.

`[2000, null]` - Would search from 2000 to the current date.

`[null, null]` - Does not filter by date.

Further information and understanding can be found by
[reading the tests here](https://github.com/climatepolicyradar/navigator-backend/blob/ddebbd17f6b62cf7909e6e4c575285b8b00a41b2/tests/routes/test_search.py#L634).

##### sort_field (optional) & sort_order (optional, defaults to descending)

The field to sort by can be chosen from “date” or “title”
[see related code](https://github.com/climatepolicyradar/navigator-backend/blob/ddebbd17f6b62cf7909e6e4c575285b8b00a41b2/app/api/api_v1/schemas/search.py#L20).

The order can be chosen from ascending (use “asc”) or descending (use “desc”),
[see related code.](https://github.com/climatepolicyradar/navigator-backend/blob/ddebbd17f6b62cf7909e6e4c575285b8b00a41b2/app/api/api_v1/schemas/search.py#L13)

##### limit & offset

These values control pagination, allowing a front end application to page
through the results. The `limit` refers to the maximum number of results to
return and `offset` where to start returning the results from that were
retrieved via the backend.

### **Response Payload**

The response returns a list of families and includes their associated documents
along with their passage matches. The payload has the following scheme:

```json
{
  "hits": 0,
  "query_time_ms": 0,
  "total_time_ms": 0,
  "families": [ <see family below> ]
}
```

#### Properties of Response Payload

##### hits

The total number of families that meet the search criteria.

##### query_time_ms

The time Vespa spent performing the query.

##### total_time_ms

The total time spent in getting the response.

##### families

A list of family objects, each following the scheme below:

```json
{
  "family_slug": "string",
  "family_name": "string",
  "family_description": "string",
  "family_category": "string",
  "family_date": "string",
  "family_last_updated_date": "string",
  "family_source": "string",
  "family_geographies": "string[]",
  "family_metadata": {},
  "family_title_match": true,
  "family_description_match": true,
  "family_documents": [ < see family document below > ]
}
```

##### family_slug

This slug forms part of the URL to navigate to the family. For example, with a
slug of  `climate-change-adaptation-strategy_1882`, a URL can be created to this
family of documents as:

[`https://app.climatepolicyradar.org/document/climate-change-adaptation-strategy_1882`](https://app.climatepolicyradar.org/document/climate-change-adaptation-strategy_1882)

##### family_name

The name of the family.

##### family_description

The description of the family.

##### family_category

The family category, for example: Executive (see list in
[code here](https://github.com/climatepolicyradar/navigator-backend/blob/1529e0ff85b73a8e52a94e7eb510e3882307e64e/app/db/models/law_policy/family.py#L15))

##### family_date

The date the family of documents was published, this date is found by looking
for the date associated with the datetime_event_name value from the event
taxonomy for this family (e.g., Passed/Approved, Project Approved or Concept
Approved).

##### family_last_updated_date

The date the family of documents was published, this is from the most recent
event of this family of documents.

##### family_source

The source, currently organisation name. Either “CCLW” or “UNFCCC”

##### family_geographies

The geographical location(s) of the family in
[ISO 3166-1 alpha-3](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3)

##### family_metadata

An object if metadata for the family, the schema will change given the
`family_source.`

##### family_title_match

Boolean value that is true if the search is matched within the family’s title.

##### family_description_match

Boolean value that is true if the search is matched within the family’s
description.

##### family_documents

A list of the family’s documents in the following scheme:

```json
{
  "document_title": "string",
  "document_slug": "string",
  "document_type": "string",
  "document_source_url": "string",
  "document_url": "string",
  "document_content_type": "string",
  "document_passage_matches": [
    {
      "text": "string",
      "text_block_id": "string",
      "text_block_page": 0,
      "text_block_coords": [["string", "string"]]
    }
  ]
}
```

##### document_title

The title of the document.

##### document_slug

This slug forms part of the URL to navigate to the particular document. For
example, with a slug of  \``national-climate-change-adaptation-strategy_06f8`, a
``URL can be created to the document as:

<https://app.climatepolicyradar.org/documents/national-climate-change-adaptation-strategy_06f8>

##### document_type

The type of document, for example: “Strategy”, see the
[loaded metadata here](https://github.com/climatepolicyradar/navigator-backend/blob/1529e0ff85b73a8e52a94e7eb510e3882307e64e/app/data_migrations/data/law_policy/document_type_data.json).

##### document_source_url

The source url of the external site that was used to ingest into the system.

##### document_url

The CDN url of where the document can be found within our system.

##### document_content_type

The content_type of the document found at the above URLs.
[Complete list is available at the IANA site.](https://www.iana.org/assignments/media-types/media-types.xhtml)
Most common is “`application/pdf`” and “`text/html`”.

##### document_passage_matches

This is a list of passages that match the search criteria within this document.
The length of which is affected by `max_passages_per_doc`in the request.``This
is used for passage highlighting, please contact us for further information
should you wish to use this data.

### **Examples**

The following examples are of using curl to call the API endpoint to retrieve
results via the command line.

```bash
API_HOST=https://app.climatepolicyradar.org

curl "$API_HOST/api/v1/searches" \
     -X POST \
     -H 'Accept: application/json' \
     -H 'Content-Type: application/json' \
     --data-raw '{"query_string":"", "exact_match":true, "keyword_filters":{}, "sort_field":null, "sort_order":"desc", "limit":100, "offset":0}'
```
