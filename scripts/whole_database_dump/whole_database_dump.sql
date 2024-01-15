WITH 
deduplicated_family_slugs as (
	select
	  distinct ON (slug.family_import_id) slug.family_import_id, slug.created, slug.name
	from (
	  SELECT
		slug.family_import_id as "family_import_id",
		count(*) as count
		from slug
		where slug.family_import_id is not null
		group by slug.family_import_id
		having count(*) > 1
	) duplicates
	left join slug
	  on duplicates.family_import_id = slug.family_import_id
	order by slug.family_import_id desc, slug.created desc, slug.ctid desc
),
unique_family_slugs as (
	select
	  distinct ON (slug.family_import_id) slug.family_import_id, slug.created, slug.name
	from (
	  SELECT
		slug.family_import_id as "family_import_id",
		count(*) as count
		from slug
		where slug.family_import_id is not null
		group by slug.family_import_id
		having count(*) = 1
	) non_duplicates
	left join slug
	  on non_duplicates.family_import_id = slug.family_import_id
	order by slug.family_import_id desc, slug.created desc, slug.ctid desc
), most_recent_family_slugs as (
	select 
		deduplicated_family_slugs.family_import_id as "family_import_id",
		deduplicated_family_slugs.created as "created",
		deduplicated_family_slugs.name as "name"
	from deduplicated_family_slugs
	UNION ALL
	select 
		unique_family_slugs.family_import_id as "family_import_id",
		unique_family_slugs.created as "created",
		unique_family_slugs.name as "name"
	from unique_family_slugs
	order by family_import_id desc, created desc
), deduplicated_doc_slugs as (
	select
	  distinct ON (slug.family_document_import_id) slug.family_document_import_id, slug.created, slug.name
	from (
	  SELECT
		slug.family_document_import_id as "family_document_import_id",
		count(*) as count
		from slug
		where slug.family_document_import_id is not null
		group by slug.family_document_import_id
		having count(*) > 1
	) duplicates
	left join slug
	  on duplicates.family_document_import_id = slug.family_document_import_id
	order by slug.family_document_import_id desc, slug.created desc, slug.ctid desc
),
unique_doc_slugs as (
	select
	  distinct ON (slug.family_document_import_id) slug.family_document_import_id, slug.created, slug.name
	from (
	  SELECT
		slug.family_document_import_id as "family_document_import_id",
		count(*) as count
		from slug
		where slug.family_document_import_id is not null
		group by slug.family_document_import_id
		having count(*) = 1
	) non_duplicates
	left join slug
	  on non_duplicates.family_document_import_id = slug.family_document_import_id
	order by slug.family_document_import_id desc, slug.created desc, slug.ctid desc
), most_recent_doc_slugs as (
	select 
		deduplicated_doc_slugs.family_document_import_id as "family_document_import_id",
		deduplicated_doc_slugs.created,
		deduplicated_doc_slugs.name
	from deduplicated_doc_slugs
	UNION ALL
	select 
		unique_doc_slugs.family_document_import_id as "family_document_import_id",
		unique_doc_slugs.created,
		unique_doc_slugs.name
	from unique_doc_slugs
	order by family_document_import_id desc, created desc
), event_dates as (
select 
    family_event.family_import_id as family_import_id,
	min(case 
			when family_event.event_type_name='Passed/Approved' then family_event.date::date
			else family_event.date::date
	end) published_date,
	max(family_event.date::date) last_changed
from family_event 
group by family_import_id
)

SELECT
	n1.collection_import_ids as "Collection ID(s)",
	n1.collection_titles as "Collection Title(s)",
	n1.collection_descriptions as "Collection Description(s)",
	f.import_id as "Family ID",
	f.title as "Family Title",
	f.description as "Family Summary",
	fs.name as "Family Slug",
	d.import_id as "Document ID",
	p.title as "Document Title",
	INITCAP(d.document_role::TEXT) as "Document Role",
	d.variant_name as "Document Variant",
	ds.name as "Document Slug",
	p.source_url as "Document Content URL",
	d.document_type as "Document Type",
	CASE
	   WHEN f.family_category = 'UNFCCC' THEN 'UNFCCC'
	   ELSE INITCAP(f.family_category::TEXT)
   	END "Document Category",
	n2.language as "Language",
	o.name as "Source",
	g.value as "Geography ISO",
	g.display_value as "Geography",
	array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'topic')), ';') as "Topic/Response",
	array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'hazard')), ';') as "Hazard",
	array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'sector')), ';') as "Sector",
	array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'keyword')), ';') as "Keyword",
	array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'framework')), ';') as "Framework",
	array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'instrument')), ';') as "Instrument",
	array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'author')), ';') as "Author",
	array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'author_type')), ';') as "Author Type",
	fp.published_date as "First event in timeline",
	fp.last_changed as "Last event in timeline",
	n3.event_type_names as "Full timeline of events (types)",
	n3.event_dates as "Full timeline of events (dates)",
	d.created::date as "Date Added to System",
	f.last_modified::date as " Last Modified on System"
FROM physical_document p 
JOIN family_document d
	ON p.id = d.physical_document_id
JOIN family f
	ON d.family_import_id = f.import_id
inner join geography g
	on g.id = f.geography_id
join family_organisation fo
	on fo.family_import_id = f.import_id
join organisation o
	on o.id = fo.organisation_id
join family_metadata fm
	on fm.family_import_id = f.import_id


FULL JOIN (
	SELECT
		collection_family.family_import_id as "family_import_id",
		string_agg(collection.import_id, ';') AS collection_import_ids,
		string_agg(collection.title, ';') AS collection_titles,
		string_agg(collection.description, ';') AS collection_descriptions
	FROM
		collection
	INNEr JOIN  collection_family ON collection_family.collection_import_id = collection.import_id
	GROUP  BY collection_family.family_import_id
) n1 ON n1.family_import_id=f.import_id

	
left JOIN (
	SELECT 
		p.id as "id",
		string_agg(l.name, ';' ORDER BY l.name) AS "language"
	FROM   physical_document p
	left join physical_document_language pdl
		on pdl.document_id = p.id
	left join language l
		on l.id = pdl.language_id
	GROUP  BY p.id
) n2 ON n2.id=d.physical_document_id


FULL JOIN (
	SELECT
		family_event.family_import_id,
		string_agg(family_event.import_id, ';') AS event_import_ids,
		string_agg(family_event.title, ';') AS event_titles,
		string_agg(family_event.event_type_name, ';') AS event_type_names,
		string_agg(family_event.date::date::text, ';') AS event_dates
	FROM
		family_event
	INNEr JOIN  family ON family.import_id = family_event.family_import_id
	GROUP  BY family_event.family_import_id
) n3 ON n3.family_import_id=f.import_id


LEFT JOIN most_recent_doc_slugs ds
	on ds.family_document_import_id = d.import_id
LEFT JOIN most_recent_family_slugs fs
	on fs.family_import_id = f.import_id
	
LEFT JOIN event_dates fp
	on fp.family_import_id = f.import_id

ORDER BY d.created desc, n1.family_import_id
