/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { Corpus } from "./Corpus";
import type { FamilyDocumentPublic } from "./FamilyDocumentPublic";
import type { FamilyEventPublic } from "./FamilyEventPublic";
export type FamilyPublic = {
  import_id: string;
  title: string;
  concepts: Array<Record<string, any>>;
  corpus: Corpus;
  readonly corpus_id: string;
  readonly organisation: string;
  readonly organisation_attribution_url: string | null;
  readonly summary: string;
  readonly geographies: Array<string>;
  readonly published_date: string | null;
  readonly last_updated_date: string | null;
  readonly slug: string;
  readonly category: string;
  readonly corpus_type_name: string;
  readonly collections: Array<Record<string, any>>;
  readonly events: Array<FamilyEventPublic>;
  readonly documents: Array<FamilyDocumentPublic>;
  readonly metadata: Record<string, any>;
};
