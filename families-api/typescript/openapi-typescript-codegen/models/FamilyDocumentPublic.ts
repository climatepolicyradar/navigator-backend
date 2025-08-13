/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { FamilyEventPublic } from "./FamilyEventPublic";
export type FamilyDocumentPublic = {
  import_id: string;
  slug: string;
  title: string;
  cdn_object: string;
  variant: string | null;
  md5_sum: string | null;
  source_url: string | null;
  content_type: string | null;
  language: string | null;
  languages: Array<string>;
  document_type: string | null;
  document_role: string | null;
  events: Array<FamilyEventPublic>;
};
