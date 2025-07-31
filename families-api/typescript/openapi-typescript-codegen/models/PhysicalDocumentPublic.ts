/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { FamilyDocumentPublic } from "./FamilyDocumentPublic";
export type PhysicalDocumentPublic = {
  id: number;
  title: string;
  md5_sum: string | null;
  source_url: string | null;
  content_type: string | null;
  cdn_object: string | null;
  family_document: FamilyDocumentPublic | null;
};
