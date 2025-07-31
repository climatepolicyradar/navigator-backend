/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { APIItemResponse_FamilyPublic_ } from "../models/APIItemResponse_FamilyPublic_";
import type { APIListResponse_GeographyDocumentCount_ } from "../models/APIListResponse_GeographyDocumentCount_";
import type { APIListResponse_PhysicalDocumentPublic_ } from "../models/APIListResponse_PhysicalDocumentPublic_";
import type { CancelablePromise } from "../core/CancelablePromise";
import { OpenAPI } from "../core/OpenAPI";
import { request as __request } from "../core/request";
export class DefaultService {
  /**
   * Health Check
   * @returns any Successful Response
   * @throws ApiError
   */
  public static healthCheckHealthGet(): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/health",
    });
  }
  /**
   * Read Documents
   * @returns APIListResponse_PhysicalDocumentPublic_ Successful Response
   * @throws ApiError
   */
  public static readDocumentsFamiliesGet(): CancelablePromise<APIListResponse_PhysicalDocumentPublic_> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/families/",
    });
  }
  /**
   * Read Concepts
   * @returns any Successful Response
   * @throws ApiError
   */
  public static readConceptsFamiliesConceptsGet(): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/families/concepts",
    });
  }
  /**
   * Read Family
   * @param familyId
   * @returns APIItemResponse_FamilyPublic_ Successful Response
   * @throws ApiError
   */
  public static readFamilyFamiliesFamilyIdGet(
    familyId: string,
  ): CancelablePromise<APIItemResponse_FamilyPublic_> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/families/{family_id}",
      path: {
        family_id: familyId,
      },
      errors: {
        422: `Validation Error`,
      },
    });
  }
  /**
   * Docs By Geo
   * @returns APIListResponse_GeographyDocumentCount_ Successful Response
   * @throws ApiError
   */
  public static docsByGeoFamiliesAggregationsByGeographyGet(): CancelablePromise<APIListResponse_GeographyDocumentCount_> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/families/aggregations/by-geography",
    });
  }
  /**
   * Health Check
   * @returns any Successful Response
   * @throws ApiError
   */
  public static healthCheckFamiliesHealthGet(): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/families/health",
    });
  }
}
