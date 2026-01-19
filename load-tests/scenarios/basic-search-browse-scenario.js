import { search_pootle } from "../scripts/search-pootle.js";

const queries = JSON.parse(open("../data/static.json")).data.queries;

export function basic_search_browse_scenario() {
  let base_url = globalThis.base_url;
  let api_url = globalThis.api_url;

  search_pootle(queries, base_url, api_url);
}
