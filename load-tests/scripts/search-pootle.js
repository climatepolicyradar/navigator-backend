/**
 * This script is for load testing the CCC application.
 * It simulates user interactions with the
 * web application and API, emulating search and document page visits.
 *
 * User types:
 * - Slow: 5s wait time between actions
 * - Medium: 2s wait time between actions
 * - Fast: 1s wait time between actions
 *
 * User actions:
 * - Load the landing page
 * - Perform a search using a random query from a predefined list (probability p_search)
 * - Select a random result and load the document page (probability p_choose)
 *
 * When a probability is not met, the virtual user (VU) will end, and a
 * new VU will be created by K6 to replace it.
 *
 */
import { sleep } from "k6";
import {
  do_basic_search,
  get_with_app_token,
  get_random,
} from "../common/utils.js";

export function search_pootle(queries, base_url, api_url) {
  console.log("search_pootle", base_url, api_url);
  let user_speed = [5, 2, 1][Math.floor(Math.random() * 3)];
  let p_search = 0.9; // probability of searching
  let p_choose = 0.7; // probability of choosing a result

  let stack = ["landing_page"];

  while (stack.length > 0) {
    let page = stack.pop();
    if (page == "landing_page") {
      // Perform landing page actions
      let res = get_with_app_token(`${base_url}`);

      // Load config
      res = get_with_app_token(`${api_url}/api/v1/config`);

      sleep(user_speed);

      if (Math.random() < p_search) {
        stack.push("search_page");
      }
    } else if (page == "search_page") {
      let query = get_random(queries);
      let res = do_basic_search(query, base_url, api_url);

      sleep(user_speed);

      if (res.status == 200) {
        if (Math.random() < p_choose) {
          const rand_result = get_random(res.json()["families"]);
          const rand_doc = get_random(rand_result["family_documents"]);
          stack.push("document_page:" + rand_doc["document_slug"]);
          stack.push("family_page:" + rand_result["family_slug"]);
        }
      }
    } else if (page.startsWith("document_page")) {
      console.log("Loading document page: " + page);
      let slug = page.split(":")[1];
      let res = get_with_app_token(`${base_url}/documents/${slug}`);
      sleep(user_speed);

      // Go back to search page with probability p_search
      if (Math.random() < p_search) {
        stack.push("search_page");
      }
    } else if (page.startsWith("family_page")) {
      console.log("Loading family page: " + page);
      let slug = page.split(":")[1];
      let res = get_with_app_token(`${base_url}/document/${slug}`);
      sleep(user_speed);

      // Go back to search page with probability p_search
      if (Math.random() < p_search) {
        stack.push("search_page");
      }
    }
  }
}
