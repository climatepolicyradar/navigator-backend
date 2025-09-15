import http from "k6/http";
import { check } from "k6";

export function get_random(list) {
  return list[Math.floor(Math.random() * list.length)];
}

export function check_response(res) {
  check(res, {
    "is status 200": (r) => r.status === 200,
    "status non-200": (r) => {
      if (r.status !== 200 && r.status !== 429) {
        console.error("Non 200 response", r.url, r.status, r.status_text);
        return false;
      }
      return true;
    },
    "rate-limited": (r) => {
      if (r.status === 429) {
        console.error("Rate limited", r.url, r.status, r.status_text);
        return false;
      }
      return true;
    },
  });
}

export function get_with_app_token(url) {
  let res = http.get(url, {
    headers: {
      "App-Token":
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhbGxvd2VkX2NvcnBvcmFfaWRzIjpbIkFjYWRlbWljLmNvcnB1cy5MaXRpZ2F0aW9uLm4wMDAwIl0sInN1YiI6IkNDQyIsImF1ZCI6ImNjYy5zdGFnaW5nLmNsaW1hdGVwb2xpY3lyYWRhci5vcmciLCJpc3MiOiJDbGltYXRlIFBvbGljeSBSYWRhciIsImV4cCI6MjA2NDU3MzY4OC4wLCJpYXQiOjE3NDkwNDA4ODh9.3HILyhmw3b91IUATQ9Irz15SYJbuNwgyy4V77W9ETRk",
    },
  });
  check_response(res);
  return res;
}

export function post_with_app_token(url, data) {
  let res = http.post(url, JSON.stringify(data), {
    headers: {
      "App-Token":
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhbGxvd2VkX2NvcnBvcmFfaWRzIjpbIkFjYWRlbWljLmNvcnB1cy5MaXRpZ2F0aW9uLm4wMDAwIl0sInN1YiI6IkNDQyIsImF1ZCI6ImNjYy5zdGFnaW5nLmNsaW1hdGVwb2xpY3lyYWRhci5vcmciLCJpc3MiOiJDbGltYXRlIFBvbGljeSBSYWRhciIsImV4cCI6MjA2NDU3MzY4OC4wLCJpYXQiOjE3NDkwNDA4ODh9.3HILyhmw3b91IUATQ9Irz15SYJbuNwgyy4V77W9ETRk",
      "Content-Type": "application/json",
    },
  });
  check_response(res);
  return res;
}

export function do_basic_search(query, base_url, api_url) {
  console.log("Searching for: " + query);

  // Get main HTML page
  let res = get_with_app_token(
    `${base_url}/search?q=${encodeURIComponent(query)}`,
  );

  const params = {
    query_string: encodeURIComponent(query),
    exact_match: false,
    max_passages_per_doc: 10,
    keyword_filters: {},
    year_range: [1947, 2023],
    sort_field: null,
    sort_order: "desc",
    limit: 10,
    offset: 0,
  };
  // Get API results
  res = post_with_app_token(`${api_url}/api/v1/searches`, params);
  return res;
}
