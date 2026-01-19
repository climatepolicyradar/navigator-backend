import { check, sleep, group } from "k6";
import { htmlReport } from "https://raw.githubusercontent.com/benc-uk/k6-reporter/main/dist/bundle.js";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.1/index.js";
import http from "k6/http";

const base_url = "https://ccc.staging.climatepolicyradar.org/";
const api_url = "https://ccc.staging.climatepolicyradar.org/api/";

/**
 * The handleSummary function generates the test summary report in both
 * HTML and stdout format.
 */
export function handleSummary(data) {
  return {
    "./load-tests/reports/k6-hello-world.html": htmlReport(data),
    stdout: textSummary(data, { indent: " ", enableColors: true }),
  };
}

export default function () {
  http.get(base_url);
  sleep(1);
}
