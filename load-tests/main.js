// Scavenged from https://github.com/tom-miseur/k6-templates/blob/main/vanilla/main.js

// Export scenario functions for k6 to use
export { basic_search_browse_scenario } from "./scenarios/basic-search-browse-scenario.js";

globalThis.environment = __ENV.ENVIRONMENT || "staging"; // Default to staging!!
globalThis.scenario = __ENV.SCENARIO || "basic-search-browse";

// load test config, used to populate exported options object:
const testConfig = JSON.parse(open("./config/tests.json"));

const urls = {
  production: {
    base: "https://ccc.climatepolicyradar.org",
    api: "https://cpr.climatepolicyradar.org",
  },
  staging: {
    base: "https://ccc.staging.climatepolicyradar.org",
    api: "https://cpr.staging.climatepolicyradar.org",
  },
};

const base_url = urls[globalThis.environment].base;
const api_url = urls[globalThis.environment].api;

globalThis.base_url = base_url;
globalThis.api_url = api_url;

console.log("Executing scenario", globalThis.scenario);
console.log("Environment", globalThis.environment);

// Remove unselected scenarios from test config
const { scenarios, ...testConfigWithoutScenarios } = testConfig;

// Put universal options here
export const options = Object.assign(
  {
    scenarios: {
      [globalThis.scenario]: testConfig.scenarios[globalThis.scenario],
    },
    ext: {
      cloud: {
        projectID: 4453718, // navigator-backend on grafana
        name: "CCC Launch load testing",
      },
    },
  },
  testConfigWithoutScenarios,
);

export default function () {
  console.log("No scenarios in test.json. Executing default function...");
}
