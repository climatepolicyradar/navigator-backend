# Load tests for navigator backend

This directory contains initial load test scripts for the navigator backend.

We use k6 and grafana cloud to run the tests.

## Running the tests

You'll need a
[grafana cloud token](https://climatepolicyradar.grafana.net/a/k6-app/settings/api-token)
to run the tests with Grafana. Run `export K6_CLOUD_TOKEN=<token>` to set it.

### Basic running

To run the tests locally, run `k6 run main.js`.

To run the tests in grafana cloud, run `k6 cloud main.js`.

To run the tests locally but stream data to Grafana, run
`k6 run --out cloud main.js`.

### Options

Use environment variables to control test parameters.

- `ENVIRONMENT`: The environment to run the tests in. Defaults to `staging`.
  Possible values: `staging`, `production`.
- `SCENARIO`: The scenario to run. Defaults to `basic-search-browse`. Possible
  values are listed in `config/tests.json`.
- `BASE_URL`: The base URL to run the tests against. Defaults to
  `https://ccc.staging.climatepolicyradar.org`.
- `API_URL`: The API URL to run the tests against. Defaults to
  `https://cpr.staging.climatepolicyradar.org`.
- `SCENARIO`: The scenario to run. Defaults to `basic-search-browse`. Possible
  values are listed in `config/tests.json`.

For example: `ENVIRONMENT=staging SCENARIO=basic-search-browse k6 run main.js`

### Scenarios

See `config/tests.json` for the list of scenarios and parameters.

- Basic search and browse (basic-search-browse) [SMOKE TEST] A basic search and
  browse scenario
- Search and browse average load (search-browse-average-load) [AVG LOAD TEST] A
  search and browse scenario with an average load. 1 basic s&b VU results in
  about 1.33 req/s. I think that means 4VUs is about right for a realistic load
  test.
- Search and browse CCC load (search-browse-ccc-load) [AVG LOAD TEST] A search
  and browse scenario with an average load. CCC will likely double load.
- Search and browse low stress load (search-browse-low-stress-load) [STRESS
  TEST] A search and browse scenario with a low stress load.
- Search and browse mid stress load (search-browse-mid-stress-load) [STRESS
  TEST] A search and browse scenario with a mid stress load.
- Search and browse high stress load (search-browse-high-stress-load) [STRESS
  TEST] A search and browse scenario with a high stress load.

## Next steps

K6 has a lot of functionality that we may or may not want to make use of.

- Browser-based testing. See `browser-basic.js` for an example. K6 uses a
  playwright-compatible API to control a browser. Although billed at 10x more
  than basic k6 tests, it might help us understand frontend performance better
  -- though whether we get a good diff on the value against current tooling is
  unknown.

- Generating k6 scripts from openapi specs. We can automate the generation of
  openapi specs from our FastAPI services, and then use those specs to generate
  interfaces k6 can use to wrap the API. This would help us reduce the cost of
  change on these tests and get better coverage.

- More scenarios. The basic load test script is a simple search-browse user
  journey. Scenarios let us test more complex user journeys.

## Best practices

- Use a small number of browser virtual users with larger numbers of protocol
  virtual users -- browser VUs consume 10 times more VU hours compared to
  Protocol VUs.
- This tests browser performance while API is under heavy load and gives a good
  sense of combined frontend and backend performance.
- See
  https://grafana.com/docs/k6/latest/using-k6-browser/recommended-practices/hybrid-approach-to-performance/

## Further reading

- You can get a lot of control over how k6 shapes the number of virtual users
  over time. See
  [this](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/) or
  [this](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/)
  for more information.
