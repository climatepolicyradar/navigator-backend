//Browser VUs consume 10 times more VU hours compared to Protocol VUs. For more details, refer to Understand your Performance Testing invoice.

import { browser } from "k6/browser";
import { expect, check } from "k6";

const base_url = "https://ccc.staging.climatepolicyradar.org";
const api_url = "https://cpr.staging.climatepolicyradar.org";

export const options = {
  scenarios: {
    ui: {
      executor: "shared-iterations",
      options: {
        browser: {
          type: "chromium",
        },
      },
      vus: 1,
      iterations: 1,
      maxDuration: "10s",
    },
  },
  thresholds: {
    checks: ["rate==1.0"],
  },
  cloud: {
    name: "CCC Launch load testing -- browser",
    projectID: 4453718,
  },
};

export default async function () {
  const page = await browser.newPage();

  try {
    await page.goto(base_url);
    // Verify we're on the CCC homepage by checking the Alpha logo
    await expect(page.locator("h1")).toHaveText("Climate Case Chart");
    await expect(page.locator('[data-cy="search-input"]')).toHaveAttribute(
      "placeholder",
      "Search the full text of any document",
    );

    const searchInput = page.locator('[data-cy="search-input"]');
    await expect(searchInput).toBeVisible();
    await expect(searchInput).toHaveAttribute(
      "placeholder",
      "Search the full text of any document",
    );

    // Search button
    const searchButton = page.locator('button[aria-label="Search"]');
    await expect(searchButton).toBeVisible();

    // Search form
    await expect(page.locator('[data-cy="search-form"]')).toBeVisible();

    // Exact match checkbox
    await expect(page.locator("#exact-match")).toBeVisible();

    // Quick search suggestions
    await expect(page.getByText("Search by:")).toBeVisible();
  } finally {
    await page.close();
  }
}
