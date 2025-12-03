import {
  DataProvider,
  GitHubBanner,
  Refine,
  WelcomePage,
} from "@refinedev/core";
import { DevtoolsPanel, DevtoolsProvider } from "@refinedev/devtools";
import { RefineKbar, RefineKbarProvider } from "@refinedev/kbar";

import routerProvider, {
  DocumentTitleHandler,
  UnsavedChangesNotifier,
} from "@refinedev/react-router";
import dataProvider from "@refinedev/simple-rest";
import { BrowserRouter, Route, Routes } from "react-router";
import "./App.css";
import { Toaster } from "./components/refine-ui/notification/toaster";
import { useNotificationProvider } from "./components/refine-ui/notification/use-notification-provider";
import { ThemeProvider } from "./components/refine-ui/theme/theme-provider";
import Documents from "./Documents";

const baseProvider = dataProvider(
  "https://skillful-analysis-production.up.railway.app",
);

const documentsDataProvider: DataProvider = {
  ...baseProvider,

  getList: async ({ resource, pagination, filters }) => {
    const { currentPage = 1, pageSize = 100 } = pagination ?? {};

    const offset = (currentPage - 1) * pageSize;
    const limit = pageSize;

    const url = new URL(
      `https://skillful-analysis-production.up.railway.app/${resource}`,
    );
    url.searchParams.set("offset", offset.toString());
    url.searchParams.set("limit", limit.toString());

    if (filters) {
      const allFilter = filters.find((filter) => {
        return "field" in filter && filter.field === "all";
      });

      if (allFilter) {
        if (Array.isArray(allFilter.value)) {
          allFilter.value.forEach((filterString) => {
            const [field, value] = filterString.split("=");
            url.searchParams.append(field, value);
          });
        }
      }
    }

    console.log(url.toString());
    const response = await fetch(url.toString());
    const responseJson = await response.json();

    return {
      data: responseJson.data,
      total: responseJson.total,
    };
  },
};

function App() {
  return (
    <BrowserRouter>
      <RefineKbarProvider>
        <ThemeProvider>
          <DevtoolsProvider>
            <Refine
              dataProvider={documentsDataProvider}
              notificationProvider={useNotificationProvider()}
              routerProvider={routerProvider}
              options={{
                syncWithLocation: true,
                warnWhenUnsavedChanges: true,
                projectId: "nLXDfl-TPJHKS-dUb6Az",
              }}
            >
              <Routes>
                <Route index element={<Documents />} />
              </Routes>
              <Toaster />
              <RefineKbar />
              <UnsavedChangesNotifier />
              <DocumentTitleHandler />
            </Refine>
            <DevtoolsPanel />
          </DevtoolsProvider>
        </ThemeProvider>
      </RefineKbarProvider>
    </BrowserRouter>
  );
}

export default App;
