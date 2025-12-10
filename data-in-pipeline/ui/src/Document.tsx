import { LoadingOverlay } from "@/components/refine-ui/layout/loading-overlay";
import { DataTable } from "@/components/refine-ui/data-table/data-table";
import {
  ShowView,
  ShowViewHeader,
} from "@/components/refine-ui/views/show-view";
import { Link, useParsed, useShow } from "@refinedev/core";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { type Document } from "./Documents";

export default function Document() {
  const { id } = useParsed();

  const {
    result: document,
    query: { isFetching, isError, refetch },
  } = useShow<Document>({
    resource: "documents",
    id,
  });

  return (
    <ShowView>
      <ShowViewHeader title="Documents" />
      <LoadingOverlay loading={isFetching}>
        <div className="max-w-4xl mx-auto">
          <Card>
            <CardHeader>
              <div className="space-y-1">
                <CardTitle className="text-2xl">{document?.title}</CardTitle>
                <p className="text-sm font-mono text-muted-foreground">
                  {document?.id}
                </p>
              </div>
            </CardHeader>
            <CardContent className="space-y-6">
              <div>
                <h3 className="text-sm font-medium leading-none mb-3">
                  Labels
                </h3>
                {document?.labels?.length ? (
                  <div className="flex flex-wrap gap-2">
                    {document.labels.map((label, index) => (
                      <Badge
                        key={index}
                        variant="secondary"
                        className="px-3 py-1 text-sm"
                      >
                        {label.type}: {label.label.title}
                      </Badge>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No labels</p>
                )}
              </div>

              <Separator />

              <div>
                <h3 className="text-sm font-medium leading-none mb-3">
                  Relationships
                </h3>
                {document?.relationships?.length ? (
                  <div className="flex flex-wrap gap-2">
                    {document.relationships.map((relationship, index) => (
                      <Badge
                        key={index}
                        variant="secondary"
                        className="hover:bg-accent px-3 py-1 text-sm h-auto"
                      >
                        <Link
                          to={`/documents/${relationship.document.id}`}
                          className="flex items-center gap-1"
                        >
                          <span className="font-semibold">
                            {relationship.type}:
                          </span>
                          <span>{relationship.document.title}</span>
                        </Link>
                      </Badge>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    No relationships
                  </p>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </LoadingOverlay>
    </ShowView>
  );
}
