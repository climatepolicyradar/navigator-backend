import { useMemo } from "react";
import { useTable } from "@refinedev/react-table";
import type { ColumnDef } from "@tanstack/react-table";
import { DataTable } from "@/components/refine-ui/data-table/data-table";
import { DataTableSorter } from "@/components/refine-ui/data-table/data-table-sorter";
import { DataTableFilterDropdownText } from "@/components/refine-ui/data-table/data-table-filter";
import {
  ListView,
  ListViewHeader,
} from "@/components/refine-ui/views/list-view";

type Label = {
  type: string;
  label: {
    id: string;
    title: string;
    type: string;
  };
};

type Relationship = {
  type: string;
  document: {
    id: string;
    title: string;
    labels: Label[];
  };
};

type Document = {
  id: string;
  title: string;
  labels: Label[];
  relationships: Relationship[];
};

export default function Documents() {
  const columns = useMemo<ColumnDef<Document>[]>(
    () => [
      {
        id: "id",
        accessorKey: "id",
        header: ({ column }) => (
          <div className="flex items-center gap-1">
            <span>ID</span>
            <DataTableSorter column={column} />
          </div>
        ),
      },
      {
        id: "title",
        accessorKey: "title",
        header: ({ column, table }) => (
          <div className="flex items-center gap-1">
            <span>Title</span>
            <div>
              <DataTableFilterDropdownText
                defaultOperator="contains"
                column={column}
                table={table}
                placeholder="Filter by title"
              />
            </div>
          </div>
        ),
      },
    ],
    [],
  );

  const table = useTable<Document>({
    columns,
    refineCoreProps: {
      resource: "documents",
    },
  });

  return (
    <ListView>
      <ListViewHeader title="Documents" />
      <DataTable table={table} />
    </ListView>
  );
}
