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
  id: string;
  type: string;
};

export default function Labels() {
  const columns = useMemo<ColumnDef<Label>[]>(
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
        id: "type",
        accessorKey: "type",
        header: ({ column, table }) => (
          <div className="flex items-center gap-1">
            <span>Type</span>
            <div>
              <DataTableFilterDropdownText
                defaultOperator="contains"
                column={column}
                table={table}
                placeholder="Filter by type"
              />
            </div>
          </div>
        ),
      },
    ],
    [],
  );

  const table = useTable<Label>({
    columns,
    refineCoreProps: {
      resource: "labels",
      pagination: {
        pageSize: 100,
      },
    },
  });

  return (
    <ListView>
      <ListViewHeader title="Labels" />

      <DataTable table={table} />
    </ListView>
  );
}
