import { useMemo, useState, useEffect } from "react";
import { useSearchParams } from "react-router";
import { useTable } from "@refinedev/react-table";
import type { ColumnDef } from "@tanstack/react-table";
import { DataTable } from "@/components/refine-ui/data-table/data-table";
import { DataTableSorter } from "@/components/refine-ui/data-table/data-table-sorter";
import { DataTableFilterDropdownText } from "@/components/refine-ui/data-table/data-table-filter";
import {
  ListView,
  ListViewHeader,
} from "@/components/refine-ui/views/list-view";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

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
      {
        id: "labels",
        accessorKey: "labels",
        header: "Labels",
        cell: ({ row }) => {
          const labels = row.original.labels || [];
          return (
            <div className="flex flex-wrap gap-1">
              {labels.map((label, index) => (
                <Badge key={index} variant="secondary">
                  {label.type}: {label.label.title}
                </Badge>
              ))}
            </div>
          );
        },
      },
      {
        id: "relationships",
        accessorKey: "relationships",
        header: "Relationships",
        cell: ({ row }) => {
          const relationships = row.original.relationships || [];
          return (
            <div className="flex flex-wrap gap-1">
              {relationships.map((rel, index) => (
                <Badge key={index} variant="outline">
                  {rel.type}: {rel.document.title}
                </Badge>
              ))}
            </div>
          );
        },
      },
    ],
    [],
  );

  const table = useTable<Document>({
    columns,
    refineCoreProps: {
      resource: "documents",
      pagination: {
        pageSize: 100,
      },
    },
  });

  const initialFilters = table.refineCore.filters.find((filter) => {
    return "field" in filter && filter.field === "all";
  });
  const [filters, setFilters] = useState<string[]>(initialFilters?.value || []);

  useEffect(() => {
    if (filters.length === 0) {
      table.refineCore.setFilters([], "replace");
    } else {
      const filter = {
        field: "all",
        operator: "in" as const,
        value: filters,
      };

      table.refineCore.setFilters([filter]);
    }
  }, [filters]);

  return (
    <ListView>
      <ListViewHeader title="Documents" />
      <div className="mb-4 space-y-2">
        <div className="flex gap-2">
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const operatorValue = e.currentTarget.operator.value;
              const operator = operatorValue === "+" ? "" : operatorValue;
              const field = e.currentTarget.field.value;
              const value = e.currentTarget.value.value;
              setFilters([...filters, `${operator}${field}=${value}`]);
            }}
          >
            <div className="flex gap-2 items-center">
              <Select name="operator">
                <SelectTrigger>
                  <SelectValue placeholder="+/-" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="+">+</SelectItem>
                  <SelectItem value="-">-</SelectItem>
                </SelectContent>
              </Select>
              <Select name="field">
                <SelectTrigger>
                  <SelectValue placeholder="field" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="labels.label.id">
                    labels.label.id
                  </SelectItem>
                  <SelectItem value="labels.label.type">
                    labels.label.type
                  </SelectItem>
                  <SelectItem value="relationships.type">
                    relationships.type
                  </SelectItem>
                  <SelectItem value="len(relationships)">
                    len(relationships)
                  </SelectItem>
                  <SelectItem value="len(labels)">len(labels)</SelectItem>
                </SelectContent>
              </Select>
              <Input name="value" className="max-w-md" />
              <Button type="submit">Add Filter</Button>
            </div>
          </form>
        </div>
        {filters.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {filters.map((filter) => (
              <Badge
                key={filter}
                variant="default"
                className="cursor-pointer"
                onClick={() => {
                  const filterToRemove = filter;
                  setFilters(
                    filters.filter((filter) => filter !== filterToRemove),
                  );
                }}
              >
                {filter} ×
              </Badge>
            ))}
          </div>
        )}
      </div>
      <DataTable table={table} />
    </ListView>
  );
}
