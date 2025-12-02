import { useMemo, useState, useEffect } from "react";
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
  const [labelFilters, setLabelFilters] = useState<string[]>([]);
  const [labelInput, setLabelInput] = useState("");

  const handleAddLabel = () => {
    if (labelInput.trim() && !labelFilters.includes(labelInput.trim())) {
      setLabelFilters([...labelFilters, labelInput.trim()]);
      setLabelInput("");
    }
  };

  const handleRemoveLabel = (labelToRemove: string) => {
    setLabelFilters(labelFilters.filter((label) => label !== labelToRemove));
  };

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

  // Update filters whenever labelFilters changes
  useEffect(() => {
    if (labelFilters.length === 0) {
      table.refineCore.setFilters([], "replace");
    } else {
      const filters = {
        field: "labels.label.id",
        operator: "in" as const,
        value: labelFilters.map((labelId) => labelId),
      };

      table.refineCore.setFilters([filters]);
    }
  }, [labelFilters]);

  return (
    <ListView>
      <ListViewHeader title="Documents" />
      <div className="mb-4 space-y-2">
        <div className="flex gap-2">
          <Input
            placeholder="Enter label ID (e.g., no_family_labels)"
            value={labelInput}
            onChange={(e) => setLabelInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                handleAddLabel();
              }
            }}
            className="max-w-md"
          />
          <Button onClick={handleAddLabel}>Add Filter</Button>
        </div>
        {labelFilters.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {labelFilters.map((labelId) => (
              <Badge
                key={labelId}
                variant="default"
                className="cursor-pointer"
                onClick={() => handleRemoveLabel(labelId)}
              >
                {labelId} ×
              </Badge>
            ))}
          </div>
        )}
      </div>
      <DataTable table={table} />
    </ListView>
  );
}
