import { useState, useEffect } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { ChevronRight, Search, Loader2 } from "lucide-react";
import type { SchemaInfo, TableInfo } from "@/lib/types";
import { api } from "@/lib/tauri";

interface TablePickerProps {
  connectionUrl: string;
  selectedTables: string[];
  onSelectionChange: (tables: string[]) => void;
}

interface SchemaState {
  info: SchemaInfo;
  tables: TableInfo[];
  loaded: boolean;
  open: boolean;
}

export function TablePicker({
  connectionUrl,
  selectedTables,
  onSelectionChange,
}: TablePickerProps) {
  const [schemas, setSchemas] = useState<SchemaState[]>([]);
  const [loading, setLoading] = useState(false);
  const [filter, setFilter] = useState("");

  useEffect(() => {
    if (!connectionUrl) return;
    setLoading(true);
    api.introspect
      .schemas(connectionUrl)
      .then((list) => {
        setSchemas(
          list.map((s) => ({
            info: s,
            tables: [],
            loaded: false,
            open: false,
          }))
        );
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [connectionUrl]);

  const loadTables = async (schemaName: string) => {
    try {
      const tables = await api.introspect.tables(connectionUrl, schemaName);
      setSchemas((prev) =>
        prev.map((s) =>
          s.info.name === schemaName
            ? { ...s, tables, loaded: true }
            : s
        )
      );
    } catch (err) {
      console.error(err);
    }
  };

  const toggleSchema = (schemaName: string) => {
    setSchemas((prev) =>
      prev.map((s) => {
        if (s.info.name === schemaName) {
          if (!s.loaded && !s.open) {
            loadTables(schemaName);
          }
          return { ...s, open: !s.open };
        }
        return s;
      })
    );
  };

  const isTableSelected = (schema: string, table: string) =>
    selectedTables.includes(`${schema}.${table}`);

  const toggleTable = (schema: string, table: string) => {
    const key = `${schema}.${table}`;
    if (selectedTables.includes(key)) {
      onSelectionChange(selectedTables.filter((t) => t !== key));
    } else {
      onSelectionChange([...selectedTables, key]);
    }
  };

  const isSchemaFullySelected = (s: SchemaState) =>
    s.loaded && s.tables.length > 0 && s.tables.every((t) => isTableSelected(s.info.name, t.name));

  const toggleSchemaAll = (s: SchemaState) => {
    if (!s.loaded) return;
    const keys = s.tables.map((t) => `${s.info.name}.${t.name}`);
    if (isSchemaFullySelected(s)) {
      onSelectionChange(selectedTables.filter((t) => !keys.includes(t)));
    } else {
      const existing = selectedTables.filter((t) => !keys.includes(t));
      onSelectionChange([...existing, ...keys]);
    }
  };

  const filterLower = filter.toLowerCase();

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8 text-muted-foreground">
        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
        Loading schemas...
      </div>
    );
  }

  if (!connectionUrl) {
    return (
      <p className="py-8 text-center text-sm text-muted-foreground">
        Select a connection to browse tables
      </p>
    );
  }

  return (
    <div className="space-y-2">
      <div className="relative">
        <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Filter tables..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="pl-9"
        />
      </div>
      <ScrollArea className="h-[280px] rounded-md border p-2">
        {schemas.map((s) => {
          const filteredTables = s.tables.filter((t) =>
            t.name.toLowerCase().includes(filterLower)
          );
          if (filter && s.loaded && filteredTables.length === 0) return null;

          return (
            <Collapsible
              key={s.info.name}
              open={s.open}
              onOpenChange={() => toggleSchema(s.info.name)}
            >
              <div className="flex items-center gap-2 py-1">
                <Checkbox
                  checked={isSchemaFullySelected(s)}
                  onCheckedChange={() => toggleSchemaAll(s)}
                  disabled={!s.loaded}
                />
                <CollapsibleTrigger className="flex items-center gap-1 text-sm font-medium flex-1">
                  <ChevronRight
                    className={`h-3.5 w-3.5 transition-transform ${
                      s.open ? "rotate-90" : ""
                    }`}
                  />
                  {s.info.name}
                  <span className="ml-auto text-xs text-muted-foreground">
                    {s.info.table_count} tables
                  </span>
                </CollapsibleTrigger>
              </div>
              <CollapsibleContent>
                <div className="ml-6 space-y-0.5">
                  {!s.loaded ? (
                    <div className="flex items-center gap-1 text-xs text-muted-foreground py-1">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      Loading...
                    </div>
                  ) : (
                    filteredTables.map((t) => (
                      <label
                        key={t.name}
                        className="flex items-center gap-2 py-0.5 text-sm cursor-pointer hover:bg-accent/30 rounded px-1"
                      >
                        <Checkbox
                          checked={isTableSelected(s.info.name, t.name)}
                          onCheckedChange={() =>
                            toggleTable(s.info.name, t.name)
                          }
                        />
                        <span className="flex-1 truncate">{t.name}</span>
                        <span className="text-xs text-muted-foreground tabular-nums">
                          {t.row_estimate.toLocaleString()} rows
                        </span>
                        <span className="text-xs text-muted-foreground tabular-nums w-16 text-right">
                          {t.size_pretty}
                        </span>
                      </label>
                    ))
                  )}
                </div>
              </CollapsibleContent>
            </Collapsible>
          );
        })}
      </ScrollArea>
      {selectedTables.length > 0 && (
        <p className="text-xs text-muted-foreground">
          {selectedTables.length} table{selectedTables.length !== 1 ? "s" : ""} selected
          {" "}
          <button
            className="underline hover:text-foreground"
            onClick={() => onSelectionChange([])}
          >
            clear
          </button>
        </p>
      )}
    </div>
  );
}
