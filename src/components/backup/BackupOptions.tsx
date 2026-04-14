import { useEffect, useState } from "react";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type {
  StorageConfig,
  ParquetStorageMode,
  HivePartitionKind,
  HivePartitioning,
  ParquetOptions,
  ColumnInfo,
} from "@/lib/types";
import { api } from "@/lib/tauri";

export interface BackupOptionsState {
  format: string;
  compression: string;
  filenamePrefix: string;
  retention: number;
  keepLocal: boolean;
  noOwner: boolean;
  noAcl: boolean;
  storageId: string;
  streaming: boolean;
  parquetOptions: ParquetOptions;
}

interface BackupOptionsProps {
  options: BackupOptionsState;
  onChange: (opts: BackupOptionsState) => void;
  storageTargets: StorageConfig[];
  connectionUrl?: string;
  selectedTables?: string[];
}

export function BackupOptions({
  options,
  onChange,
  storageTargets,
  connectionUrl,
  selectedTables,
}: BackupOptionsProps) {
  const set = (partial: Partial<BackupOptionsState>) =>
    onChange({ ...options, ...partial });

  const isParquet = options.format === "parquet";
  const pq = options.parquetOptions;

  const setPq = (partial: Partial<ParquetOptions>) =>
    set({ parquetOptions: { ...pq, ...partial } });

  const setHive = (partial: Partial<HivePartitioning>) =>
    setPq({ hive_partitioning: { ...pq.hive_partitioning, ...partial } });

  const [datetimeCols, setDatetimeCols] = useState<ColumnInfo[]>([]);
  const [loadingCols, setLoadingCols] = useState(false);

  const hiveKind = pq.hive_partitioning.kind;
  const needsColumn = hiveKind !== "none";

  useEffect(() => {
    if (!isParquet || !needsColumn || !connectionUrl || !selectedTables?.length) {
      setDatetimeCols([]);
      return;
    }

    setLoadingCols(true);
    const firstTable = selectedTables[0];
    const [schema, table] = firstTable.includes(".")
      ? firstTable.split(".", 2)
      : ["public", firstTable];

    api.introspect
      .datetimeColumns(connectionUrl, schema, table)
      .then(setDatetimeCols)
      .catch(() => setDatetimeCols([]))
      .finally(() => setLoadingCols(false));
  }, [isParquet, needsColumn, connectionUrl, selectedTables?.[0]]);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-3">
        <div className="space-y-1.5">
          <Label className="text-xs">Format</Label>
          <Select value={options.format} onValueChange={(v) => set({ format: v })}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="custom">Custom (.dump)</SelectItem>
              <SelectItem value="plain">Plain SQL (.sql)</SelectItem>
              <SelectItem value="tar">Tar (.tar)</SelectItem>
              <SelectItem value="parquet">Parquet (.parquet)</SelectItem>
            </SelectContent>
          </Select>
        </div>
        {!isParquet && (
          <div className="space-y-1.5">
            <Label className="text-xs">Compression</Label>
            <Select
              value={options.compression}
              onValueChange={(v) => set({ compression: v })}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="gzip-fast">Gzip (Fast)</SelectItem>
                <SelectItem value="gzip">Gzip (Default)</SelectItem>
                <SelectItem value="gzip-best">Gzip (Best)</SelectItem>
                <SelectItem value="none">None</SelectItem>
              </SelectContent>
            </Select>
          </div>
        )}
        <div className="space-y-1.5">
          <Label className="text-xs">Storage Target</Label>
          <Select
            value={options.storageId}
            onValueChange={(v) => set({ storageId: v })}
          >
            <SelectTrigger>
              <SelectValue placeholder="Select storage" />
            </SelectTrigger>
            <SelectContent>
              {storageTargets.map((s) => (
                <SelectItem key={s.id} value={s.id}>
                  {s.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-1.5">
          <Label className="text-xs">Filename Prefix</Label>
          <Input
            value={options.filenamePrefix}
            onChange={(e) => set({ filenamePrefix: e.target.value })}
          />
        </div>
        <div className="space-y-1.5">
          <Label className="text-xs">Retention (backups to keep)</Label>
          <Input
            type="number"
            min={0}
            value={options.retention}
            onChange={(e) => set({ retention: Number(e.target.value) })}
          />
        </div>
      </div>

      {isParquet && (
        <div className="space-y-3 rounded-md border border-border p-3">
          <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
            Parquet Options
          </p>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <Label className="text-xs">Storage Mode</Label>
              <Select
                value={pq.storage_mode}
                onValueChange={(v) => setPq({ storage_mode: v as ParquetStorageMode })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="archive">Archive (.tar.gz)</SelectItem>
                  <SelectItem value="individual">Individual Files</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Max Rows Per File</Label>
              <Input
                type="number"
                min={1000}
                placeholder="No limit (500k default)"
                value={pq.max_rows_per_file ?? ""}
                onChange={(e) =>
                  setPq({
                    max_rows_per_file: e.target.value
                      ? Number(e.target.value)
                      : null,
                  })
                }
              />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <Label className="text-xs">Hive Partitioning</Label>
              <Select
                value={hiveKind}
                onValueChange={(v) => setHive({ kind: v as HivePartitionKind })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">None</SelectItem>
                  <SelectItem value="year">By Year</SelectItem>
                  <SelectItem value="year_month">By Year / Month</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {needsColumn && (
              <div className="space-y-1.5">
                <Label className="text-xs">Partition Column</Label>
                <Select
                  value={pq.hive_partitioning.column ?? ""}
                  onValueChange={(v) => setHive({ column: v })}
                  disabled={loadingCols || datetimeCols.length === 0}
                >
                  <SelectTrigger>
                    <SelectValue
                      placeholder={
                        loadingCols
                          ? "Loading..."
                          : datetimeCols.length === 0
                            ? "No datetime columns"
                            : "Select column"
                      }
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {datetimeCols.map((col) => (
                      <SelectItem key={col.name} value={col.name}>
                        {col.name} ({col.data_type})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="flex items-center gap-6">
        {!isParquet && (
          <>
            <label className="flex items-center gap-2 text-sm">
              <Switch
                checked={options.noOwner}
                onCheckedChange={(v) => set({ noOwner: v })}
              />
              No Owner
            </label>
            <label className="flex items-center gap-2 text-sm">
              <Switch
                checked={options.noAcl}
                onCheckedChange={(v) => set({ noAcl: v })}
              />
              No ACL
            </label>
          </>
        )}
        <label className="flex items-center gap-2 text-sm">
          <Switch
            checked={options.keepLocal}
            onCheckedChange={(v) => set({ keepLocal: v })}
          />
          Keep Local Copy
        </label>
        {!isParquet && (
          <label
            className="flex items-center gap-2 text-sm"
            title="Stream pg_dump directly to S3 without writing a local temp file. Recommended for large databases."
          >
            <Switch
              checked={options.streaming}
              onCheckedChange={(v) => set({ streaming: v })}
            />
            Stream to Cloud
          </label>
        )}
      </div>
    </div>
  );
}
