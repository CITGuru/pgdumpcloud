import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Play, Loader2 } from "lucide-react";
import { TablePicker } from "./TablePicker";
import { BackupOptions, type BackupOptionsState } from "./BackupOptions";
import { api, buildConnectionUrl, buildUrlForDb } from "@/lib/tauri";
import type {
  ConnectionConfig,
  StorageConfig,
  DatabaseInfo,
  BackupRequest,
} from "@/lib/types";

interface BackupPanelProps {
  connections: ConnectionConfig[];
  storageTargets: StorageConfig[];
  selectedConnectionId: string | null;
  onJobCreated?: (jobId: string) => void;
}

export function BackupPanel({
  connections,
  storageTargets,
  selectedConnectionId,
  onJobCreated,
}: BackupPanelProps) {
  const selectedConn = connections.find((c) => c.id === selectedConnectionId);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]);
  const [selectedDb, setSelectedDb] = useState<string>("");
  const [loadingDbs, setLoadingDbs] = useState(false);
  const [options, setOptions] = useState<BackupOptionsState>({
    format: "custom",
    compression: "gzip",
    filenamePrefix: "backup",
    retention: 7,
    keepLocal: false,
    noOwner: true,
    noAcl: true,
    storageId: storageTargets[0]?.id ?? "",
    streaming: false,
  });

  useEffect(() => {
    if (!selectedConn) {
      setDatabases([]);
      setSelectedDb("");
      return;
    }
    setLoadingDbs(true);
    setSelectedTables([]);
    const defaultUrl = buildConnectionUrl(selectedConn);
    api.introspect
      .databases(defaultUrl)
      .then((dbs) => {
        setDatabases(dbs);
        setSelectedDb(selectedConn.database || "postgres");
      })
      .catch((err) => {
        console.error("Failed to list databases:", err);
        setDatabases([]);
        setSelectedDb(selectedConn.database || "postgres");
      })
      .finally(() => setLoadingDbs(false));
  }, [selectedConnectionId]);

  const activeUrl = selectedConn
    ? buildUrlForDb(selectedConn, selectedDb || selectedConn.database)
    : "";

  const handleRunBackup = async () => {
    if (!selectedConn || !activeUrl) return;

    const storage = storageTargets.find((s) => s.id === options.storageId);
    if (!storage) return;

    const schemas = [
      ...new Set(selectedTables.map((t) => t.split(".")[0])),
    ];

    const request: BackupRequest = {
      connection_url: activeUrl,
      format: options.format,
      compression: options.compression,
      schemas: selectedTables.length > 0 ? schemas : [],
      tables: selectedTables.length > 0 ? selectedTables : [],
      no_owner: options.noOwner,
      no_acl: options.noAcl,
      storage_endpoint: storage.endpoint ?? "",
      storage_bucket: storage.bucket ?? "",
      storage_region: storage.region ?? "us-east-1",
      storage_access_key: storage.access_key ?? "",
      storage_secret_key: storage.secret_key ?? "",
      storage_prefix: storage.prefix ?? "",
      filename_prefix: options.filenamePrefix,
      retention: options.retention,
      keep_local: options.keepLocal,
      streaming: options.streaming,
    };

    setSubmitting(true);
    setError(null);
    try {
      const jobId = await api.jobs.createBackup(request);
      onJobCreated?.(jobId);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  };

  const handleDbChange = (db: string) => {
    setSelectedDb(db);
    setSelectedTables([]);
  };

  function formatSize(bytes: number | null): string {
    if (bytes == null) return "";
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
    if (bytes < 1024 * 1024 * 1024)
      return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Database</CardTitle>
        </CardHeader>
        <CardContent>
          {!selectedConn ? (
            <p className="text-sm text-muted-foreground">
              Select a connection from the sidebar
            </p>
          ) : (
            <div className="space-y-2">
              <Label className="text-xs">Target Database</Label>
              <Select value={selectedDb} onValueChange={handleDbChange}>
                <SelectTrigger>
                  <SelectValue placeholder={loadingDbs ? "Loading..." : "Select database"} />
                </SelectTrigger>
                <SelectContent>
                  {databases.map((db) => (
                    <SelectItem key={db.name} value={db.name}>
                      <div className="flex items-center gap-2">
                        <span>{db.name}</span>
                        {db.size_bytes != null && (
                          <span className="text-xs text-muted-foreground">
                            {formatSize(db.size_bytes)}
                          </span>
                        )}
                      </div>
                    </SelectItem>
                  ))}
                  {databases.length === 0 && !loadingDbs && (
                    <SelectItem value={selectedConn.database || "postgres"} disabled>
                      No databases found
                    </SelectItem>
                  )}
                </SelectContent>
              </Select>
              {loadingDbs && (
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  Loading databases...
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Schemas & Tables</CardTitle>
        </CardHeader>
        <CardContent>
          <TablePicker
            connectionUrl={activeUrl}
            selectedTables={selectedTables}
            onSelectionChange={setSelectedTables}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Options</CardTitle>
        </CardHeader>
        <CardContent>
          <BackupOptions
            options={options}
            onChange={setOptions}
            storageTargets={storageTargets}
          />
        </CardContent>
      </Card>

      <div className="flex items-center gap-3">
        <Button
          onClick={handleRunBackup}
          disabled={submitting || !selectedConn || !options.storageId || !selectedDb}
          className="gap-2"
        >
          {submitting ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Play className="h-4 w-4" />
          )}
          {submitting ? "Creating Job..." : "Run Backup"}
        </Button>
        {!selectedConn && (
          <p className="text-xs text-muted-foreground">
            Select a connection from the sidebar
          </p>
        )}
      </div>

      {error && <p className="text-sm text-destructive">{error}</p>}
    </div>
  );
}
