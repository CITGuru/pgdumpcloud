import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { RotateCcw, Loader2 } from "lucide-react";
import { BackupBrowser } from "./BackupBrowser";
import { api, buildConnectionUrl, buildUrlForDb } from "@/lib/tauri";
import type {
  ConnectionConfig,
  StorageConfig,
  DatabaseInfo,
  RestoreRequest,
} from "@/lib/types";

interface RestorePanelProps {
  connections: ConnectionConfig[];
  storageTargets: StorageConfig[];
  onJobCreated?: (jobId: string) => void;
}

export function RestorePanel({
  connections,
  storageTargets,
  onJobCreated,
}: RestorePanelProps) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [storageId, setStorageId] = useState(storageTargets[0]?.id ?? "");
  const [targetConnId, setTargetConnId] = useState(connections[0]?.id ?? "");
  const [targetDb, setTargetDb] = useState("");
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]);
  const [loadingDbs, setLoadingDbs] = useState(false);
  const [selectedBackup, setSelectedBackup] = useState("");
  const [clean, setClean] = useState(true);
  const [noOwner, setNoOwner] = useState(true);
  const [noAcl, setNoAcl] = useState(true);
  const [dataOnly, setDataOnly] = useState(false);

  const storage = storageTargets.find((s) => s.id === storageId) ?? null;
  const targetConn = connections.find((c) => c.id === targetConnId);

  useEffect(() => {
    if (!targetConn) {
      setDatabases([]);
      setTargetDb("");
      return;
    }
    setLoadingDbs(true);
    const defaultUrl = buildConnectionUrl(targetConn);
    api.introspect
      .databases(defaultUrl)
      .then((dbs) => {
        setDatabases(dbs);
        setTargetDb(targetConn.database || "postgres");
      })
      .catch(() => {
        setDatabases([]);
        setTargetDb(targetConn.database || "postgres");
      })
      .finally(() => setLoadingDbs(false));
  }, [targetConnId]);

  const handleRestore = async () => {
    if (!selectedBackup || !targetConn || !storage || !targetDb) return;

    const restoreUrl = buildUrlForDb(targetConn, targetDb);

    const request: RestoreRequest = {
      backup_key: selectedBackup,
      target_url: restoreUrl,
      clean,
      no_owner: noOwner,
      no_acl: noAcl,
      data_only: dataOnly,
      storage_endpoint: storage.endpoint ?? "",
      storage_bucket: storage.bucket ?? "",
      storage_region: storage.region ?? "us-east-1",
      storage_access_key: storage.access_key ?? "",
      storage_secret_key: storage.secret_key ?? "",
    };

    setSubmitting(true);
    setError(null);
    try {
      const jobId = await api.jobs.createRestore(request);
      onJobCreated?.(jobId);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Source</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="space-y-1.5">
            <Label className="text-xs">Storage Target</Label>
            <Select value={storageId} onValueChange={setStorageId}>
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
          <BackupBrowser
            storage={storage}
            selectedKey={selectedBackup}
            onSelect={setSelectedBackup}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Target</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <Label className="text-xs">Connection</Label>
              <Select value={targetConnId} onValueChange={setTargetConnId}>
                <SelectTrigger>
                  <SelectValue placeholder="Select connection" />
                </SelectTrigger>
                <SelectContent>
                  {connections.map((c) => (
                    <SelectItem key={c.id} value={c.id}>
                      {c.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Database</Label>
              <Select value={targetDb} onValueChange={setTargetDb}>
                <SelectTrigger>
                  <SelectValue placeholder={loadingDbs ? "Loading..." : "Select database"} />
                </SelectTrigger>
                <SelectContent>
                  {databases.map((db) => (
                    <SelectItem key={db.name} value={db.name}>
                      {db.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-6">
            <label className="flex items-center gap-2 text-sm">
              <Switch checked={clean} onCheckedChange={setClean} />
              Clean (drop existing)
            </label>
            <label className="flex items-center gap-2 text-sm">
              <Switch checked={noOwner} onCheckedChange={setNoOwner} />
              No Owner
            </label>
            <label className="flex items-center gap-2 text-sm">
              <Switch checked={noAcl} onCheckedChange={setNoAcl} />
              No ACL
            </label>
            <label className="flex items-center gap-2 text-sm">
              <Switch checked={dataOnly} onCheckedChange={setDataOnly} />
              Data Only
            </label>
          </div>
          {dataOnly && (
            <p className="text-xs text-muted-foreground">
              Only restore row data. Target database must already have the schema set up.
            </p>
          )}
        </CardContent>
      </Card>

      <div className="flex items-center gap-3">
        <Button
          onClick={handleRestore}
          disabled={submitting || !selectedBackup || !targetConn || !targetDb}
          className="gap-2"
          variant="default"
        >
          {submitting ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <RotateCcw className="h-4 w-4" />
          )}
          {submitting ? "Creating Job..." : "Restore Selected"}
        </Button>
      </div>

      {error && <p className="text-sm text-destructive">{error}</p>}
    </div>
  );
}
