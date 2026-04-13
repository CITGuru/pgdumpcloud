import { useState, useEffect, useRef } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Loader2, CheckCircle2, XCircle, Link, Unlink } from "lucide-react";
import type { ConnectionConfig, ConnectionInfo } from "@/lib/types";
import { buildConnectionUrl } from "@/lib/tauri";

interface ConnectionFormProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  initial?: ConnectionConfig;
  onSave: (conn: ConnectionConfig) => Promise<void>;
  onTest: (url: string) => Promise<ConnectionInfo>;
}

function parseUrlToFields(url: string) {
  try {
    const u = new URL(url);
    return {
      host: u.hostname || "localhost",
      port: u.port ? Number(u.port) : 5432,
      username: decodeURIComponent(u.username || ""),
      password: decodeURIComponent(u.password || ""),
      database: u.pathname.replace(/^\//, "") || "postgres",
    };
  } catch {
    return null;
  }
}

export function ConnectionForm({
  open,
  onOpenChange,
  initial,
  onSave,
  onTest,
}: ConnectionFormProps) {
  const [name, setName] = useState("");
  const [host, setHost] = useState("localhost");
  const [port, setPort] = useState(5432);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [database, setDatabase] = useState("postgres");
  const [urlInput, setUrlInput] = useState("");
  const [urlMode, setUrlMode] = useState(false);

  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<ConnectionInfo | null>(null);
  const [testError, setTestError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const skipSync = useRef(false);

  useEffect(() => {
    if (!open) return;
    if (initial) {
      setName(initial.name);
      setHost(initial.host || "localhost");
      setPort(initial.port || 5432);
      setUsername(initial.username || "");
      setPassword(initial.password || "");
      setDatabase(initial.database || "postgres");
      setUrlInput(buildConnectionUrl(initial));
      setUrlMode(false);
    } else {
      setName("");
      setHost("localhost");
      setPort(5432);
      setUsername("");
      setPassword("");
      setDatabase("postgres");
      setUrlInput("");
      setUrlMode(false);
    }
    setTestResult(null);
    setTestError(null);
  }, [open, initial]);

  useEffect(() => {
    if (skipSync.current) {
      skipSync.current = false;
      return;
    }
    if (!urlMode) {
      const builtUrl = buildConnectionUrl({ host, port, username, password, database });
      setUrlInput(builtUrl);
    }
  }, [host, port, username, password, database, urlMode]);

  const handleUrlChange = (val: string) => {
    setUrlInput(val);
    const parsed = parseUrlToFields(val);
    if (parsed) {
      skipSync.current = true;
      setHost(parsed.host);
      setPort(parsed.port);
      setUsername(parsed.username);
      setPassword(parsed.password);
      setDatabase(parsed.database);
    }
  };

  const currentUrl = buildConnectionUrl({ host, port, username, password, database });

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    setTestError(null);
    try {
      const info = await onTest(currentUrl);
      setTestResult(info);
    } catch (err) {
      setTestError(err instanceof Error ? err.message : String(err));
    } finally {
      setTesting(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      await onSave({
        id: initial?.id ?? crypto.randomUUID(),
        name,
        host,
        port,
        username,
        password,
        database,
        ssl_mode: undefined,
      });
      onOpenChange(false);
    } catch (err) {
      console.error(err);
    } finally {
      setSaving(false);
    }
  };

  const canSave = name.trim() && host.trim();

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>
            {initial ? "Edit Connection" : "New Connection"}
          </DialogTitle>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label htmlFor="conn-name">Connection Name</Label>
            <Input
              id="conn-name"
              placeholder="Production Server"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>

          <Separator />

          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label className="text-xs text-muted-foreground">
                Database URL
              </Label>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 gap-1 text-xs"
                onClick={() => setUrlMode(!urlMode)}
              >
                {urlMode ? (
                  <><Unlink className="h-3 w-3" /> Use fields</>
                ) : (
                  <><Link className="h-3 w-3" /> Edit URL</>
                )}
              </Button>
            </div>
            <Input
              value={urlInput}
              onChange={(e) => handleUrlChange(e.target.value)}
              readOnly={!urlMode}
              className={`font-mono text-xs overflow-x-auto ${!urlMode ? "bg-muted/50 text-muted-foreground cursor-default" : ""}`}
              placeholder="postgres://user:pass@host:5432/dbname"
            />
          </div>

          <div className="grid grid-cols-[1fr_100px] gap-3">
            <div className="space-y-1.5">
              <Label className="text-xs">Host</Label>
              <Input
                value={host}
                onChange={(e) => setHost(e.target.value)}
                placeholder="localhost"
              />
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Port</Label>
              <Input
                type="number"
                value={port}
                onChange={(e) => setPort(Number(e.target.value) || 5432)}
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <Label className="text-xs">Username</Label>
              <Input
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="postgres"
              />
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Password</Label>
              <Input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>
          </div>

          <div className="space-y-1.5">
            <Label className="text-xs">Default Database</Label>
            <Input
              value={database}
              onChange={(e) => setDatabase(e.target.value)}
              placeholder="postgres"
            />
            <p className="text-[11px] text-muted-foreground">
              Used to connect to the server. You can select other databases after saving.
            </p>
          </div>

          <Separator />

          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={handleTest}
              disabled={testing || !host}
            >
              {testing && <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />}
              Test Connection
            </Button>
            {testResult && (
              <Badge variant="outline" className="gap-1 text-green-600 border-green-600">
                <CheckCircle2 className="h-3 w-3" />
                {testResult.latency_ms}ms
              </Badge>
            )}
            {testError && (
              <Badge variant="outline" className="gap-1 text-destructive border-destructive">
                <XCircle className="h-3 w-3" />
                Failed
              </Badge>
            )}
          </div>
          {testResult && (
            <p className="text-xs text-muted-foreground break-words">
              {testResult.pg_version}
            </p>
          )}
          {testError && (
            <p className="text-xs text-destructive break-words">{testError}</p>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={saving || !canSave}>
            {saving && <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />}
            Save
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
