import { useState, useEffect } from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Loader2, FileArchive, FileTypeCorner } from "lucide-react";
import type { BackupEntry, StorageConfig } from "@/lib/types";
import { api } from "@/lib/tauri";

interface BackupBrowserProps {
  storage: StorageConfig | null;
  selectedKey: string;
  onSelect: (key: string) => void;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatAge(dateStr: string | null): string {
  if (!dateStr) return "-";
  try {
    const d = new Date(dateStr);
    const diffMs = Date.now() - d.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHrs = Math.floor(diffMins / 60);
    if (diffHrs < 24) return `${diffHrs}h ago`;
    const diffDays = Math.floor(diffHrs / 24);
    return `${diffDays}d ago`;
  } catch {
    return dateStr;
  }
}

export function BackupBrowser({ storage, selectedKey, onSelect }: BackupBrowserProps) {
  const [entries, setEntries] = useState<BackupEntry[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!storage) return;
    setLoading(true);
    api.backup
      .list(
        storage.endpoint ?? "",
        storage.bucket ?? "",
        storage.region ?? "us-east-1",
        storage.access_key ?? "",
        storage.secret_key ?? "",
        storage.prefix ?? ""
      )
      .then(setEntries)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [storage]);

  if (!storage) {
    return (
      <p className="py-8 text-center text-sm text-muted-foreground">
        Select a storage target to browse backups
      </p>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8 text-muted-foreground">
        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
        Loading backups...
      </div>
    );
  }

  const typesKeys = new Set(
    entries.filter((e) => e.key.endsWith(".types.sql")).map((e) => e.key)
  );
  const backups = entries.filter((e) => !e.key.endsWith(".types.sql"));

  function hasTypesCompanion(key: string): boolean {
    const base = key.replace(/\.gz$/, "").replace(/\.(dump|sql|tar)$/, "");
    return typesKeys.has(`${base}.types.sql`);
  }

  if (backups.length === 0) {
    return (
      <p className="py-8 text-center text-sm text-muted-foreground">
        No backups found
      </p>
    );
  }

  return (
    <ScrollArea className="h-[240px] rounded-md border p-2">
      <RadioGroup value={selectedKey} onValueChange={onSelect}>
        {backups.map((entry) => (
          <label
            key={entry.key}
            className={`flex items-center gap-3 rounded-md px-3 py-2 cursor-pointer transition-colors ${
              selectedKey === entry.key ? "bg-accent" : "hover:bg-accent/50"
            }`}
          >
            <RadioGroupItem value={entry.key} />
            <FileArchive className="h-4 w-4 shrink-0 text-muted-foreground" />
            <span className="flex-1 text-sm font-mono truncate">
              {entry.key}
            </span>
            {hasTypesCompanion(entry.key) && (
              <span title="Includes type definitions">
                <FileTypeCorner className="h-4 w-4 shrink-0 text-green-500" />
              </span>
            )}
            <span className="text-xs text-muted-foreground tabular-nums">
              {formatBytes(entry.size)}
            </span>
            <span className="text-xs text-muted-foreground tabular-nums w-16 text-right">
              {formatAge(entry.last_modified)}
            </span>
          </label>
        ))}
      </RadioGroup>
    </ScrollArea>
  );
}
