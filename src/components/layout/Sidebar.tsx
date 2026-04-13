import {
  Database,
  HardDrive,
  Plus,
  Trash2,
  Pencil,
  Loader2,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import type { ConnectionConfig, StorageConfig } from "@/lib/types";

interface SidebarProps {
  connections: ConnectionConfig[];
  storageTargets: StorageConfig[];
  selectedConnectionId: string | null;
  onSelectConnection: (id: string) => void;
  onAddConnection: () => void;
  onEditConnection: (conn: ConnectionConfig) => void;
  onDeleteConnection: (id: string) => void;
  onAddStorage: () => void;
  onDeleteStorage: (id: string) => void;
  activeJobCount?: number;
}

export function Sidebar({
  connections,
  storageTargets,
  selectedConnectionId,
  onSelectConnection,
  onAddConnection,
  onEditConnection,
  onDeleteConnection,
  onAddStorage,
  onDeleteStorage,
  activeJobCount = 0,
}: SidebarProps) {
  return (
    <aside className="flex w-64 flex-col border-r border-border bg-card overflow-hidden">
      <div className="flex items-center justify-between px-4 pt-4 pb-1 shrink-0">
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
          Connections
        </h2>
        <Button variant="ghost" size="icon" className="h-6 w-6 shrink-0" onClick={onAddConnection}>
          <Plus className="h-3.5 w-3.5" />
        </Button>
      </div>

      <ScrollArea className="flex-1 min-h-0">
        <div className="px-4 pb-2">
          <div className="space-y-1">
            {connections.map((conn) => (
              <div
                key={conn.id}
                className={`group flex items-start gap-2 rounded-md px-2 py-1.5 text-sm cursor-pointer transition-colors ${
                  selectedConnectionId === conn.id
                    ? "bg-accent text-accent-foreground"
                    : "hover:bg-accent/50"
                }`}
                onClick={() => onSelectConnection(conn.id)}
              >
                <Database className="h-3.5 w-3.5 shrink-0 mt-0.5 text-muted-foreground" />
                <div className="min-w-0 flex-1">
                  <span className="block truncate">{conn.name}</span>
                  <span className="block truncate text-[10px] text-muted-foreground leading-tight">
                    {conn.host}:{conn.port}/{conn.database}
                  </span>
                </div>
                <div className="hidden group-hover:flex gap-0.5 shrink-0">
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-5 w-5"
                    onClick={(e) => {
                      e.stopPropagation();
                      onEditConnection(conn);
                    }}
                  >
                    <Pencil className="h-3 w-3" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-5 w-5 text-destructive"
                    onClick={(e) => {
                      e.stopPropagation();
                      onDeleteConnection(conn.id);
                    }}
                  >
                    <Trash2 className="h-3 w-3" />
                  </Button>
                </div>
              </div>
            ))}
            {connections.length === 0 && (
              <p className="text-xs text-muted-foreground px-2 py-1">
                No connections yet
              </p>
            )}
          </div>

        </div>
      </ScrollArea>

      <Separator className="shrink-0" />

      <div className="flex items-center justify-between px-4 pt-3 pb-1 shrink-0">
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
          Storage
        </h2>
        <Button variant="ghost" size="icon" className="h-6 w-6 shrink-0" onClick={onAddStorage}>
          <Plus className="h-3.5 w-3.5" />
        </Button>
      </div>

      <ScrollArea className="flex-1 min-h-0">
        <div className="px-4 pb-4">
          <div className="space-y-1">
            {storageTargets.map((target) => (
              <div
                key={target.id}
                className="group flex items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-accent/50 transition-colors"
              >
                <HardDrive className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                <span className="truncate min-w-0 flex-1">{target.name}</span>
                <Button
                  variant="ghost"
                  size="icon"
                  className="hidden group-hover:flex h-5 w-5 shrink-0 text-destructive"
                  onClick={() => onDeleteStorage(target.id)}
                >
                  <Trash2 className="h-3 w-3" />
                </Button>
              </div>
            ))}
            {storageTargets.length === 0 && (
              <p className="text-xs text-muted-foreground px-2 py-1">
                No storage targets yet
              </p>
            )}
          </div>
        </div>
      </ScrollArea>

      {activeJobCount > 0 && (
        <>
          <Separator className="shrink-0" />
          <div className="px-4 py-3 shrink-0">
            <div className="flex items-center gap-2 text-sm">
              <Loader2 className="h-3.5 w-3.5 animate-spin text-blue-500" />
              <span className="text-xs text-muted-foreground">
                {activeJobCount} job{activeJobCount !== 1 ? "s" : ""} running
              </span>
            </div>
          </div>
        </>
      )}
    </aside>
  );
}
