import { ReactNode } from "react";
import { Header } from "./Header";
import { Sidebar } from "./Sidebar";
import type { ConnectionConfig, StorageConfig } from "@/lib/types";

interface MainLayoutProps {
  children: ReactNode;
  theme: "light" | "dark";
  onToggleTheme: () => void;
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

export function MainLayout({
  children,
  theme,
  onToggleTheme,
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
}: MainLayoutProps) {
  return (
    <div className="flex h-screen flex-col bg-background text-foreground">
      <Header theme={theme} onToggleTheme={onToggleTheme} />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar
          connections={connections}
          storageTargets={storageTargets}
          selectedConnectionId={selectedConnectionId}
          onSelectConnection={onSelectConnection}
          onAddConnection={onAddConnection}
          onEditConnection={onEditConnection}
          onDeleteConnection={onDeleteConnection}
          onAddStorage={onAddStorage}
          onDeleteStorage={onDeleteStorage}
          activeJobCount={activeJobCount}
        />
        <main className="flex-1 overflow-auto">{children}</main>
      </div>
    </div>
  );
}
