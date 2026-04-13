import { useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { MainLayout } from "@/components/layout/MainLayout";
import { ConnectionForm } from "@/components/connections/ConnectionForm";
import { StorageForm } from "@/components/storage/StorageForm";
import { BackupPanel } from "@/components/backup/BackupPanel";
import { RestorePanel } from "@/components/restore/RestorePanel";
import { JobsList } from "@/components/jobs/JobsList";
import { JobDetail } from "@/components/jobs/JobDetail";
import { SettingsPanel } from "@/components/settings/SettingsPanel";
import { useTheme } from "@/hooks/use-theme";
import { useConnections } from "@/hooks/use-connections";
import { useStorage } from "@/hooks/use-storage";
import { useJobs } from "@/hooks/use-jobs";
import {
  Database,
  RotateCcw,
  Briefcase,
  Settings,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { ConnectionConfig } from "@/lib/types";
import "./App.css";

export default function App() {
  const { theme, toggle: toggleTheme } = useTheme();
  const {
    connections,
    add: addConnection,
    update: updateConnection,
    remove: removeConnection,
    test: testConnection,
  } = useConnections();
  const {
    targets: storageTargets,
    add: addStorage,
    remove: removeStorage,
    test: testStorage,
  } = useStorage();
  const { jobs, activeJobs, deleteJob, retryJob } = useJobs();

  const [activeTab, setActiveTab] = useState("backup");
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [selectedConnectionId, setSelectedConnectionId] = useState<string | null>(null);
  const [connFormOpen, setConnFormOpen] = useState(false);
  const [connFormInitial, setConnFormInitial] = useState<ConnectionConfig | undefined>();
  const [storageFormOpen, setStorageFormOpen] = useState(false);

  const handleAddConnection = () => {
    setConnFormInitial(undefined);
    setConnFormOpen(true);
  };

  const handleEditConnection = (conn: ConnectionConfig) => {
    setConnFormInitial(conn);
    setConnFormOpen(true);
  };

  const handleSaveConnection = async (conn: ConnectionConfig) => {
    if (connFormInitial) {
      await updateConnection(conn);
    } else {
      await addConnection(conn);
    }
  };

  const handleJobCreated = (jobId: string) => {
    setSelectedJobId(jobId);
    setActiveTab("jobs");
  };

  const handleRetryFromDetail = (newJobId: string) => {
    setSelectedJobId(newJobId);
  };

  return (
    <MainLayout
      theme={theme}
      onToggleTheme={toggleTheme}
      connections={connections}
      storageTargets={storageTargets}
      selectedConnectionId={selectedConnectionId}
      onSelectConnection={setSelectedConnectionId}
      onAddConnection={handleAddConnection}
      onEditConnection={handleEditConnection}
      onDeleteConnection={removeConnection}
      onAddStorage={() => setStorageFormOpen(true)}
      onDeleteStorage={removeStorage}
      activeJobCount={activeJobs.length}
    >
      <div className="p-6">
        <Tabs value={activeTab} onValueChange={(v) => { setActiveTab(v); setSelectedJobId(null); }}>
          <TabsList className="mb-4">
            <TabsTrigger value="backup" className="gap-1.5">
              <Database className="h-3.5 w-3.5" />
              Backup
            </TabsTrigger>
            <TabsTrigger value="restore" className="gap-1.5">
              <RotateCcw className="h-3.5 w-3.5" />
              Restore
            </TabsTrigger>
            <TabsTrigger value="jobs" className="gap-1.5">
              <Briefcase className="h-3.5 w-3.5" />
              Jobs
              {activeJobs.length > 0 && (
                <Badge variant="default" className="ml-1 h-4 min-w-4 px-1 text-[10px] leading-none">
                  {activeJobs.length}
                </Badge>
              )}
            </TabsTrigger>
            <TabsTrigger value="settings" className="gap-1.5">
              <Settings className="h-3.5 w-3.5" />
              Settings
            </TabsTrigger>
          </TabsList>

          <TabsContent value="backup">
            <BackupPanel
              connections={connections}
              storageTargets={storageTargets}
              selectedConnectionId={selectedConnectionId}
              onJobCreated={handleJobCreated}
            />
          </TabsContent>

          <TabsContent value="restore">
            <RestorePanel
              connections={connections}
              storageTargets={storageTargets}
              onJobCreated={handleJobCreated}
            />
          </TabsContent>

          <TabsContent value="jobs">
            {selectedJobId ? (
              <JobDetail
                jobId={selectedJobId}
                onBack={() => setSelectedJobId(null)}
                onRetry={handleRetryFromDetail}
              />
            ) : (
              <JobsList
                jobs={jobs}
                onSelectJob={setSelectedJobId}
                onDeleteJob={deleteJob}
                onRetryJob={async (id) => {
                  const newId = await retryJob(id);
                  setSelectedJobId(newId);
                }}
              />
            )}
          </TabsContent>

          <TabsContent value="settings">
            <SettingsPanel theme={theme} onToggleTheme={toggleTheme} />
          </TabsContent>
        </Tabs>
      </div>

      <ConnectionForm
        open={connFormOpen}
        onOpenChange={setConnFormOpen}
        initial={connFormInitial}
        onSave={handleSaveConnection}
        onTest={testConnection}
      />

      <StorageForm
        open={storageFormOpen}
        onOpenChange={setStorageFormOpen}
        onSave={addStorage}
        onTest={testStorage}
      />
    </MainLayout>
  );
}
