import { useState, useCallback, useEffect, useMemo } from "react";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type {
  BackupRequest,
  RestoreRequest,
  JobSummary,
  JobStatusEvent,
  JobEvent,
} from "@/lib/types";
import { api } from "@/lib/tauri";

export function useJobs() {
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState(true);

  const reload = useCallback(async () => {
    try {
      const list = await api.jobs.list();
      setJobs(list);
    } catch (err) {
      console.error("Failed to load jobs:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    reload();

    const unlisteners: Promise<UnlistenFn>[] = [];

    unlisteners.push(
      listen<JobStatusEvent>("job:status_changed", (event) => {
        setJobs((prev) => {
          const idx = prev.findIndex((j) => j.id === event.payload.job_id);
          if (idx === -1) {
            reload();
            return prev;
          }
          const updated = [...prev];
          updated[idx] = { ...updated[idx], status: event.payload.status };
          return updated;
        });
      })
    );

    unlisteners.push(
      listen<JobEvent>("job:progress", (event) => {
        setJobs((prev) =>
          prev.map((j) => {
            if (j.id !== event.payload.job_id) return j;
            if (j.status === "Cancelled" || j.status === "Failed" || j.status === "Completed") return j;
            return { ...j, status: "Running" };
          })
        );
      })
    );

    return () => {
      unlisteners.forEach((p) => p.then((fn) => fn()));
    };
  }, [reload]);

  const createBackup = useCallback(
    async (request: BackupRequest) => {
      const jobId = await api.jobs.createBackup(request);
      await reload();
      return jobId;
    },
    [reload]
  );

  const createRestore = useCallback(
    async (request: RestoreRequest) => {
      const jobId = await api.jobs.createRestore(request);
      await reload();
      return jobId;
    },
    [reload]
  );

  const cancelJob = useCallback(async (id: string) => {
    await api.jobs.cancel(id);
  }, []);

  const deleteJob = useCallback(
    async (id: string) => {
      await api.jobs.delete(id);
      await reload();
    },
    [reload]
  );

  const retryJob = useCallback(
    async (id: string) => {
      const newId = await api.jobs.retry(id);
      await reload();
      return newId;
    },
    [reload]
  );

  const activeJobs = useMemo(
    () => jobs.filter((j) => j.status === "Running" || j.status === "Queued"),
    [jobs]
  );

  return {
    jobs,
    loading,
    activeJobs,
    createBackup,
    createRestore,
    cancelJob,
    deleteJob,
    retryJob,
    reload,
  };
}
