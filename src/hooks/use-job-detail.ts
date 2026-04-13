import { useState, useCallback, useEffect, useRef } from "react";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { Job, LogEntry, JobEvent, JobStatusEvent } from "@/lib/types";
import { api } from "@/lib/tauri";

const LOGS_PAGE_SIZE = 200;

export function useJobDetail(jobId: string | null) {
  const [job, setJob] = useState<Job | null>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [logTotal, setLogTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [logsLoading, setLogsLoading] = useState(false);
  const loadedOffsetRef = useRef(0);

  const loadLogs = useCallback(
    async (offset: number) => {
      if (!jobId) return;
      setLogsLoading(true);
      try {
        const resp = await api.jobs.getLogs(jobId, offset, LOGS_PAGE_SIZE);
        setLogs((prev) => (offset === 0 ? resp.logs : [...prev, ...resp.logs]));
        setLogTotal(resp.total);
        loadedOffsetRef.current = offset + resp.logs.length;
      } catch (err) {
        console.error("Failed to load job logs:", err);
      } finally {
        setLogsLoading(false);
      }
    },
    [jobId]
  );

  const load = useCallback(async () => {
    if (!jobId) {
      setJob(null);
      setLogs([]);
      return;
    }
    setLoading(true);
    try {
      const data = await api.jobs.get(jobId);
      setJob(data);
      await loadLogs(0);
    } catch (err) {
      console.error("Failed to load job:", err);
    } finally {
      setLoading(false);
    }
  }, [jobId, loadLogs]);

  const loadMoreLogs = useCallback(() => {
    if (logsLoading || loadedOffsetRef.current >= logTotal) return;
    loadLogs(loadedOffsetRef.current);
  }, [logsLoading, logTotal, loadLogs]);

  useEffect(() => {
    load();

    if (!jobId) return;

    const unlisteners: Promise<UnlistenFn>[] = [];

    unlisteners.push(
      listen<JobEvent>("job:progress", (event) => {
        if (event.payload.job_id !== jobId) return;
        const progressEvent = event.payload.event;

        if ("Progress" in progressEvent) {
          setJob((prev) =>
            prev ? { ...prev, progress: progressEvent } : prev
          );
          return;
        }

        let logMessage = "";
        if ("PhaseStarted" in progressEvent) {
          logMessage = `${progressEvent.PhaseStarted.phase} started`;
        } else if ("PhaseCompleted" in progressEvent) {
          logMessage = `${progressEvent.PhaseCompleted.phase} completed`;
        } else if ("Error" in progressEvent) {
          logMessage = `Error: ${progressEvent.Error.message}`;
        } else if ("Finished" in progressEvent) {
          logMessage = `Finished: ${progressEvent.Finished.message}`;
        }

        setJob((prev) =>
          prev ? { ...prev, progress: progressEvent } : prev
        );
        if (logMessage) {
          const entry: LogEntry = {
            timestamp: new Date().toISOString(),
            message: logMessage,
          };
          setLogs((prev) => [...prev, entry]);
          setLogTotal((prev) => prev + 1);
          loadedOffsetRef.current += 1;
        }
      })
    );

    unlisteners.push(
      listen<JobStatusEvent>("job:status_changed", (event) => {
        if (event.payload.job_id !== jobId) return;
        setJob((prev) =>
          prev ? { ...prev, status: event.payload.status } : prev
        );
      })
    );

    return () => {
      unlisteners.forEach((p) => p.then((fn) => fn()));
    };
  }, [jobId, load]);

  const cancel = useCallback(async () => {
    if (!jobId) return;
    await api.jobs.cancel(jobId);
  }, [jobId]);

  const retry = useCallback(async () => {
    if (!jobId) return;
    return await api.jobs.retry(jobId);
  }, [jobId]);

  return { job, logs, logTotal, loading, logsLoading, loadMoreLogs, cancel, retry, reload: load };
}
