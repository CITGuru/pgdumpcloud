import { useState, useCallback, useEffect } from "react";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { BackupRequest, ProgressEvent } from "@/lib/types";
import { api } from "@/lib/tauri";

export function useBackup() {
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState<ProgressEvent | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let unlisten: UnlistenFn | null = null;

    listen<ProgressEvent>("backup:progress", (event) => {
      setProgress(event.payload);
    }).then((fn) => {
      unlisten = fn;
    });

    return () => {
      unlisten?.();
    };
  }, []);

  const run = useCallback(async (request: BackupRequest) => {
    setRunning(true);
    setError(null);
    setProgress(null);
    try {
      const result = await api.backup.run(request);
      return result;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      throw err;
    } finally {
      setRunning(false);
    }
  }, []);

  return { running, progress, error, run };
}
