import { useState, useEffect, useCallback } from "react";
import type { StorageConfig } from "@/lib/types";
import { api } from "@/lib/tauri";

export function useStorage() {
  const [targets, setTargets] = useState<StorageConfig[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const list = await api.storage.list();
      setTargets(list);
    } catch (err) {
      console.error("Failed to load storage targets:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const add = async (target: StorageConfig) => {
    await api.storage.add(target);
    await refresh();
  };

  const remove = async (id: string) => {
    await api.storage.delete(id);
    await refresh();
  };

  const test = async (
    endpoint: string,
    bucket: string,
    region: string,
    accessKey: string,
    secretKey: string
  ) => {
    return api.storage.test(endpoint, bucket, region, accessKey, secretKey);
  };

  return { targets, loading, add, remove, test, refresh };
}
