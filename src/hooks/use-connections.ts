import { useState, useEffect, useCallback } from "react";
import type { ConnectionConfig, ConnectionInfo } from "@/lib/types";
import { api } from "@/lib/tauri";

export function useConnections() {
  const [connections, setConnections] = useState<ConnectionConfig[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const list = await api.connections.list();
      setConnections(list);
    } catch (err) {
      console.error("Failed to load connections:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const add = async (conn: ConnectionConfig) => {
    await api.connections.add(conn);
    await refresh();
  };

  const update = async (conn: ConnectionConfig) => {
    await api.connections.update(conn);
    await refresh();
  };

  const remove = async (id: string) => {
    await api.connections.delete(id);
    await refresh();
  };

  const test = async (url: string): Promise<ConnectionInfo> => {
    return api.connections.test(url);
  };

  return { connections, loading, add, update, remove, test, refresh };
}
