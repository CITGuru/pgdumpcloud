import { useEffect, useRef, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import {
  ArrowLeft,
  Database,
  RotateCcw,
  RefreshCw,
  Ban,
  Loader2,
  CheckCircle2,
  XCircle,
  Clock,
  Terminal,
} from "lucide-react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { ProgressEvent, JobStatus } from "@/lib/types";
import { useJobDetail } from "@/hooks/use-job-detail";

interface JobDetailProps {
  jobId: string;
  onBack: () => void;
  onRetry: (newJobId: string) => void;
}

const PHASE_LABELS: Record<string, string> = {
  Dumping: "Dumping database",
  Compressing: "Compressing",
  Uploading: "Uploading",
  Downloading: "Downloading",
  Decompressing: "Decompressing",
  Restoring: "Restoring",
  StreamingUpload: "Streaming to cloud",
  Exporting: "Exporting to Parquet",
};

function parseProgress(event: ProgressEvent | null) {
  if (!event) return { percent: 0, phase: "", detail: "", status: "running" as const, indeterminate: false };

  if ("PhaseStarted" in event) {
    const label = PHASE_LABELS[event.PhaseStarted.phase] ?? event.PhaseStarted.phase;
    return { percent: 0, phase: label, detail: "", status: "running" as const, indeterminate: false };
  }
  if ("Progress" in event) {
    const { phase, bytes, total } = event.Progress;
    const label = PHASE_LABELS[phase] ?? phase;
    if (total) {
      const percent = Math.round((bytes / total) * 100);
      const detail = `${formatBytes(bytes)} / ${formatBytes(total)}`;
      return { percent, phase: label, detail, status: "running" as const, indeterminate: false };
    }
    if (phase === "Restoring") {
      const detail = `${bytes} objects processed`;
      return { percent: 0, phase: label, detail, status: "running" as const, indeterminate: true };
    }
    return { percent: 0, phase: label, detail: `${formatBytes(bytes)} uploaded`, status: "running" as const, indeterminate: true };
  }
  if ("TableProgress" in event) {
    const { schema, table, index, total_tables } = event.TableProgress;
    const percent = total_tables > 0 ? Math.round(((index + 1) / total_tables) * 100) : 0;
    return {
      percent,
      phase: "Exporting to Parquet",
      detail: `Table ${index + 1}/${total_tables}: ${schema}.${table}`,
      status: "running" as const,
      indeterminate: false,
    };
  }
  if ("PhaseCompleted" in event) {
    const label = PHASE_LABELS[event.PhaseCompleted.phase] ?? event.PhaseCompleted.phase;
    return { percent: 100, phase: label, detail: "", status: "running" as const, indeterminate: false };
  }
  if ("Error" in event) {
    return { percent: 0, phase: "", detail: event.Error.message, status: "error" as const, indeterminate: false };
  }
  if ("Finished" in event) {
    return { percent: 100, phase: "", detail: event.Finished.message, status: "done" as const, indeterminate: false };
  }
  return { percent: 0, phase: "", detail: "", status: "running" as const, indeterminate: false };
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatTime(iso: string | null): string {
  if (!iso) return "-";
  return new Date(iso).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatDuration(startedAt: string | null, finishedAt: string | null): string {
  if (!startedAt) return "-";
  const start = new Date(startedAt).getTime();
  const end = finishedAt ? new Date(finishedAt).getTime() : Date.now();
  const seconds = Math.round((end - start) / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${minutes}m ${secs}s`;
}

const timeFormatter = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

const STATUS_BADGE: Record<JobStatus, { variant: "default" | "secondary" | "destructive" | "outline"; icon: React.ReactNode }> = {
  Queued: { variant: "secondary", icon: <Clock className="h-3 w-3" /> },
  Running: { variant: "default", icon: <Loader2 className="h-3 w-3 animate-spin" /> },
  Completed: { variant: "secondary", icon: <CheckCircle2 className="h-3 w-3 text-green-500" /> },
  Failed: { variant: "destructive", icon: <XCircle className="h-3 w-3" /> },
  Cancelled: { variant: "outline", icon: <Ban className="h-3 w-3" /> },
};

export function JobDetail({ jobId, onBack, onRetry }: JobDetailProps) {
  const { job, logs, logTotal, loading, logsLoading, loadMoreLogs, cancel, retry } = useJobDetail(jobId);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const shouldAutoScroll = useRef(true);
  const prevLogCount = useRef(0);

  const virtualizer = useVirtualizer({
    count: logs.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: () => 24,
    overscan: 20,
  });

  const handleScroll = useCallback(() => {
    const el = scrollContainerRef.current;
    if (!el) return;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    shouldAutoScroll.current = distanceFromBottom < 50;
  }, []);

  useEffect(() => {
    if (logs.length > prevLogCount.current && shouldAutoScroll.current) {
      virtualizer.scrollToIndex(logs.length - 1, { align: "end" });
    }
    prevLogCount.current = logs.length;
  }, [logs.length, virtualizer]);

  if (loading || !job) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center py-12">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </CardContent>
      </Card>
    );
  }

  const prog = parseProgress(job.progress);
  const isActive = job.status === "Running" || job.status === "Queued";
  const badge = STATUS_BADGE[job.status];

  const handleRetry = async () => {
    const newId = await retry();
    if (newId) onRetry(newId);
  };

  const hasMoreLogs = logs.length < logTotal;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onBack}>
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex items-center gap-2">
          {job.kind === "Backup" ? (
            <Database className="h-4 w-4 text-muted-foreground" />
          ) : (
            <RotateCcw className="h-4 w-4 text-muted-foreground" />
          )}
          <span className="text-sm font-medium">{job.kind} Job</span>
        </div>
        <Badge variant={badge.variant} className="gap-1">
          {badge.icon}
          {job.status}
        </Badge>
        <div className="flex-1" />
        {isActive && (
          <Button variant="outline" size="sm" className="gap-1.5" onClick={cancel}>
            <Ban className="h-3.5 w-3.5" />
            Cancel
          </Button>
        )}
        {job.status === "Failed" && (
          <Button variant="outline" size="sm" className="gap-1.5" onClick={handleRetry}>
            <RefreshCw className="h-3.5 w-3.5" />
            Retry
          </Button>
        )}
      </div>

      <Card>
        <CardContent className="pt-4">
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div>
              <p className="text-xs text-muted-foreground mb-0.5">Created</p>
              <p className="tabular-nums">{formatTime(job.created_at)}</p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground mb-0.5">Started</p>
              <p className="tabular-nums">{formatTime(job.started_at)}</p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground mb-0.5">Duration</p>
              <p className="tabular-nums">
                {formatDuration(job.started_at, job.finished_at)}
              </p>
            </div>
          </div>
          {job.result && (
            <div className="mt-3 pt-3 border-t border-border">
              <p className="text-xs text-muted-foreground mb-0.5">Result</p>
              <p className="text-sm font-mono">{job.result}</p>
            </div>
          )}
          {job.error && (
            <div className="mt-3 pt-3 border-t border-border">
              <p className="text-xs text-destructive mb-0.5">Error</p>
              <p className="text-sm text-destructive font-mono">{job.error}</p>
            </div>
          )}
        </CardContent>
      </Card>

      {isActive && (
        <Card>
          <CardContent className="pt-4 space-y-2">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin text-primary" />
                <span className="text-sm font-medium">
                  {prog.phase || "Preparing..."}
                </span>
              </div>
              {!prog.indeterminate && prog.percent > 0 && (
                <Badge variant="secondary" className="text-xs tabular-nums">
                  {prog.percent}%
                </Badge>
              )}
            </div>
            {prog.indeterminate ? (
              <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
                <div className="h-full w-1/3 rounded-full bg-primary animate-pulse" />
              </div>
            ) : (
              <Progress value={prog.percent} className="h-2" />
            )}
            {prog.detail && (
              <p className="text-xs text-muted-foreground">{prog.detail}</p>
            )}
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm flex items-center gap-2">
            <Terminal className="h-3.5 w-3.5" />
            Logs
            {logTotal > 0 && (
              <span className="text-xs font-normal text-muted-foreground">
                ({logs.length}{hasMoreLogs ? ` of ${logTotal}` : ""})
              </span>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div
            ref={scrollContainerRef}
            onScroll={handleScroll}
            className="h-[500px] overflow-auto rounded-md border border-border bg-muted/30 p-3"
          >
            {logs.length === 0 && !logsLoading ? (
              <p className="text-xs text-muted-foreground">No log entries yet.</p>
            ) : (
              <>
                {hasMoreLogs && (
                  <div className="flex justify-center pb-2">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-xs"
                      onClick={loadMoreLogs}
                      disabled={logsLoading}
                    >
                      {logsLoading ? (
                        <Loader2 className="h-3 w-3 animate-spin mr-1" />
                      ) : null}
                      Load more logs
                    </Button>
                  </div>
                )}
                <div
                  className="relative font-mono text-xs"
                  style={{ height: `${virtualizer.getTotalSize()}px` }}
                >
                  {virtualizer.getVirtualItems().map((virtualRow) => {
                    const entry = logs[virtualRow.index];
                    const time = timeFormatter.format(new Date(entry.timestamp));
                    return (
                      <div
                        key={virtualRow.index}
                        className="absolute top-0 left-0 w-full flex gap-2"
                        style={{
                          height: `${virtualRow.size}px`,
                          transform: `translateY(${virtualRow.start}px)`,
                        }}
                      >
                        <span className="text-muted-foreground shrink-0 tabular-nums">
                          {time}
                        </span>
                        <span
                          className={
                            entry.message.startsWith("Error")
                              ? "text-destructive"
                              : entry.message.startsWith("Finished")
                                ? "text-green-600 dark:text-green-400"
                                : ""
                          }
                        >
                          {entry.message}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
