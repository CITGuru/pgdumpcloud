import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Briefcase,
  Database,
  RotateCcw,
  Trash2,
  RefreshCw,
  Loader2,
  CheckCircle2,
  XCircle,
  Clock,
  Ban,
  ChevronRight,
} from "lucide-react";
import type { JobSummary, JobStatus } from "@/lib/types";

type FilterStatus = "all" | JobStatus;

interface JobsListProps {
  jobs: JobSummary[];
  onSelectJob: (id: string) => void;
  onDeleteJob: (id: string) => void;
  onRetryJob: (id: string) => void;
}

const STATUS_ICON: Record<JobStatus, React.ReactNode> = {
  Queued: <Clock className="h-3.5 w-3.5 text-muted-foreground" />,
  Running: <Loader2 className="h-3.5 w-3.5 animate-spin text-blue-500" />,
  Completed: <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />,
  Failed: <XCircle className="h-3.5 w-3.5 text-destructive" />,
  Cancelled: <Ban className="h-3.5 w-3.5 text-muted-foreground" />,
};

const STATUS_VARIANT: Record<
  JobStatus,
  "default" | "secondary" | "destructive" | "outline"
> = {
  Queued: "secondary",
  Running: "default",
  Completed: "secondary",
  Failed: "destructive",
  Cancelled: "outline",
};

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

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

const FILTERS: { label: string; value: FilterStatus }[] = [
  { label: "All", value: "all" },
  { label: "Running", value: "Running" },
  { label: "Completed", value: "Completed" },
  { label: "Failed", value: "Failed" },
];

export function JobsList({
  jobs,
  onSelectJob,
  onDeleteJob,
  onRetryJob,
}: JobsListProps) {
  const [filter, setFilter] = useState<FilterStatus>("all");

  const filtered =
    filter === "all" ? jobs : jobs.filter((j) => j.status === filter);

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base flex items-center gap-2">
            <Briefcase className="h-4 w-4" />
            Jobs
          </CardTitle>
          <div className="flex gap-1">
            {FILTERS.map((f) => (
              <Button
                key={f.value}
                variant={filter === f.value ? "default" : "ghost"}
                size="sm"
                className="h-7 px-2.5 text-xs"
                onClick={() => setFilter(f.value)}
              >
                {f.label}
                {f.value !== "all" && (
                  <span className="ml-1 tabular-nums">
                    {jobs.filter((j) => j.status === f.value).length}
                  </span>
                )}
              </Button>
            ))}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {filtered.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
            <Briefcase className="h-10 w-10 mb-3 opacity-40" />
            <p className="text-sm font-medium">
              {filter === "all" ? "No jobs yet" : `No ${filter.toLowerCase()} jobs`}
            </p>
            <p className="text-xs mt-1">
              Start a backup or restore to create a job.
            </p>
          </div>
        ) : (
          <ScrollArea className="max-h-[60vh]">
            <div className="space-y-1">
              {filtered.map((job) => (
                <div
                  key={job.id}
                  className="group flex items-center gap-3 rounded-lg border border-border px-3 py-2.5 cursor-pointer hover:bg-accent/50 transition-colors"
                  onClick={() => onSelectJob(job.id)}
                >
                  <div className="shrink-0">{STATUS_ICON[job.status]}</div>

                  <div className="flex items-center gap-1.5 shrink-0">
                    {job.kind === "Backup" ? (
                      <Database className="h-3.5 w-3.5 text-muted-foreground" />
                    ) : (
                      <RotateCcw className="h-3.5 w-3.5 text-muted-foreground" />
                    )}
                    <span className="text-xs font-medium text-muted-foreground uppercase">
                      {job.kind}
                    </span>
                  </div>

                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <Badge variant={STATUS_VARIANT[job.status]} className="text-[10px] px-1.5 py-0">
                        {job.status}
                      </Badge>
                      {job.result && job.status === "Completed" && (
                        <span className="text-xs text-muted-foreground truncate max-w-[200px]">
                          {job.result}
                        </span>
                      )}
                      {job.error && (
                        <span className="text-xs text-destructive truncate max-w-[200px]">
                          {job.error}
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="flex items-center gap-2 shrink-0 text-xs text-muted-foreground tabular-nums">
                    <span>{formatTime(job.created_at)}</span>
                    <span className="text-[10px]">
                      {formatDuration(job.started_at, job.finished_at)}
                    </span>
                  </div>

                  <div className="flex items-center gap-0.5 shrink-0 opacity-0 group-hover:opacity-100 transition-opacity">
                    {job.status === "Failed" && (
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6"
                        onClick={(e) => {
                          e.stopPropagation();
                          onRetryJob(job.id);
                        }}
                      >
                        <RefreshCw className="h-3 w-3" />
                      </Button>
                    )}
                    {job.status !== "Running" && job.status !== "Queued" && (
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6 text-destructive"
                        onClick={(e) => {
                          e.stopPropagation();
                          onDeleteJob(job.id);
                        }}
                      >
                        <Trash2 className="h-3 w-3" />
                      </Button>
                    )}
                    <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
                  </div>
                </div>
              ))}
            </div>
          </ScrollArea>
        )}
      </CardContent>
    </Card>
  );
}
