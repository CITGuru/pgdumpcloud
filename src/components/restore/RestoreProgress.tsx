import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { CheckCircle2, XCircle, Loader2 } from "lucide-react";
import type { ProgressEvent } from "@/lib/types";

interface RestoreProgressProps {
  event: ProgressEvent | null;
  running: boolean;
}

function parseEvent(event: ProgressEvent) {
  if ("PhaseStarted" in event) return { type: "started" as const, phase: event.PhaseStarted.phase };
  if ("Progress" in event) return { type: "progress" as const, ...event.Progress };
  if ("PhaseCompleted" in event) return { type: "completed" as const, phase: event.PhaseCompleted.phase };
  if ("Error" in event) return { type: "error" as const, message: event.Error.message };
  if ("Finished" in event) return { type: "finished" as const, message: event.Finished.message };
  return { type: "unknown" as const };
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function RestoreProgress({ event, running }: RestoreProgressProps) {
  if (!event && !running) return null;

  const parsed = event ? parseEvent(event) : null;

  let percent = 0;
  let phase = "";
  let status: "running" | "done" | "error" = "running";
  let detail = "";

  if (parsed) {
    switch (parsed.type) {
      case "started":
        phase = parsed.phase;
        break;
      case "progress":
        phase = parsed.phase;
        if (parsed.total) {
          percent = Math.round((parsed.bytes / parsed.total) * 100);
          detail = `${formatBytes(parsed.bytes)} / ${formatBytes(parsed.total)}`;
        } else {
          detail = formatBytes(parsed.bytes);
        }
        break;
      case "completed":
        phase = parsed.phase;
        percent = 100;
        break;
      case "error":
        status = "error";
        detail = parsed.message;
        break;
      case "finished":
        status = "done";
        detail = parsed.message;
        percent = 100;
        break;
    }
  }

  return (
    <div className="rounded-lg border border-border p-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          {status === "running" && <Loader2 className="h-4 w-4 animate-spin text-primary" />}
          {status === "done" && <CheckCircle2 className="h-4 w-4 text-green-500" />}
          {status === "error" && <XCircle className="h-4 w-4 text-destructive" />}
          <span className="text-sm font-medium">
            {status === "done" ? "Restore Complete" : status === "error" ? "Error" : phase || "Preparing..."}
          </span>
        </div>
        {percent > 0 && (
          <Badge variant="secondary" className="text-xs tabular-nums">
            {percent}%
          </Badge>
        )}
      </div>
      {status === "running" && <Progress value={percent} className="h-2" />}
      {detail && (
        <p className={`text-xs ${status === "error" ? "text-destructive" : "text-muted-foreground"}`}>
          {detail}
        </p>
      )}
    </div>
  );
}
