import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Clock } from "lucide-react";

export function BackupHistory() {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base flex items-center gap-2">
          <Clock className="h-4 w-4" />
          Backup & Restore History
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
          <Clock className="h-10 w-10 mb-3 opacity-40" />
          <p className="text-sm font-medium">No history yet</p>
          <p className="text-xs mt-1">
            Operations will appear here after your first backup or restore.
          </p>
        </div>
      </CardContent>
    </Card>
  );
}
