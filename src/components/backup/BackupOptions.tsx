import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { StorageConfig } from "@/lib/types";

export interface BackupOptionsState {
  format: string;
  compression: string;
  filenamePrefix: string;
  retention: number;
  keepLocal: boolean;
  noOwner: boolean;
  noAcl: boolean;
  storageId: string;
  streaming: boolean;
}

interface BackupOptionsProps {
  options: BackupOptionsState;
  onChange: (opts: BackupOptionsState) => void;
  storageTargets: StorageConfig[];
}

export function BackupOptions({
  options,
  onChange,
  storageTargets,
}: BackupOptionsProps) {
  const set = (partial: Partial<BackupOptionsState>) =>
    onChange({ ...options, ...partial });

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-3">
        <div className="space-y-1.5">
          <Label className="text-xs">Format</Label>
          <Select value={options.format} onValueChange={(v) => set({ format: v })}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="custom">Custom (.dump)</SelectItem>
              <SelectItem value="plain">Plain SQL (.sql)</SelectItem>
              <SelectItem value="tar">Tar (.tar)</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1.5">
          <Label className="text-xs">Compression</Label>
          <Select
            value={options.compression}
            onValueChange={(v) => set({ compression: v })}
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="gzip-fast">Gzip (Fast)</SelectItem>
              <SelectItem value="gzip">Gzip (Default)</SelectItem>
              <SelectItem value="gzip-best">Gzip (Best)</SelectItem>
              <SelectItem value="none">None</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1.5">
          <Label className="text-xs">Storage Target</Label>
          <Select
            value={options.storageId}
            onValueChange={(v) => set({ storageId: v })}
          >
            <SelectTrigger>
              <SelectValue placeholder="Select storage" />
            </SelectTrigger>
            <SelectContent>
              {storageTargets.map((s) => (
                <SelectItem key={s.id} value={s.id}>
                  {s.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-1.5">
          <Label className="text-xs">Filename Prefix</Label>
          <Input
            value={options.filenamePrefix}
            onChange={(e) => set({ filenamePrefix: e.target.value })}
          />
        </div>
        <div className="space-y-1.5">
          <Label className="text-xs">Retention (backups to keep)</Label>
          <Input
            type="number"
            min={0}
            value={options.retention}
            onChange={(e) => set({ retention: Number(e.target.value) })}
          />
        </div>
      </div>

      <div className="flex items-center gap-6">
        <label className="flex items-center gap-2 text-sm">
          <Switch
            checked={options.noOwner}
            onCheckedChange={(v) => set({ noOwner: v })}
          />
          No Owner
        </label>
        <label className="flex items-center gap-2 text-sm">
          <Switch
            checked={options.noAcl}
            onCheckedChange={(v) => set({ noAcl: v })}
          />
          No ACL
        </label>
        <label className="flex items-center gap-2 text-sm">
          <Switch
            checked={options.keepLocal}
            onCheckedChange={(v) => set({ keepLocal: v })}
          />
          Keep Local Copy
        </label>
        <label className="flex items-center gap-2 text-sm" title="Stream pg_dump directly to S3 without writing a local temp file. Recommended for large databases.">
          <Switch
            checked={options.streaming}
            onCheckedChange={(v) => set({ streaming: v })}
          />
          Stream to Cloud
        </label>
      </div>
    </div>
  );
}
