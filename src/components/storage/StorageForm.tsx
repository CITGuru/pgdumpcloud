import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Loader2, CheckCircle2, XCircle } from "lucide-react";
import type { StorageConfig } from "@/lib/types";

interface StorageFormProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSave: (target: StorageConfig) => Promise<void>;
  onTest: (
    endpoint: string,
    bucket: string,
    region: string,
    accessKey: string,
    secretKey: string
  ) => Promise<string>;
}

export function StorageForm({
  open,
  onOpenChange,
  onSave,
  onTest,
}: StorageFormProps) {
  const [name, setName] = useState("");
  const [provider, setProvider] = useState("s3");
  const [endpoint, setEndpoint] = useState("");
  const [bucket, setBucket] = useState("");
  const [region, setRegion] = useState("us-east-1");
  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [prefix, setPrefix] = useState("");
  const [testing, setTesting] = useState(false);
  const [testOk, setTestOk] = useState(false);
  const [testError, setTestError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const handleTest = async () => {
    setTesting(true);
    setTestOk(false);
    setTestError(null);
    try {
      await onTest(endpoint, bucket, region, accessKey, secretKey);
      setTestOk(true);
    } catch (err) {
      setTestError(err instanceof Error ? err.message : String(err));
    } finally {
      setTesting(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      await onSave({
        id: crypto.randomUUID(),
        name,
        provider,
        endpoint: endpoint || undefined,
        bucket: bucket || undefined,
        region: region || undefined,
        access_key: accessKey || undefined,
        secret_key: secretKey || undefined,
        prefix: prefix || undefined,
      });
      onOpenChange(false);
    } catch (err) {
      console.error(err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>New Storage Target</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label>Name</Label>
            <Input
              placeholder="My S3 Bucket"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label>Provider</Label>
            <Select value={provider} onValueChange={setProvider}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="s3">S3-Compatible</SelectItem>
                <SelectItem value="rclone" disabled>
                  rclone (coming soon)
                </SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Endpoint URL</Label>
            <Input
              placeholder="https://s3.us-east-1.amazonaws.com"
              value={endpoint}
              onChange={(e) => setEndpoint(e.target.value)}
              className="font-mono text-sm"
            />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-2">
              <Label>Bucket</Label>
              <Input
                placeholder="pg-backups"
                value={bucket}
                onChange={(e) => setBucket(e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <Label>Region</Label>
              <Input
                placeholder="us-east-1"
                value={region}
                onChange={(e) => setRegion(e.target.value)}
              />
            </div>
          </div>
          <div className="space-y-2">
            <Label>Access Key</Label>
            <Input
              value={accessKey}
              onChange={(e) => setAccessKey(e.target.value)}
              className="font-mono text-sm"
            />
          </div>
          <div className="space-y-2">
            <Label>Secret Key</Label>
            <Input
              type="password"
              value={secretKey}
              onChange={(e) => setSecretKey(e.target.value)}
              className="font-mono text-sm"
            />
          </div>
          <div className="space-y-2">
            <Label>Path Prefix (optional)</Label>
            <Input
              placeholder="backups/prod/"
              value={prefix}
              onChange={(e) => setPrefix(e.target.value)}
            />
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={handleTest}
              disabled={testing || !endpoint || !bucket}
            >
              {testing && <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />}
              Test Connection
            </Button>
            {testOk && (
              <Badge variant="outline" className="gap-1 text-green-600 border-green-600">
                <CheckCircle2 className="h-3 w-3" />
                Connected
              </Badge>
            )}
            {testError && (
              <Badge variant="outline" className="gap-1 text-destructive border-destructive">
                <XCircle className="h-3 w-3" />
                Failed
              </Badge>
            )}
          </div>
          {testError && (
            <p className="text-xs text-destructive">{testError}</p>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={saving || !name || !endpoint || !bucket}>
            {saving && <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />}
            Save
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
