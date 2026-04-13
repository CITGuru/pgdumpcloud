import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Settings } from "lucide-react";

interface SettingsPanelProps {
  theme: "light" | "dark";
  onToggleTheme: () => void;
}

export function SettingsPanel({ theme, onToggleTheme }: SettingsPanelProps) {
  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Settings className="h-4 w-4" />
            General
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <Label className="text-xs">Default Dump Format</Label>
              <Select defaultValue="custom">
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
              <Label className="text-xs">Default Compression</Label>
              <Select defaultValue="gzip">
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="gzip">Gzip</SelectItem>
                  <SelectItem value="none">None</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <Label className="text-xs">Default Filename Prefix</Label>
              <Input defaultValue="backup" />
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Default Retention</Label>
              <Input type="number" defaultValue={7} min={0} />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Appearance</CardTitle>
        </CardHeader>
        <CardContent>
          <label className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium">Dark Mode</p>
              <p className="text-xs text-muted-foreground">
                Toggle between light and dark theme
              </p>
            </div>
            <Switch checked={theme === "dark"} onCheckedChange={onToggleTheme} />
          </label>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Advanced</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-1.5">
            <Label className="text-xs">pg_dump Path (leave empty for PATH)</Label>
            <Input placeholder="/usr/bin/pg_dump" className="font-mono text-sm" />
          </div>
          <div className="space-y-1.5">
            <Label className="text-xs">pg_restore Path</Label>
            <Input placeholder="/usr/bin/pg_restore" className="font-mono text-sm" />
          </div>
          <div className="space-y-1.5">
            <Label className="text-xs">rclone Path</Label>
            <Input placeholder="/usr/bin/rclone" className="font-mono text-sm" />
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
