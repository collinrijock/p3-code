import { createFileRoute } from "@tanstack/react-router";
import { CableIcon, CircleIcon, Rows3Icon, ShieldCheckIcon } from "lucide-react";
import type { ReactNode } from "react";

import { Badge } from "../components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { SidebarInset } from "../components/ui/sidebar";
import { WorkspacePageHeader } from "../components/workspace/WorkspacePageHeader";

function FeadsRouteView() {
  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden bg-background text-foreground">
      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <WorkspacePageHeader
          title="Feads"
          description="Tasks, ownership, and coordination across your agent fleet"
          actions={
            <Badge variant="outline">
              <CircleIcon className="size-2 fill-current" />
              Not connected
            </Badge>
          }
        />
        <main className="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
          <div className="mx-auto grid w-full max-w-5xl gap-4 lg:grid-cols-[minmax(0,1.4fr)_minmax(18rem,0.6fr)]">
            <Card className="min-h-96">
              <CardHeader>
                <div className="mb-3 flex size-10 items-center justify-center rounded-xl border border-border bg-muted/40">
                  <Rows3Icon className="size-5 text-muted-foreground" />
                </div>
                <CardTitle>Connect a Feads workspace</CardTitle>
                <p className="max-w-xl text-sm leading-relaxed text-muted-foreground">
                  This P3 shell is ready for the Feads read model. Once a backend connection is
                  configured, this page will show assigned beads, blockers, mentions, and ownership
                  without leaving the coding workspace.
                </p>
              </CardHeader>
              <CardContent className="grid gap-3 sm:grid-cols-2">
                <Capability
                  icon={<Rows3Icon />}
                  title="Work queue"
                  description="Assigned and available beads, grouped by true state."
                />
                <Capability
                  icon={<CableIcon />}
                  title="Dependencies"
                  description="Blocked work and the upstream bead that unlocks it."
                />
                <Capability
                  icon={<ShieldCheckIcon />}
                  title="Ownership"
                  description="Artifact claims and overlapping agent work."
                />
                <Capability
                  icon={<CircleIcon />}
                  title="Live status"
                  description="Fresh heartbeats, mentions, and session activity."
                />
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">Integration boundary</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4 text-sm text-muted-foreground">
                <p>
                  P3 needs a server-side Feads adapter before this public client can safely read
                  private boards.
                </p>
                <ol className="space-y-3">
                  <IntegrationStep
                    number="1"
                    text="Add a Feads endpoint and credential to the backend environment."
                  />
                  <IntegrationStep
                    number="2"
                    text="Expose a scoped, read-only snapshot contract to the web client."
                  />
                  <IntegrationStep
                    number="3"
                    text="Stream updates into this view and add write actions deliberately."
                  />
                </ol>
              </CardContent>
            </Card>
          </div>
        </main>
      </div>
    </SidebarInset>
  );
}

function Capability({
  icon,
  title,
  description,
}: {
  readonly icon: ReactNode;
  readonly title: string;
  readonly description: string;
}) {
  return (
    <div className="rounded-xl border border-border/70 bg-muted/20 p-4">
      <div className="mb-2 flex size-7 items-center justify-center rounded-lg bg-background text-muted-foreground [&_svg]:size-4">
        {icon}
      </div>
      <h2 className="text-sm font-medium text-foreground">{title}</h2>
      <p className="mt-1 text-xs leading-relaxed">{description}</p>
    </div>
  );
}

function IntegrationStep({ number, text }: { readonly number: string; readonly text: string }) {
  return (
    <li className="flex gap-3">
      <span className="flex size-5 shrink-0 items-center justify-center rounded-full border border-border text-[10px] font-medium text-foreground">
        {number}
      </span>
      <span className="leading-relaxed">{text}</span>
    </li>
  );
}

export const Route = createFileRoute("/_chat/feads")({ component: FeadsRouteView });
