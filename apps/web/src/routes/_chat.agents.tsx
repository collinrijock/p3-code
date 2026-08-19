import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { createFileRoute, Link } from "@tanstack/react-router";
import { BotIcon, CircleAlertIcon, CircleCheckIcon, LoaderCircleIcon } from "lucide-react";
import { useMemo, type ReactNode } from "react";

import { resolveSidebarThreadStatus, type SidebarThreadStatus } from "../components/Sidebar.logic";
import { Badge } from "../components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "../components/ui/empty";
import { SidebarInset } from "../components/ui/sidebar";
import { WorkspacePageHeader } from "../components/workspace/WorkspacePageHeader";
import {
  useAllEnvironmentShellsBootstrapped,
  useProjects,
  useThreadShells,
} from "../state/entities";
import { formatRelativeTimeLabel } from "../timestampFormat";

const STATUS_PRESENTATION: Record<
  SidebarThreadStatus,
  {
    readonly label: string;
    readonly dot: string;
    readonly badge: "error" | "info" | "outline" | "warning";
  }
> = {
  approval: { label: "Approval", dot: "bg-warning", badge: "warning" },
  input: { label: "Needs input", dot: "bg-warning", badge: "warning" },
  working: { label: "Working", dot: "bg-info", badge: "info" },
  monitoring: { label: "Monitoring", dot: "bg-info", badge: "info" },
  failed: { label: "Failed", dot: "bg-destructive", badge: "error" },
  ready: { label: "Ready", dot: "bg-muted-foreground/45", badge: "outline" },
};

function isActiveAgent(thread: EnvironmentThreadShell): boolean {
  const status = resolveSidebarThreadStatus(thread);
  return status !== "ready" || thread.archivedAt === null;
}

function AgentsRouteView() {
  const threads = useThreadShells();
  const projects = useProjects();
  const bootstrapped = useAllEnvironmentShellsBootstrapped();
  const projectNames = useMemo(
    () =>
      new Map(projects.map((project) => [`${project.environmentId}:${project.id}`, project.title])),
    [projects],
  );
  const agents = useMemo(
    () =>
      threads
        .filter(isActiveAgent)
        .toSorted((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt)),
    [threads],
  );
  const statusCounts = useMemo(() => {
    const counts = { working: 0, attention: 0, failed: 0, ready: 0 };
    for (const agent of agents) {
      const status = resolveSidebarThreadStatus(agent);
      if (status === "working" || status === "monitoring") counts.working += 1;
      else if (status === "approval" || status === "input") counts.attention += 1;
      else if (status === "failed") counts.failed += 1;
      else counts.ready += 1;
    }
    return counts;
  }, [agents]);

  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden bg-background text-foreground">
      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <WorkspacePageHeader
          title="Agents"
          description="Live activity across every project and harness"
          actions={
            <Badge variant={statusCounts.working > 0 ? "info" : "outline"}>
              {statusCounts.working} live
            </Badge>
          }
        />
        <main className="min-h-0 flex-1 overflow-y-auto p-4 sm:p-6">
          <div className="mx-auto flex w-full max-w-6xl flex-col gap-5">
            <section className="grid grid-cols-2 gap-3 sm:grid-cols-4" aria-label="Fleet summary">
              <SummaryCard
                label="Working"
                value={statusCounts.working}
                icon={<LoaderCircleIcon className="size-4 text-info" />}
              />
              <SummaryCard
                label="Needs you"
                value={statusCounts.attention}
                icon={<CircleAlertIcon className="size-4 text-warning" />}
              />
              <SummaryCard
                label="Failed"
                value={statusCounts.failed}
                icon={<CircleAlertIcon className="size-4 text-destructive" />}
              />
              <SummaryCard
                label="Ready"
                value={statusCounts.ready}
                icon={<CircleCheckIcon className="size-4 text-muted-foreground" />}
              />
            </section>

            {!bootstrapped ? null : agents.length === 0 ? (
              <Empty className="min-h-80 rounded-2xl border border-dashed border-border">
                <EmptyHeader>
                  <BotIcon className="mx-auto size-6 text-muted-foreground" />
                  <EmptyTitle>No agents yet</EmptyTitle>
                  <EmptyDescription>
                    Start a thread and its harness will appear here.
                  </EmptyDescription>
                </EmptyHeader>
              </Empty>
            ) : (
              <section
                className="grid gap-3 md:grid-cols-2 xl:grid-cols-3"
                aria-label="Agent fleet"
              >
                {agents.map((agent) => {
                  const status = resolveSidebarThreadStatus(agent);
                  const presentation = STATUS_PRESENTATION[status];
                  const projectName =
                    projectNames.get(`${agent.environmentId}:${agent.projectId}`) ??
                    "Unknown project";
                  return (
                    <Link
                      key={`${agent.environmentId}:${agent.id}`}
                      to="/$environmentId/$threadId"
                      params={{ environmentId: agent.environmentId, threadId: agent.id }}
                      className="rounded-2xl outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    >
                      <Card className="h-full transition-colors hover:border-foreground/20 hover:bg-card/80">
                        <CardHeader className="gap-3 pb-3">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <CardTitle className="truncate text-sm">{agent.title}</CardTitle>
                              <p className="mt-1 truncate text-xs text-muted-foreground">
                                {projectName}
                              </p>
                            </div>
                            <Badge variant={presentation.badge}>
                              <span className={`size-1.5 rounded-full ${presentation.dot}`} />
                              {presentation.label}
                            </Badge>
                          </div>
                        </CardHeader>
                        <CardContent className="grid gap-2 pt-0 text-xs text-muted-foreground">
                          <div className="flex items-center justify-between gap-3">
                            <span className="truncate">
                              {agent.session?.providerName ?? agent.modelSelection.instanceId}
                            </span>
                            <span className="shrink-0 tabular-nums">
                              {formatRelativeTimeLabel(agent.updatedAt)}
                            </span>
                          </div>
                          {agent.planProgress ? (
                            <div className="truncate text-foreground/80">
                              {agent.planProgress.step}
                            </div>
                          ) : null}
                          {agent.session?.lastError ? (
                            <div className="line-clamp-2 text-destructive-foreground">
                              {agent.session.lastError}
                            </div>
                          ) : null}
                        </CardContent>
                      </Card>
                    </Link>
                  );
                })}
              </section>
            )}
          </div>
        </main>
      </div>
    </SidebarInset>
  );
}

function SummaryCard({
  label,
  value,
  icon,
}: {
  readonly label: string;
  readonly value: number;
  readonly icon: ReactNode;
}) {
  return (
    <Card className="rounded-xl">
      <CardContent className="flex items-center justify-between gap-3 p-4">
        <div>
          <p className="text-xs text-muted-foreground">{label}</p>
          <p className="mt-1 text-xl font-semibold tabular-nums">{value}</p>
        </div>
        {icon}
      </CardContent>
    </Card>
  );
}

export const Route = createFileRoute("/_chat/agents")({ component: AgentsRouteView });
