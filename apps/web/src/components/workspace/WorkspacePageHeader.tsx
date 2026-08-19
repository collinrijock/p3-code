import type { ReactNode } from "react";

import { cn } from "~/lib/utils";
import { COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS } from "~/workspaceTitlebar";

export function WorkspacePageHeader({
  title,
  description,
  actions,
}: {
  readonly title: string;
  readonly description: string;
  readonly actions?: ReactNode;
}) {
  return (
    <header
      className={cn(
        "flex min-h-[var(--workspace-topbar-height)] shrink-0 items-center justify-between gap-4 border-b border-border px-4 py-2 transition-[padding-left] duration-200 ease-linear motion-reduce:transition-none sm:px-6",
        COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS,
      )}
    >
      <div className="min-w-0">
        <h1 className="truncate text-sm font-semibold text-foreground">{title}</h1>
        <p className="hidden truncate text-xs text-muted-foreground sm:block">{description}</p>
      </div>
      {actions}
    </header>
  );
}
