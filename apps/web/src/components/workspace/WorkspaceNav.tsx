import { Link, useLocation } from "@tanstack/react-router";
import { BotIcon, Rows3Icon } from "lucide-react";

import { cn } from "~/lib/utils";
import { SidebarMenuButton, useSidebar } from "~/components/ui/sidebar";

const WORKSPACE_VIEWS = [
  { to: "/agents", label: "Agents", Icon: BotIcon },
  { to: "/feads", label: "Feads", Icon: Rows3Icon },
] as const;

export function WorkspaceNav() {
  const pathname = useLocation({ select: (location) => location.pathname });
  const { isMobile, setOpenMobile } = useSidebar();

  return (
    <nav aria-label="Workspace views" className="grid grid-cols-2 gap-1">
      {WORKSPACE_VIEWS.map(({ to, label, Icon }) => {
        const active = pathname === to || pathname.startsWith(`${to}/`);
        return (
          <SidebarMenuButton
            key={to}
            render={<Link to={to} onClick={() => isMobile && setOpenMobile(false)} />}
            className={cn(
              "justify-center gap-1.5 focus-visible:ring-offset-2 focus-visible:ring-offset-sidebar",
              active
                ? "bg-sidebar-row-active text-sidebar-foreground font-medium"
                : "text-sidebar-muted-foreground hover:bg-sidebar-row-hover hover:text-sidebar-foreground",
            )}
            aria-current={active ? "page" : undefined}
          >
            <Icon className="size-4" />
            <span>{label}</span>
          </SidebarMenuButton>
        );
      })}
    </nav>
  );
}
