import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

type StatusType = "success" | "warning" | "error" | "info" | "pending"

interface StatusBadgeProps {
  status: StatusType
  text: string
  className?: string
}

export function StatusBadge({ status, text, className }: StatusBadgeProps) {
  return (
    <Badge
      className={cn(
        "px-2 py-1 text-xs font-medium",
        status === "success" && "bg-green-100 text-green-800 hover:bg-green-100",
        status === "warning" && "bg-yellow-100 text-yellow-800 hover:bg-yellow-100",
        status === "error" && "bg-red-100 text-red-800 hover:bg-red-100",
        status === "info" && "bg-blue-100 text-blue-800 hover:bg-blue-100",
        status === "pending" && "bg-gray-100 text-gray-800 hover:bg-gray-100",
        className,
      )}
    >
      {text}
    </Badge>
  )
}
