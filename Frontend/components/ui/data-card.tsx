import type React from "react"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { cn } from "@/lib/utils"

interface DataCardProps {
  title: string
  value: string | number
  description?: string
  icon?: React.ReactNode
  trend?: {
    value: number
    label: string
  }
  footer?: React.ReactNode
  className?: string
}

export function DataCard({ title, value, description, icon, trend, footer, className }: DataCardProps) {
  return (
    <Card className={cn("overflow-hidden", className)}>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <div>
          <CardTitle className="text-sm font-medium">{title}</CardTitle>
          {description && <CardDescription>{description}</CardDescription>}
        </div>
        {icon && <div className="text-muted-foreground">{icon}</div>}
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{value}</div>
        {trend && (
          <p
            className={cn(
              "text-xs",
              trend.value > 0 ? "text-green-500" : trend.value < 0 ? "text-red-500" : "text-muted-foreground",
            )}
          >
            {trend.value > 0 ? "+" : ""}
            {trend.value}% {trend.label}
          </p>
        )}
      </CardContent>
      {footer && <CardFooter className="border-t px-4 py-3">{footer}</CardFooter>}
    </Card>
  )
}
