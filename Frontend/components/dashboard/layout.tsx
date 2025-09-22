"use client"

import type React from "react"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { useWeb3 } from "@/context/web3-context"
import { Loader2 } from "lucide-react"
import { DashboardSidebar } from "@/components/dashboard/sidebar"
import { DashboardHeader } from "@/components/dashboard/header"
import { SidebarProvider, SidebarInset } from "@/components/ui/sidebar"

interface DashboardLayoutProps {
  children: React.ReactNode
}

export default function DashboardLayout({ children }: DashboardLayoutProps) {
  const { account, isLoading } = useWeb3()
  const router = useRouter()

  useEffect(() => {
    if (!account && !isLoading) {
      router.push("/")
    }
  }, [account, isLoading, router])

  if (isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-green-600" />
      </div>
    )
  }

  return (
    <SidebarProvider>
      <DashboardSidebar />
      <SidebarInset className="bg-background">
        <DashboardHeader />
        <main className="flex-1 p-6 md:p-8">{children}</main>
      </SidebarInset>
    </SidebarProvider>
  )
}
