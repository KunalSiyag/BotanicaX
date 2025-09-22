"use client"

import { useState } from "react"
import { usePathname } from "next/navigation"
import { useWeb3 } from "@/context/web3-context"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { Badge } from "@/components/ui/badge"
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Bell, Search, User, Settings, LogOut, HelpCircle } from "lucide-react"
import { SidebarTrigger } from "@/components/ui/sidebar"

export function DashboardHeader() {
  const { account, disconnectWallet } = useWeb3()
  const pathname = usePathname()
  const [searchQuery, setSearchQuery] = useState("")

  const getPageTitle = (path: string) => {
    const segments = path.split("/").filter(Boolean)
    if (segments.length === 0) return "Dashboard"

    const lastSegment = segments[segments.length - 1]
    return lastSegment.charAt(0).toUpperCase() + lastSegment.slice(1)
  }

  const getBreadcrumbs = (path: string) => {
    const segments = path.split("/").filter(Boolean)
    const breadcrumbs = []

    if (segments.length === 0) {
      breadcrumbs.push({ name: "Dashboard", path: "/dashboard", isCurrentPage: true })
    } else {
      breadcrumbs.push({ name: "Dashboard", path: "/dashboard", isCurrentPage: false })

      let currentPath = ""
      for (let i = 0; i < segments.length; i++) {
        currentPath += `/${segments[i]}`
        const isCurrentPage = i === segments.length - 1

        breadcrumbs.push({
          name: segments[i].charAt(0).toUpperCase() + segments[i].slice(1),
          path: currentPath,
          isCurrentPage,
        })
      }
    }

    return breadcrumbs
  }

  const breadcrumbs = getBreadcrumbs(pathname)

  const truncateAddress = (address: string) => {
    if (!address) return ""
    return `${address.slice(0, 6)}...${address.slice(-4)}`
  }

  return (
    <header className="sticky top-0 z-30 flex h-16 items-center gap-4 border-b bg-background/95 px-6 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <SidebarTrigger className="md:hidden" />

      <div className="flex flex-1 items-center justify-between">
        <Breadcrumb>
          <BreadcrumbList>
            {breadcrumbs.map((crumb, index) => (
              <BreadcrumbItem key={crumb.path}>
                {!crumb.isCurrentPage ? (
                  <>
                    <BreadcrumbLink href={crumb.path}>{crumb.name}</BreadcrumbLink>
                    <BreadcrumbSeparator />
                  </>
                ) : (
                  <BreadcrumbPage>{crumb.name}</BreadcrumbPage>
                )}
              </BreadcrumbItem>
            ))}
          </BreadcrumbList>
        </Breadcrumb>

        <div className="flex items-center gap-4">
          <div className="relative hidden md:block">
            <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              type="search"
              placeholder="Search..."
              className="w-64 rounded-full bg-background pl-8 md:w-80"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>

          <Button variant="outline" size="icon" className="relative">
            <Bell className="h-5 w-5" />
            <Badge className="absolute -right-1 -top-1 h-5 w-5 rounded-full p-0 text-xs">3</Badge>
            <span className="sr-only">Notifications</span>
          </Button>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="relative h-8 w-8 rounded-full">
                <Avatar className="h-8 w-8">
                  <AvatarFallback className="bg-green-100 text-green-800">
                    {account ? account.substring(2, 4).toUpperCase() : "NA"}
                  </AvatarFallback>
                </Avatar>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent className="w-56" align="end" forceMount>
              <DropdownMenuLabel className="font-normal">
                <div className="flex flex-col space-y-1">
                  <p className="text-sm font-medium leading-none">{truncateAddress(account || "")}</p>
                  <p className="text-xs leading-none text-muted-foreground">Connected Wallet</p>
                </div>
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem>
                <User className="mr-2 h-4 w-4" />
                <span>Profile</span>
              </DropdownMenuItem>
              <DropdownMenuItem>
                <Settings className="mr-2 h-4 w-4" />
                <span>Settings</span>
              </DropdownMenuItem>
              <DropdownMenuItem>
                <HelpCircle className="mr-2 h-4 w-4" />
                <span>Help</span>
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={disconnectWallet}>
                <LogOut className="mr-2 h-4 w-4" />
                <span>Disconnect</span>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    </header>
  )
}
