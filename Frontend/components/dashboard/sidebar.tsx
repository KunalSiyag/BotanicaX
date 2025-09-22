"use client"

import { useState, useEffect } from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { useWeb3 } from "@/context/web3-context"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  SidebarSeparator,
} from "@/components/ui/sidebar"
import { Button } from "@/components/ui/button"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Home,
  Award,
  Coins,
  BarChart3,
  LogOut,
  Percent,
  ShoppingBag,
  Truck,
  Trophy,
  Leaf,
  DollarSign,
  Settings,
  User,
  Search,
  HelpCircle,
  Bell,
  Sun,
  Moon,
  Laptop,
} from "lucide-react"
import { useUITheme } from "@/components/ui-theme"

export function DashboardSidebar() {
  const { account, disconnectWallet, isOwner } = useWeb3()
  const pathname = usePathname()
  const { theme, setTheme } = useUITheme()
  const [score, setScore] = useState<number | null>(null)

  useEffect(() => {
    // Fetch user score
    const fetchScore = async () => {
      if (account) {
        try {
          const response = await fetch(`/api/subsidy/${account}`)
          const data = await response.json()
          setScore(data.score)
        } catch (error) {
          console.error("Error fetching score:", error)
        }
      }
    }

    fetchScore()
  }, [account])

  const mainNavItems = [
    {
      name: "Dashboard",
      href: "/dashboard",
      icon: <Home className="h-5 w-5" />,
    },
    {
      name: isOwner ? "Update Score" : "View Score",
      href: "/score",
      icon: <BarChart3 className="h-5 w-5" />,
    },
    {
      name: "NFT Rewards",
      href: "/nft",
      icon: <Award className="h-5 w-5" />,
    },
    {
      name: "Token Rewards",
      href: "/rewards",
      icon: <Coins className="h-5 w-5" />,
    },
    {
      name: "Subsidies",
      href: "/perks",
      icon: <Percent className="h-5 w-5" />,
    },
    {
      name: "Marketplace",
      href: "/marketplace",
      icon: <ShoppingBag className="h-5 w-5" />,
    },
    {
      name: "Equipment Rentals",
      href: "/rentals",
      icon: <Truck className="h-5 w-5" />,
    },
    {
      name: "Carbon Credits",
      href: "/carbon",
      icon: <Leaf className="h-5 w-5" />,
    },
    {
      name: "Loans",
      href: "/loans",
      icon: <DollarSign className="h-5 w-5" />,
    },
  ]

  const truncateAddress = (address: string) => {
    if (!address) return ""
    return `${address.slice(0, 6)}...${address.slice(-4)}`
  }

  return (
    <Sidebar variant="floating" className="border-none shadow-lg">
      <SidebarHeader className="py-4">
        <div className="flex items-center px-2">
          <Leaf className="h-6 w-6 text-green-600 dark:text-green-400 mr-2" />
          <span className="font-bold text-xl">Smart Agri</span>
        </div>
        <div className="mt-4 px-2">
          <Input
            placeholder="Search..."
            className="bg-background"
            prefix={<Search className="h-4 w-4 text-muted-foreground" />}
          />
        </div>
      </SidebarHeader>
      <SidebarSeparator />
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Main Navigation</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {mainNavItems.map((item) => (
                <SidebarMenuItem key={item.href}>
                  <SidebarMenuButton asChild isActive={pathname === item.href} tooltip={item.name}>
                    <Link href={item.href}>
                      {item.icon}
                      <span>{item.name}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarSeparator />

        <SidebarGroup>
          <SidebarGroupLabel>Your Status</SidebarGroupLabel>
          <SidebarGroupContent>
            <div className="p-3 bg-muted/50 rounded-lg">
              <div className="flex items-center mb-2">
                <div className="mr-3">
                  <Avatar>
                    <AvatarFallback className="bg-green-100 text-green-800">
                      {account ? account.substring(2, 4).toUpperCase() : "NA"}
                    </AvatarFallback>
                  </Avatar>
                </div>
                <div>
                  <p className="text-sm font-medium">{truncateAddress(account || "")}</p>
                  <p className="text-xs text-muted-foreground">{isOwner ? "Admin" : "Farmer"}</p>
                </div>
              </div>
              {score !== null && (
                <div className="mt-2">
                  <div className="flex justify-between items-center mb-1">
                    <span className="text-xs font-medium">Sustainability Score</span>
                    <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200">
                      {score}/1000
                    </Badge>
                  </div>
                  <div className="w-full h-1.5 bg-gray-200 rounded-full overflow-hidden">
                    <div className="h-full bg-green-600" style={{ width: `${(score / 1000) * 100}%` }}></div>
                  </div>
                </div>
              )}
            </div>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="border-t">
        <div className="flex items-center justify-between p-4">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon">
                <Bell className="h-5 w-5" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel>Notifications</DropdownMenuLabel>
              <DropdownMenuSeparator />
              <div className="p-4 text-center text-sm text-muted-foreground">No new notifications</div>
            </DropdownMenuContent>
          </DropdownMenu>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon">
                {theme === "light" ? (
                  <Sun className="h-5 w-5" />
                ) : theme === "dark" ? (
                  <Moon className="h-5 w-5" />
                ) : (
                  <Laptop className="h-5 w-5" />
                )}
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel>Theme</DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={() => setTheme("light")}>
                <Sun className="mr-2 h-4 w-4" />
                Light
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setTheme("dark")}>
                <Moon className="mr-2 h-4 w-4" />
                Dark
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => setTheme("system")}>
                <Laptop className="mr-2 h-4 w-4" />
                System
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon">
                <HelpCircle className="h-5 w-5" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel>Help</DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem>Documentation</DropdownMenuItem>
              <DropdownMenuItem>Support</DropdownMenuItem>
              <DropdownMenuItem>FAQ</DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon">
                <User className="h-5 w-5" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel>My Account</DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem>
                <User className="mr-2 h-4 w-4" />
                Profile
              </DropdownMenuItem>
              <DropdownMenuItem>
                <Settings className="mr-2 h-4 w-4" />
                Settings
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={disconnectWallet}>
                <LogOut className="mr-2 h-4 w-4" />
                Disconnect
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  )
}
