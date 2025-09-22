"use client"

import type React from "react"

import { useState } from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { useWeb3 } from "@/context/web3-context"
import { Button } from "@/components/ui/button"
import {
  Home,
  Award,
  Coins,
  BarChart3,
  Menu,
  X,
  LogOut,
  Percent,
  ShoppingBag,
  Truck,
  Trophy,
  Leaf,
  DollarSign,
} from "lucide-react"

interface DashboardLayoutProps {
  children: React.ReactNode
}

export default function DashboardLayout({ children }: DashboardLayoutProps) {
  const { disconnectWallet, isOwner } = useWeb3()
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const pathname = usePathname()

  const navItems = [
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

  return (
    <div className="flex min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Mobile sidebar toggle */}
      <div className="fixed top-4 left-4 z-50 md:hidden">
        <Button variant="outline" size="icon" onClick={() => setSidebarOpen(!sidebarOpen)} className="rounded-full">
          {sidebarOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
        </Button>
      </div>

      {/* Sidebar */}
      <div
        className={`fixed inset-y-0 left-0 z-40 w-64 transform bg-white dark:bg-gray-800 shadow-lg transition-transform duration-200 ease-in-out md:translate-x-0 ${
          sidebarOpen ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="flex flex-col h-full">
          <div className="p-4 border-b">
            <h2 className="text-xl font-bold text-green-600 dark:text-green-400">Smart Agri dApp</h2>
            <p className="text-sm text-muted-foreground">Sustainable Farming Platform</p>
          </div>

          <nav className="flex-1 p-4 space-y-1 overflow-y-auto">
            {navItems.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className={`flex items-center px-4 py-3 rounded-md transition-colors ${
                  pathname === item.href
                    ? "bg-green-50 text-green-700 dark:bg-green-900 dark:text-green-400"
                    : "hover:bg-gray-100 dark:hover:bg-gray-700"
                }`}
                onClick={() => setSidebarOpen(false)}
              >
                {item.icon}
                <span className="ml-3">{item.name}</span>
              </Link>
            ))}
          </nav>

          <div className="p-4 border-t">
            <Button variant="outline" className="w-full flex items-center justify-center" onClick={disconnectWallet}>
              <LogOut className="h-4 w-4 mr-2" />
              Disconnect
            </Button>
          </div>
        </div>
      </div>

      {/* Main content */}
      <div className="flex-1 md:ml-64">
        <main className="p-6 md:p-8 max-w-7xl mx-auto">{children}</main>
      </div>
    </div>
  )
}
