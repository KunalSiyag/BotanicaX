"use client"

import Link from "next/link"
import { Button } from "@/components/ui/button"
import { useWeb3 } from "@/context/web3-context"
import { Leaf } from "lucide-react"
import { usePathname } from "next/navigation"

export default function Navbar() {
  const { account, isConnected } = useWeb3()
  const pathname = usePathname()

  return (
    <header className="border-b bg-white/80 backdrop-blur-sm dark:bg-gray-950/80 sticky top-0 z-50">
      <div className="container flex h-16 items-center justify-between px-4">
        <Link href="/" className="flex items-center gap-2">
          <Leaf className="h-6 w-6 text-green-600 dark:text-green-400" />
          <span className="font-bold text-xl">Smart Agri</span>
        </Link>

        <nav className="hidden md:flex items-center gap-6 text-sm">
          <Link
            href="/login"
            className={`transition-colors hover:text-green-600 dark:hover:text-green-400 ${
              pathname === "/login" ? "text-green-600 dark:text-green-400 font-medium" : ""
            }`}
          >
            Login
          </Link>
          <Link
            href="/smart-rewards"
            className={`transition-colors hover:text-green-600 dark:hover:text-green-400 ${
              pathname === "/smart-rewards" ? "text-green-600 dark:text-green-400 font-medium" : ""
            }`}
          >
            Smart Rewards
          </Link>
          <Link
            href="/rewards"
            className={`transition-colors hover:text-green-600 dark:hover:text-green-400 ${
              pathname === "/rewards" ? "text-green-600 dark:text-green-400 font-medium" : ""
            }`}
          >
            Rewards
          </Link>
        </nav>

        <div className="flex items-center gap-4">
          {isConnected ? (
            <div className="hidden md:block">
              <p className="text-xs text-gray-500 dark:text-gray-400">Connected:</p>
              <p className="text-xs font-mono truncate max-w-[120px]">{account}</p>
            </div>
          ) : (
            <Link href="/login">
              <Button className="bg-green-600 hover:bg-green-700">Connect Wallet</Button>
            </Link>
          )}
        </div>
      </div>
    </header>
  )
}
