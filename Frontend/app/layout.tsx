import type React from "react"
import type { Metadata } from "next"
import { Inter } from "next/font/google"
import "./globals.css"
import { Web3Provider } from "@/context/web3-context"
import { Toaster } from "@/components/ui/toaster"
import { UIThemeProvider } from "@/components/ui-theme"

const inter = Inter({ subsets: ["latin"] })

export const metadata: Metadata = {
  title: "Smart Agri dApp",
  description: "Blockchain-based Smart Agriculture Platform",
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <UIThemeProvider defaultTheme="system" storageKey="smart-agri-ui-theme">
          <Web3Provider>
            {children}
            <Toaster />
          </Web3Provider>
        </UIThemeProvider>
      </body>
    </html>
  )
}
