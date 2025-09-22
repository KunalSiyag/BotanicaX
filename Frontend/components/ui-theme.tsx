"use client"

import type React from "react"

import { createContext, useContext, useState, useEffect } from "react"

type Theme = "light" | "dark" | "system"

type ThemeProviderProps = {
  children: React.ReactNode
  defaultTheme?: Theme
  storageKey?: string
}

type ThemeProviderState = {
  theme: Theme
  setTheme: (theme: Theme) => void
}

const initialState: ThemeProviderState = {
  theme: "system",
  setTheme: () => null,
}

const ThemeProviderContext = createContext<ThemeProviderState>(initialState)

export function UIThemeProvider({
  children,
  defaultTheme = "system",
  storageKey = "smart-agri-ui-theme",
  ...props
}: ThemeProviderProps) {
  const [theme, setTheme] = useState<Theme>(defaultTheme)

    // Load theme from localStorage on client-side mount
    useEffect(() => {
      // Only access localStorage on the client
      if (typeof window !== "undefined") {
        const storedTheme = localStorage.getItem(storageKey) as Theme | null
        setTheme(storedTheme || defaultTheme)
      }
    }, [storageKey, defaultTheme])
  
  useEffect(() => {
    const root = window.document.documentElement

    root.classList.remove("light", "dark")

    if (theme === "system") {
      const systemTheme = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light"

      root.classList.add(systemTheme)
      return
    }

    root.classList.add(theme)
  }, [theme])

  const value = {
    theme,
    setTheme: (theme: Theme) => {
      localStorage.setItem(storageKey, theme)
      setTheme(theme)
    },
  }

  return (
    <ThemeProviderContext.Provider {...props} value={value}>
      {children}
    </ThemeProviderContext.Provider>
  )
}

export const useUITheme = () => {
  const context = useContext(ThemeProviderContext)

  if (context === undefined) throw new Error("useUITheme must be used within a UIThemeProvider")

  return context
}
