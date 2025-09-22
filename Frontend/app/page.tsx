"use client"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { useWeb3 } from "@/context/web3-context"
import { LoginForm } from "@/components/auth/login-form"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Leaf } from "lucide-react"

export default function Home() {
  const { account } = useWeb3()
  const router = useRouter()

  useEffect(() => {
    if (account) {
      router.push("/dashboard")
    }
  }, [account, router])

  return (
    <main className="flex min-h-screen flex-col items-center justify-center p-4 bg-gradient-to-b from-green-50 to-green-100 dark:from-green-950 dark:to-green-900">
      <Card className="w-full max-w-md mb-8">
        <CardHeader className="text-center">
          <div className="flex justify-center mb-4">
            <div className="bg-green-100 dark:bg-green-800 p-4 rounded-full">
              <Leaf className="h-12 w-12 text-green-600 dark:text-green-400" />
            </div>
          </div>
          <CardTitle className="text-3xl font-bold text-green-700 dark:text-green-400">Smart Agri dApp</CardTitle>
          <CardDescription>Sustainable farming on the blockchain</CardDescription>
        </CardHeader>
        <CardContent className="text-center space-y-4">
          <p className="text-sm text-muted-foreground">
            Smart Agri is a decentralized platform that rewards sustainable farming practices with tokens, NFTs, and
            financial incentives.
          </p>
          <div className="grid grid-cols-3 gap-2 py-4">
            <div className="flex flex-col items-center">
              <div className="w-12 h-12 bg-green-100 dark:bg-green-800 rounded-full flex items-center justify-center mb-2">
                <Leaf className="h-6 w-6 text-green-600 dark:text-green-400" />
              </div>
              <p className="text-xs font-medium">Sustainability</p>
            </div>
            <div className="flex flex-col items-center">
              <div className="w-12 h-12 bg-green-100 dark:bg-green-800 rounded-full flex items-center justify-center mb-2">
                <Leaf className="h-6 w-6 text-green-600 dark:text-green-400" />
              </div>
              <p className="text-xs font-medium">Rewards</p>
            </div>
            <div className="flex flex-col items-center">
              <div className="w-12 h-12 bg-green-100 dark:bg-green-800 rounded-full flex items-center justify-center mb-2">
                <Leaf className="h-6 w-6 text-green-600 dark:text-green-400" />
              </div>
              <p className="text-xs font-medium">Community</p>
            </div>
          </div>
        </CardContent>
      </Card>

      <LoginForm />
    </main>
  )
}
