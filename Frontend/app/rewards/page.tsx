"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { useWeb3 } from "@/context/web3-context"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Loader2 } from "lucide-react"
import { useToast } from "@/components/ui/use-toast"

export default function RewardsPage() {
  const { account, rewardToken, smartRewardsNFT,certificationManager, isLoading } = useWeb3()
  const [balance, setBalance] = useState<string>("0")
  const [tier, setTier] = useState<number | null>(null)
  const [isClaiming, setIsClaiming] = useState(false)
  const [isFetching, setIsFetching] = useState(false)
  const router = useRouter()
  const { toast } = useToast()

  useEffect(() => {
    if (!account && !isLoading) {
      router.push("/")
    } else if (account) {
      fetchData()
    }
  }, [account, isLoading, router])

  const fetchData = async () => {
    if (!rewardToken || !smartRewardsNFT || !account) return

    try {
      setIsFetching(true)

      // Get token balance
      const balanceBN = await rewardToken.balanceOf(account)
      setBalance(balanceBN.toString())

      // Get farmer tier
      const score = await certificationManager.getScore(account)
      const tierBN = await smartRewardsNFT.determineTier(score)
      setTier(Number(tierBN))
    } catch (error) {
      console.error("Error fetching data:", error)
      toast({
        title: "Error",
        description: "Failed to fetch reward data",
        variant: "destructive",
      })
    } finally {
      setIsFetching(false)
    }
  }

  const handleClaimRewards = async () => {
    if (!rewardToken || !account) return

    try {
      setIsClaiming(true)

      const tx = await rewardToken.claimRewards()
      await tx.wait()

      toast({
        title: "Success",
        description: "Rewards claimed successfully!",
      })

      // Refresh balance
      await fetchData()
    } catch (error: any) {
      console.error("Error claiming rewards:", error)
      toast({
        title: "Error",
        description: error.reason || "Failed to claim rewards",
        variant: "destructive",
      })
    } finally {
      setIsClaiming(false)
    }
  }

  if (isLoading || isFetching) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-green-600" />
      </div>
    )
  }

  return (
    <DashboardLayout>
      <Card className="max-w-2xl mx-auto">
        <CardHeader>
          <CardTitle>Reward Tokens</CardTitle>
          <CardDescription>Claim and manage your Sustainable Farming Tokens (SFT)</CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="p-6 border rounded-lg text-center">
            <h3 className="text-lg font-medium mb-2">Your Token Balance</h3>
            <p className="text-4xl font-bold text-green-600">{balance} SFT</p>
          </div>

          <div className="p-6 border rounded-lg">
            <h3 className="text-lg font-medium mb-4">Rewards Information</h3>
            <div className="space-y-2">
              <p>
                <strong>Current Tier:</strong>{" "}
                {tier === 3 ? "Platinum" : tier === 2 ? "Gold" : tier === 1 ? "Silver" : "None"}
              </p>
              <p>
                <strong>Reward Amount:</strong>{" "}
                {tier === 3 ? "1500 SFT" : tier === 2 ? "1000 SFT" : tier === 1 ? "500 SFT" : "0 SFT"}
              </p>
              <p className="text-sm text-muted-foreground mt-4">
                Note: You need to have an NFT to claim rewards. Higher tier NFTs earn more tokens.
              </p>
            </div>
          </div>
        </CardContent>
        <CardFooter className="flex justify-end">
          <Button
            onClick={handleClaimRewards}
            disabled={isClaiming || !tier}
            className="bg-green-600 hover:bg-green-700"
          >
            {isClaiming ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Claiming...
              </>
            ) : (
              "Claim Rewards"
            )}
          </Button>
        </CardFooter>
      </Card>
    </DashboardLayout>
  )
}

