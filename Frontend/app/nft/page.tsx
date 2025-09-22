"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { useWeb3 } from "@/context/web3-context"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Loader2 } from "lucide-react"
import { useToast } from "@/components/ui/use-toast"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

export default function NFTPage() {
  const { account, smartRewardsNFT, certificationManager, isLoading } = useWeb3()
  const [tier, setTier] = useState<number | null>(null)
  const [score, setScore] = useState<number | null>(null)
  const [isMinting, setIsMinting] = useState(false)
  const [isFetching, setIsFetching] = useState(false)
  const [tokenURI, setTokenURI] = useState("ipfs://QmExample")
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
    if (!smartRewardsNFT || !certificationManager || !account) return

    try {
      setIsFetching(true)

      // Get farmer tier
      const tierBN = await smartRewardsNFT.farmerTier(account)
      setTier(Number(tierBN))

      // Get farmer score
      const scoreBN = await certificationManager.getScore(account)
      setScore(Number(scoreBN))
    } catch (error) {
      console.error("Error fetching data:", error)
      toast({
        title: "Error",
        description: "Failed to fetch NFT data",
        variant: "destructive",
      })
    } finally {
      setIsFetching(false)
    }
  }

  const handleMintOrUpgrade = async () => {
    if (!smartRewardsNFT || !account) return

    try {
      setIsMinting(true)

      // For this demo, we'll use a placeholder tokenURI
      // In a real app, you'd generate this based on the tier
      const tx = await smartRewardsNFT.mintOrUpgradeNFT(account, tokenURI)
      await tx.wait()

      toast({
        title: "Success",
        description: "NFT minted or upgraded successfully!",
      })

      // Refresh data
      await fetchData()
    } catch (error: any) {
      console.error("Error minting NFT:", error)
      toast({
        title: "Error",
        description: error.reason || "Failed to mint or upgrade NFT",
        variant: "destructive",
      })
    } finally {
      setIsMinting(false)
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
          <CardTitle>Smart Rewards NFT</CardTitle>
          <CardDescription>Mint or upgrade your NFT based on your sustainability score</CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="flex flex-col md:flex-row gap-6">
            <div className="flex-1 p-6 border rounded-lg text-center">
              <h3 className="text-lg font-medium mb-2">Your Current Tier</h3>
              {tier ? (
                <div>
                  <p className="text-3xl font-bold text-green-600 mb-2">
                    {tier === 3 ? "Platinum" : tier === 2 ? "Gold" : "Silver"}
                  </p>
                  <div className="w-24 h-24 mx-auto bg-muted rounded-full flex items-center justify-center">
                    <span className="text-4xl">{tier === 3 ? "🏆" : tier === 2 ? "🥇" : "🥈"}</span>
                  </div>
                </div>
              ) : (
                <p>You don't have an NFT yet</p>
              )}
            </div>

            <div className="flex-1 p-6 border rounded-lg">
              <h3 className="text-lg font-medium mb-2">Eligibility</h3>
              {score !== null ? (
                <div>
                  <p className="mb-2">
                    Your current score: <span className="font-bold">{score}</span>
                  </p>
                  {score >= 850 ? (
                    <p className="text-green-600">You are eligible for Platinum Tier (Score ≥ 850)</p>
                  ) : score >= 750 ? (
                    <p className="text-green-600">You are eligible for Gold Tier (Score ≥ 750)</p>
                  ) : score >= 700 ? (
                    <p className="text-green-600">You are eligible for Silver Tier (Score ≥ 700)</p>
                  ) : (
                    <p className="text-red-500">You need a score of at least 700 to mint an NFT</p>
                  )}
                </div>
              ) : (
                <p>Loading eligibility...</p>
              )}
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="tokenURI">Token URI (for demo purposes)</Label>
            <Input
              id="tokenURI"
              placeholder="ipfs://..."
              value={tokenURI}
              onChange={(e) => setTokenURI(e.target.value)}
            />
            <p className="text-xs text-muted-foreground">
              In a production app, this would be generated automatically based on your tier.
            </p>
          </div>
        </CardContent>
        <CardFooter className="flex justify-end">
          <Button
            onClick={handleMintOrUpgrade}
            disabled={isMinting || score === null || score < 700 || tier === 3}
            className="bg-green-600 hover:bg-green-700"
          >
            {isMinting ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Processing...
              </>
            ) : tier ? (
              score && score >= 850 && tier < 3 ? (
                "Upgrade to Platinum"
              ) : score && score >= 750 && tier < 2 ? (
                "Upgrade to Gold"
              ) : (
                "Already at highest eligible tier"
              )
            ) : (
              "Mint NFT"
            )}
          </Button>
        </CardFooter>
      </Card>
    </DashboardLayout>
  )
}
