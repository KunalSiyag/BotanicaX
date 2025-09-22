"use client"

import { useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { useWeb3 } from "@/context/web3-context"
import { Loader2, AlertCircle, Award } from "lucide-react"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { useToast } from "@/components/ui/use-toast"
import { Progress } from "@/components/ui/progress"
import { Badge } from "@/components/ui/badge"

export default function SmartRewardsPage() {
  const { address, isConnected, smartRewardsNFT, certificationManager } = useWeb3()
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [tier, setTier] = useState<number | null>(null)
  const [score, setScore] = useState<number | null>(null)
  const [hasNFT, setHasNFT] = useState(false)
  const { toast } = useToast()

  useEffect(() => {
    if (isConnected && address) {
      fetchUserData()
    }
  }, [isConnected, address])

  const fetchUserData = async () => {
    if (!smartRewardsNFT || !certificationManager || !address) return

    try {
      setLoading(true)
      setError(null)

      // Get user's score
      const userScore = await certificationManager.getScore(address)
      setScore(Number(userScore))

      // Check if user has NFT
      const balance = await smartRewardsNFT.balanceOf(address)
      setHasNFT(Number(balance) > 0)

      if (Number(balance) > 0) {
        // Get NFT tier if user has one
        const tokenId = await smartRewardsNFT.tokenOfOwnerByIndex(address, 0)
        const tokenTier = await smartRewardsNFT.getTier(tokenId)
        setTier(Number(tokenTier))
      }
    } catch (err: any) {
      setError(err.message || "Failed to fetch user data")
    } finally {
      setLoading(false)
    }
  }

  const handleMintNFT = async () => {
    if (!smartRewardsNFT || !address) return

    try {
      setLoading(true)
      setError(null)
      const tx = await smartRewardsNFT.mint()
      await tx.wait()
      toast({
        title: "NFT Minted",
        description: "Successfully minted your Smart Rewards NFT",
      })
      await fetchUserData()
    } catch (err: any) {
      setError(err.message || "Failed to mint NFT")
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to mint NFT",
      })
    } finally {
      setLoading(false)
    }
  }

  const handleUpgradeNFT = async () => {
    if (!smartRewardsNFT || !address) return

    try {
      setLoading(true)
      setError(null)
      const tx = await smartRewardsNFT.upgrade()
      await tx.wait()
      toast({
        title: "NFT Upgraded",
        description: "Successfully upgraded your Smart Rewards NFT",
      })
      await fetchUserData()
    } catch (err: any) {
      setError(err.message || "Failed to upgrade NFT")
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to upgrade NFT. Make sure your score is high enough.",
      })
    } finally {
      setLoading(false)
    }
  }

  const getTierName = (tier: number) => {
    switch (tier) {
      case 1:
        return "Bronze"
      case 2:
        return "Silver"
      case 3:
        return "Gold"
      case 4:
        return "Platinum"
      case 5:
        return "Diamond"
      default:
        return "None"
    }
  }

  const getTierColor = (tier: number) => {
    switch (tier) {
      case 1:
        return "bg-amber-700"
      case 2:
        return "bg-gray-400"
      case 3:
        return "bg-yellow-500"
      case 4:
        return "bg-blue-400"
      case 5:
        return "bg-purple-400"
      default:
        return "bg-gray-300"
    }
  }

  const getNextTierRequirement = (currentTier: number) => {
    switch (currentTier) {
      case 1:
        return 40 // Bronze to Silver
      case 2:
        return 60 // Silver to Gold
      case 3:
        return 80 // Gold to Platinum
      case 4:
        return 95 // Platinum to Diamond
      case 5:
        return null // Max tier
      default:
        return 20 // None to Bronze
    }
  }

  return (
    <div className="flex justify-center items-center min-h-[80vh]">
      <Card className="w-full max-w-md">
        <CardHeader>
          <CardTitle>Smart Rewards NFT</CardTitle>
          <CardDescription>Mint and upgrade your sustainability NFT</CardDescription>
        </CardHeader>
        <CardContent>
          {error && (
            <Alert variant="destructive" className="mb-4">
              <AlertCircle className="h-4 w-4" />
              <AlertTitle>Error</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {!isConnected ? (
            <div className="text-center p-6">
              <p className="mb-4">Please connect your wallet on the login page first</p>
              <Button onClick={() => (window.location.href = "/login")} className="bg-green-600 hover:bg-green-700">
                Go to Login
              </Button>
            </div>
          ) : loading ? (
            <div className="flex justify-center p-8">
              <Loader2 className="h-8 w-8 animate-spin text-green-600" />
            </div>
          ) : (
            <div className="space-y-6">
              {score !== null && (
                <div className="space-y-2">
                  <div className="flex justify-between">
                    <p className="text-sm font-medium">Your Sustainability Score</p>
                    <p className="text-sm font-medium">{score}/100</p>
                  </div>
                  <Progress value={score} className="h-2" />
                </div>
              )}

              {hasNFT && tier !== null ? (
                <div className="space-y-6">
                  <div className="flex flex-col items-center p-6 bg-green-50 dark:bg-green-900 rounded-lg">
                    <div className={`p-4 rounded-full ${getTierColor(tier)} mb-4`}>
                      <Award className="h-12 w-12 text-white" />
                    </div>
                    <Badge className="mb-2">{getTierName(tier)} Tier</Badge>
                    <p className="text-sm text-center text-gray-500 dark:text-gray-400">
                      Your Smart Rewards NFT reflects your commitment to sustainable farming practices
                    </p>
                  </div>

                  {tier < 5 && (
                    <div className="space-y-4">
                      <div className="space-y-2">
                        <div className="flex justify-between">
                          <p className="text-sm font-medium">Progress to Next Tier</p>
                          <p className="text-sm font-medium">
                            {score}/{getNextTierRequirement(tier)}
                          </p>
                        </div>
                        <Progress
                          value={
                            score && getNextTierRequirement(tier) ? (score / getNextTierRequirement(tier)!) * 100 : 0
                          }
                          className="h-2"
                        />
                      </div>

                      <Button
                        onClick={handleUpgradeNFT}
                        className="w-full bg-green-600 hover:bg-green-700"
                        disabled={
                          loading ||
                          (score !== null &&
                            getNextTierRequirement(tier) !== null &&
                            score < getNextTierRequirement(tier)!)
                        }
                      >
                        {loading ? (
                          <>
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            Upgrading...
                          </>
                        ) : (
                          "Upgrade NFT"
                        )}
                      </Button>

                      {score !== null &&
                        getNextTierRequirement(tier) !== null &&
                        score < getNextTierRequirement(tier)! && (
                          <p className="text-xs text-center text-amber-600 dark:text-amber-400">
                            You need a score of at least {getNextTierRequirement(tier)} to upgrade to the next tier
                          </p>
                        )}
                    </div>
                  )}
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="p-6 bg-green-50 dark:bg-green-900 rounded-lg text-center">
                    <p className="mb-2 font-medium">You don't have a Smart Rewards NFT yet</p>
                    <p className="text-sm text-gray-500 dark:text-gray-400">
                      Mint your first NFT to start your sustainability journey
                    </p>
                  </div>

                  <Button onClick={handleMintNFT} className="w-full bg-green-600 hover:bg-green-700" disabled={loading}>
                    {loading ? (
                      <>
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        Minting...
                      </>
                    ) : (
                      "Mint NFT"
                    )}
                  </Button>
                </div>
              )}
            </div>
          )}
        </CardContent>
        <CardFooter className="flex justify-center">
          <p className="text-xs text-gray-500 dark:text-gray-400">NFTs are on Polygon Amoy Testnet</p>
        </CardFooter>
      </Card>
    </div>
  )
}
