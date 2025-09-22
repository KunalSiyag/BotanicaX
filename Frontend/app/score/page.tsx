"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { useWeb3 } from "@/context/web3-context"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Loader2 } from "lucide-react"
import { useToast } from "@/components/ui/use-toast"

export default function ScorePage() {
  const { account, isOwner, certificationManager, isLoading } = useWeb3()
  const [farmerAddress, setFarmerAddress] = useState("")
  const [score, setScore] = useState("")
  const [currentScore, setCurrentScore] = useState<number | null>(null)
  const [isUpdating, setIsUpdating] = useState(false)
  const [isFetching, setIsFetching] = useState(false)
  const router = useRouter()
  const { toast } = useToast()

  useEffect(() => {
    if (!account && !isLoading) {
      router.push("/")
    }
  }, [account, isLoading, router])

  const handleGetScore = async () => {
    if (!certificationManager) return

    try {
      setIsFetching(true)
      const addressToCheck = isOwner ? farmerAddress : account
      if (!addressToCheck) {
        toast({
          title: "Error",
          description: "No address provided",
          variant: "destructive",
        })
        return
      }

      const score = await certificationManager.getScore(addressToCheck)
      setCurrentScore(Number(score))
    } catch (error) {
      console.error("Error fetching score:", error)
      toast({
        title: "Error",
        description: "Failed to fetch score",
        variant: "destructive",
      })
    } finally {
      setIsFetching(false)
    }
  }

  const handleUpdateScore = async () => {
    if (!certificationManager || !isOwner) return

    try {
      setIsUpdating(true)
      console.log("Updating score for:", farmerAddress, "to", score)
      const tx = await certificationManager.updateScore(farmerAddress, score)
      await tx.wait()

      toast({
        title: "Success",
        description: `Score updated to ${score} for ${farmerAddress}`,
      })

      setCurrentScore(Number(score))
    } catch (error) {
      console.error("Error updating score:", error)
      toast({
        title: "Error",
        description: "Failed to update score",
        variant: "destructive",
      })
    } finally {
      setIsUpdating(false)
    }
  }

  if (isLoading) {
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
          <CardTitle>{isOwner ? "Update Farmer Score" : "View Your Score"}</CardTitle>
          <CardDescription>
            {isOwner
              ? "As an admin, you can update the sustainability score for farmers"
              : "Check your current sustainability score"}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {isOwner && (
            <div className="space-y-2">
              <Label htmlFor="farmerAddress">Farmer Address</Label>
              <Input
                id="farmerAddress"
                placeholder="0x..."
                value={farmerAddress}
                onChange={(e) => setFarmerAddress(e.target.value)}
              />
            </div>
          )}

          {isOwner && (
            <div className="space-y-2">
              <Label htmlFor="score">Score (0-1000)</Label>
              <Input
                id="score"
                type="number"
                min="0"
                max="1000"
                placeholder="Enter score (0-1000)"
                value={score}
                onChange={(e) => setScore(e.target.value)}
              />
            </div>
          )}

          {currentScore !== null && (
            <div className="p-4 bg-muted rounded-md mt-4">
              <p className="font-medium">Current Score:</p>
              <p className="text-3xl font-bold text-green-600">{currentScore}</p>
              {currentScore >= 850 && <p className="text-sm mt-1">Tier: Platinum</p>}
              {currentScore >= 750 && currentScore < 850 && <p className="text-sm mt-1">Tier: Gold</p>}
              {currentScore >= 700 && currentScore < 750 && <p className="text-sm mt-1">Tier: Silver</p>}
              {currentScore < 700 && <p className="text-sm mt-1">Tier: Not Eligible for NFT</p>}
            </div>
          )}
        </CardContent>
        <CardFooter className="flex justify-end gap-2">
          <Button variant="outline" onClick={handleGetScore} disabled={isFetching}>
            {isFetching ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Fetching...
              </>
            ) : (
              "Get Score"
            )}
          </Button>

          {isOwner && (
            <Button
              onClick={handleUpdateScore}
              disabled={isUpdating || !farmerAddress || !score}
              className="bg-green-600 hover:bg-green-700"
            >
              {isUpdating ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Updating...
                </>
              ) : (
                "Update Score"
              )}
            </Button>
          )}
        </CardFooter>
      </Card>
    </DashboardLayout>
  )
}

