"use client"

import { useState, useEffect } from "react"
import { useWeb3 } from "@/context/web3-context"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Loader2, AlertCircle } from "lucide-react"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Progress } from "@/components/ui/progress"
import { Badge } from "@/components/ui/badge"
import DashboardLayout from "@/components/dashboard-layout"

export default function PerksPage() {
  const { account } = useWeb3()
  const [subsidies, setSubsidies] = useState([])
  const [score, setScore] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function fetchSubsidies() {
      if (!account) return

      try {
        setLoading(true)
        setError(null)

        const response = await fetch(`/api/subsidy/${account}`)

        if (!response.ok) {
          throw new Error("Failed to fetch subsidy data")
        }

        const data = await response.json()
        setScore(data.score)
        setSubsidies(data.subsidies)
      } catch (err) {
        console.error("Error fetching subsidies:", err)
        setError(err.message || "Failed to load subsidy information")
      } finally {
        setLoading(false)
      }
    }

    fetchSubsidies()
  }, [account])

  if (!account) {
    return (
      <DashboardLayout>
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Not connected</AlertTitle>
          <AlertDescription>Please connect your wallet to view your subsidy eligibility.</AlertDescription>
        </Alert>
      </DashboardLayout>
    )
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Subsidy Eligibility</h1>
          <p className="text-muted-foreground">
            Check your eligibility for agricultural subsidies based on your sustainability score.
          </p>
        </div>

        {loading ? (
          <div className="flex justify-center p-12">
            <Loader2 className="h-8 w-8 animate-spin text-green-600" />
          </div>
        ) : error ? (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : (
          <>
            <Card>
              <CardHeader>
                <CardTitle>Your Sustainability Score</CardTitle>
                <CardDescription>Your current score determines your subsidy eligibility</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="flex justify-between items-center">
                    <span className="text-sm font-medium">Score</span>
                    <span className="text-sm font-medium">{score}/1000</span>
                  </div>
                  <Progress value={(score / 1000) * 100} className="h-2" />

                  <div className="grid grid-cols-3 gap-4 pt-4">
                    <div className="text-center">
                      <div className="text-sm font-medium text-muted-foreground mb-2">Basic</div>
                      <div className={`h-2 bg-gray-200 rounded-full ${score >= 1 ? "bg-green-500" : ""}`}></div>
                    </div>
                    <div className="text-center">
                      <div className="text-sm font-medium text-muted-foreground mb-2">Silver (700+)</div>
                      <div className={`h-2 bg-gray-200 rounded-full ${score >= 700 ? "bg-green-500" : ""}`}></div>
                    </div>
                    <div className="text-center">
                      <div className="text-sm font-medium text-muted-foreground mb-2">Gold (850+)</div>
                      <div className={`h-2 bg-gray-200 rounded-full ${score >= 850 ? "bg-green-500" : ""}`}></div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="grid gap-4 md:grid-cols-2">
              {subsidies.length > 0 ? (
                subsidies.map((subsidy, index) => (
                  <Card key={index}>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                      <CardTitle className="text-lg font-medium">{subsidy.tier} Tier Subsidy</CardTitle>
                      <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200">
                        {subsidy.discount}
                      </Badge>
                    </CardHeader>
                    <CardContent>
                      <p className="text-sm text-muted-foreground">{subsidy.description}</p>
                    </CardContent>
                    <CardFooter>
                      <Button className="w-full bg-green-600 hover:bg-green-700">Apply for Subsidy</Button>
                    </CardFooter>
                  </Card>
                ))
              ) : (
                <Card className="md:col-span-2">
                  <CardHeader>
                    <CardTitle>No Subsidies Available</CardTitle>
                    <CardDescription>You need a minimum score of 700 to qualify for subsidies.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <Alert>
                      <AlertCircle className="h-4 w-4" />
                      <AlertTitle>Improve Your Score</AlertTitle>
                      <AlertDescription>
                        Implement more sustainable farming practices to increase your score and qualify for subsidies.
                      </AlertDescription>
                    </Alert>
                  </CardContent>
                </Card>
              )}
            </div>
          </>
        )}
      </div>
    </DashboardLayout>
  )
}
