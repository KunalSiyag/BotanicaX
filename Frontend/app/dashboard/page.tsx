"use client"

import { useState, useEffect } from "react"
import { useWeb3 } from "@/context/web3-context"
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts'

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Button } from "@/components/ui/button"
import { DataCard } from "@/components/ui/data-card"
import { StatusBadge } from "@/components/ui/status-badge"
import DashboardLayout from "@/components/dashboard/layout"
import {
  BarChart3,
  Leaf,
  Trophy,
  Coins,
  ArrowUpRight,
  ShoppingBag,
  Truck,
  DollarSign,
  Percent,
  ChevronRight,
} from "lucide-react"
import { useRouter } from "next/navigation"

export default function Dashboard() {
  const { account, isOwner } = useWeb3()
  const router = useRouter()
  const [score, setScore] = useState<number | null>(null)
  const [loading, setLoading] = useState(true)

  // Generate sustainability data with the last point as the current score
  const sustainabilityData = Array.from({ length: 12 }, (_, i) => {
    if (score === null) {
      // Fallback data while score is loading
      return {
        name: `Month ${i + 1}`,
        value: 600, // Placeholder value
      }
    }
    // Index 11 (Month 12) should be the current score
    if (i === 11) {
      return {
        name: `Month ${i + 1}`,
        value: score,
      }
    }
    // Extrapolate backwards from the current score
    // Assume a gentle upward trend towards the current score
    const baseValue = score - (11 - i) * 7.5 // Decrease by ~7.5 points per month backwards
    // Add controlled randomness (+/- 10 points) for realism
    const variation = (Math.random() - 0.5) * 20
    const value = Math.round(Math.max(0, Math.min(1000, baseValue + variation)))
    return {
      name: `Month ${i + 1}`,
      value,
    }
  })

  // Determine line colors based on trend
  const coloredData = sustainabilityData.map((point, i, arr) => ({
    ...point,
    color: i === 0 || point.value >= arr[i - 1]?.value ? '#22c55e' : '#ef4444', // Green for increase, red for decrease
  }))

  useEffect(() => {
    // Fetch user score
    const fetchScore = async () => {
      if (account) {
        try {
          setLoading(true)
          const response = await fetch(`/api/subsidy/${account}`)
          const data = await response.json()
          setScore(data.score)
        } catch (error) {
          console.error("Error fetching score:", error)
        } finally {
          setLoading(false)
        }
      }
    }

    fetchScore()
  }, [account])

  return (
    <DashboardLayout>
      <div className="flex flex-col gap-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
          <p className="text-muted-foreground">
            Welcome to your Smart Agri dashboard. Monitor your sustainability metrics and rewards.
          </p>
        </div>

        <Tabs defaultValue="overview" className="space-y-6">
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="rewards">Rewards</TabsTrigger>
            <TabsTrigger value="marketplace">Marketplace</TabsTrigger>
            <TabsTrigger value="finance">Finance</TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="space-y-6">
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
              <DataCard
                title="Sustainability Score"
                value={score !== null ? `${score}/1000` : "Loading..."}
                description="Your current sustainability rating"
                icon={<BarChart3 className="h-4 w-4" />}
                trend={{ value: 12, label: "from last month" }}
              />
              <DataCard
                title="Carbon Credits"
                value="245"
                description="Total credits earned"
                icon={<Leaf className="h-4 w-4" />}
                trend={{ value: 8, label: "from last month" }}
              />
              <DataCard
                title="Leaderboard Rank"
                value="#12"
                description="Your position among farmers"
                icon={<Trophy className="h-4 w-4" />}
                trend={{ value: 3, label: "positions up" }}
              />
              <DataCard
                title="Reward Tokens"
                value="1,250 SFT"
                description="Available balance"
                icon={<Coins className="h-4 w-4" />}
                trend={{ value: 24, label: "from last month" }}
              />
            </div>

            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              <Card className="col-span-2">
                <CardHeader>
                  <CardTitle>Sustainability Progress</CardTitle>
                  <CardDescription>
                    Track your sustainability metrics over time
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-[300px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={coloredData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="name" />
                        <YAxis domain={[0, 1000]} />
                        <Tooltip />
                        <Line
                          type="monotone"
                          dataKey="value"
                          stroke="#8884d8"
                          strokeWidth={2}
                          dot={false}
                          isAnimationActive={false}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Recent Activity</CardTitle>
                  <CardDescription>Your latest platform interactions</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex items-center gap-4">
                      <div className="bg-green-100 p-2 rounded-full">
                        <Leaf className="h-4 w-4 text-green-600" />
                      </div>
                      <div className="flex-1 space-y-1">
                        <p className="text-sm font-medium">Carbon Credits Claimed</p>
                        <p className="text-xs text-muted-foreground">50 credits • 2 days ago</p>
                      </div>
                      <StatusBadge status="success" text="Completed" />
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="bg-blue-100 p-2 rounded-full">
                        <ShoppingBag className="h-4 w-4 text-blue-600" />
                      </div>
                      <div className="flex-1 space-y-1">
                        <p className="text-sm font-medium">Marketplace Purchase</p>
                        <p className="text-xs text-muted-foreground">Organic Seeds • 3 days ago</p>
                      </div>
                      <StatusBadge status="success" text="Completed" />
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="bg-purple-100 p-2 rounded-full">
                        <Truck className="h-4 w-4 text-purple-600" />
                      </div>
                      <div className="flex-1 space-y-1">
                        <p className="text-sm font-medium">Equipment Rental</p>
                        <p className="text-xs text-muted-foreground">Precision Seeder • 1 week ago</p>
                      </div>
                      <StatusBadge status="info" text="Active" />
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="bg-yellow-100 p-2 rounded-full">
                        <Coins className="h-4 w-4 text-yellow-600" />
                      </div>
                      <div className="flex-1 space-y-1">
                        <p className="text-sm font-medium">Rewards Claimed</p>
                        <p className="text-xs text-muted-foreground">250 SFT • 2 weeks ago</p>
                      </div>
                      <StatusBadge status="success" text="Completed" />
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          <TabsContent value="rewards" className="space-y-6">
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle>NFT Rewards</CardTitle>
                  <CardDescription>Your sustainability NFT status</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-col items-center justify-center p-4">
                    <div className="w-24 h-24 bg-green-100 rounded-full flex items-center justify-center mb-4">
                      <Trophy className="h-12 w-12 text-green-600" />
                    </div>
                    <h3 className="text-xl font-bold mb-1">Silver Tier</h3>
                    <p className="text-sm text-muted-foreground mb-4">Current NFT level</p>
                    <div className="w-full bg-gray-200 rounded-full h-2 mb-2">
                      <div className="bg-green-600 h-2 rounded-full" style={{ width: "65%" }}></div>
                    </div>
                    <p className="text-xs text-muted-foreground">65% to Gold Tier</p>
                  </div>
                </CardContent>
                <div className="px-6 py-4 bg-muted/20 border-t">
                  <Button onClick={() => router.push("/nft")} variant="outline" className="w-full">
                    View NFT Details
                    <ChevronRight className="ml-2 h-4 w-4" />
                  </Button>
                </div>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle>Token Rewards</CardTitle>
                  <CardDescription>Your SFT token balance</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-col items-center justify-center p-4">
                    <div className="w-24 h-24 bg-yellow-100 rounded-full flex items-center justify-center mb-4">
                      <Coins className="h-12 w-12 text-yellow-600" />
                    </div>
                    <h3 className="text-xl font-bold mb-1">1,250 SFT</h3>
                    <p className="text-sm text-muted-foreground mb-4">Available balance</p>
                    <div className="flex items-center justify-center gap-2 text-sm">
                      <span className="text-green-600 flex items-center">
                        <ArrowUpRight className="mr-1 h-4 w-4" />
                        +250 SFT
                      </span>
                      <span className="text-muted-foreground">this month</span>
                    </div>
                  </div>
                </CardContent>
                <div className="px-6 py-4 bg-muted/20 border-t">
                  <Button onClick={() => router.push("/rewards")} variant="outline" className="w-full">
                    Claim Rewards
                    <ChevronRight className="ml-2 h-4 w-4" />
                  </Button>
                </div>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle>Carbon Credits</CardTitle>
                  <CardDescription>Your carbon offset achievements</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-col items-center justify-center p-4">
                    <div className="w-24 h-24 bg-green-100 rounded-full flex items-center justify-center mb-4">
                      <Leaf className="h-12 w-12 text-green-600" />
                    </div>
                    <h3 className="text-xl font-bold mb-1">245 Credits</h3>
                    <p className="text-sm text-muted-foreground mb-4">Total earned</p>
                    <div className="flex items-center justify-center gap-2 text-sm">
                      <span className="text-green-600 flex items-center">
                        <ArrowUpRight className="mr-1 h-4 w-4" />
                        +50 Credits
                      </span>
                      <span className="text-muted-foreground">this month</span>
                    </div>
                  </div>
                </CardContent>
                <div className="px-6 py-4 bg-muted/20 border-t">
                  <Button onClick={() => router.push("/carbon")} variant="outline" className="w-full">
                    Claim Carbon Credits
                    <ChevronRight className="ml-2 h-4 w-4" />
                  </Button>
                </div>
              </Card>
            </div>
          </TabsContent>

          <TabsContent value="marketplace" className="space-y-6">
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              <Card className="col-span-2">
                <CardHeader>
                  <CardTitle>Marketplace Activity</CardTitle>
                  <CardDescription>Recent listings and purchases</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex items-center gap-4">
                      <div className="bg-blue-100 p-2 rounded-full">
                        <ShoppingBag className="h-4 w-4 text-blue-600" />
                      </div>
                      <div className="flex-1 space-y-1">
                        <p className="text-sm font-medium">Organic Seeds</p>
                        <p className="text-xs text-muted-foreground">Listed by 0x1234...5678 • 0.05 MATIC</p>
                      </div>
                      <Button size="sm" variant="outline">
                        View
                      </Button>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="bg-blue-100 p-2 rounded-full">
                        <ShoppingBag className="h-4 w-4 text-blue-600" />
                      </div>
                      <div className="flex-1 space-y-1">
                        <p className="text-sm font-medium">Eco-Friendly Tools</p>
                        <p className="text-xs text-muted-foreground">Listed by 0x8765...4321 • 0.1 MATIC</p>
                      </div>
                      <Button size="sm" variant="outline">
                        View
                      </Button>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="bg-blue-100 p-2 rounded-full">
                        <ShoppingBag className="h-4 w-4 text-blue-600" />
                      </div>
                      <div className="flex-1 space-y-1">
                        <p className="text-sm font-medium">Soil Analysis Service</p>
                        <p className="text-xs text-muted-foreground">Listed by 0xabcd...efgh • 0.2 MATIC</p>
                      </div>
                      <Button size="sm" variant="outline">
                        View
                      </Button>
                    </div>
                  </div>
                </CardContent>
                <div className="px-6 py-4 bg-muted/20 border-t">
                  <Button onClick={() => router.push("/marketplace")} variant="outline" className="w-full">
                    Browse Marketplace
                    <ChevronRight className="ml-2 h-4 w-4" />
                  </Button>
                </div>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Equipment Rentals</CardTitle>
                  <CardDescription>Available premium equipment</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div className="flex items-center gap-4">
                      <div className="bg-purple-100 p-2 rounded-full">
                        <Truck className="h-4 w-4 text-purple-600" />
                      </div>
                      <div className="flex-1 space-y-1">
                        <p className="text-sm font-medium">Precision Seeder</p>
                        <p className="text-xs text-muted-foreground">0.5 MATIC/day • 15% discount</p>
                      </div>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="bg-purple-100 p-2 rounded-full">
                        <Truck className="h-4 w-4 text-purple-600" />
                      </div>
                      <div className="flex-1 space-y-1">
                        <p className="text-sm font-medium">Drone Sprayer</p>
                        <p className="text-xs text-muted-foreground">0.8 MATIC/day • 15% discount</p>
                      </div>
                    </div>
                  </div>
                </CardContent>
                <div className="px-6 py-4 bg-muted/20 border-t">
                  <Button onClick={() => router.push("/rentals")} variant="outline" className="w-full">
                    View Equipment
                    <ChevronRight className="ml-2 h-4 w-4" />
                  </Button>
                </div>
              </Card>
            </div>
          </TabsContent>

          <TabsContent value="finance" className="space-y-6">
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle>Subsidy Eligibility</CardTitle>
                  <CardDescription>Your current subsidy status</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-col items-center justify-center p-4">
                    <div className="w-24 h-24 bg-green-100 rounded-full flex items-center justify-center mb-4">
                      <Percent className="h-12 w-12 text-green-600" />
                    </div>
                    <h3 className="text-xl font-bold mb-1">Silver Tier</h3>
                    <p className="text-sm text-muted-foreground mb-4">10% Discount</p>
                    <StatusBadge status="success" text="Eligible" />
                  </div>
                </CardContent>
                <div className="px-6 py-4 bg-muted/20 border-t">
                  <Button onClick={() => router.push("/perks")} variant="outline" className="w-full">
                    View Subsidies
                    <ChevronRight className="ml-2 h-4 w-4" />
                  </Button>
                </div>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle>Loan Offers</CardTitle>
                  <CardDescription>Available microfinance options</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-col items-center justify-center p-4">
                    <div className="w-24 h-24 bg-blue-100 rounded-full flex items-center justify-center mb-4">
                      <DollarSign className="h-12 w-12 text-blue-600" />
                    </div>
                    <h3 className="text-xl font-bold mb-1">Standard Loan</h3>
                    <p className="text-sm text-muted-foreground mb-4">5.0% APR • 2000 MATIC</p>
                    <StatusBadge status="success" text="Eligible" />
                  </div>
                </CardContent>
                <div className="px-6 py-4 bg-muted/20 border-t">
                  <Button onClick={() => router.push("/loans")} variant="outline" className="w-full">
                    View Loan Options
                    <ChevronRight className="ml-2 h-4 w-4" />
                  </Button>
                </div>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle>Leaderboard Ranking</CardTitle>
                  <CardDescription>Your community standing</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-col items-center justify-center p-4">
                    <div className="w-24 h-24 bg-yellow-100 rounded-full flex items-center justify-center mb-4">
                      <Trophy className="h-12 w-12 text-yellow-600" />
                    </div>
                    <h3 className="text-xl font-bold mb-1">Rank #12</h3>
                    <p className="text-sm text-muted-foreground mb-4">Top 15% of farmers</p>
                    <div className="flex items-center justify-center gap-2 text-sm">
                      <span className="text-green-600 flex items-center">
                        <ArrowUpRight className="mr-1 h-4 w-4" />
                        +3 positions
                      </span>
                      <span className="text-muted-foreground">this month</span>
                    </div>
                  </div>
                </CardContent>
                <div className="px-6 py-4 bg-muted/20 border-t">
                  <Button onClick={() => router.push("/leaderboard")} variant="outline" className="w-full">
                    View Leaderboard
                    <ChevronRight className="ml-2 h-4 w-4" />
                  </Button>
                </div>
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </DashboardLayout>
  )
}