"use client"

import { useState, useEffect } from "react"
import { useWeb3 } from "@/context/web3-context"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Loader2, AlertCircle, Percent } from "lucide-react"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { DatePickerWithRange } from "@/components/date-range-picker"
import DashboardLayout from "@/components/dashboard-layout"

export default function RentalsPage() {
  const { account } = useWeb3()
  const [discount, setDiscount] = useState(0)
  const [equipment, setEquipment] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [dateRange, setDateRange] = useState({ from: null, to: null })

  useEffect(() => {
    async function fetchRentalData() {
      if (!account) return

      try {
        setLoading(true)
        setError(null)

        const response = await fetch(`/api/rental/${account}`)

        if (!response.ok) {
          throw new Error("Failed to fetch rental data")
        }

        const data = await response.json()

        setDiscount(data.discount)
        setEquipment(data.equipment)
      } catch (err) {
        console.error("Error fetching rental data:", err)
        setError(err.message || "Failed to load rental information")
      } finally {
        setLoading(false)
      }
    }

    fetchRentalData()
  }, [account])

  if (!account) {
    return (
      <DashboardLayout>
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Not connected</AlertTitle>
          <AlertDescription>Please connect your wallet to view equipment rentals.</AlertDescription>
        </Alert>
      </DashboardLayout>
    )
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Premium Equipment Rentals</h1>
          <p className="text-muted-foreground">Rent high-quality farming equipment with sustainability discounts.</p>
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
                <CardTitle className="flex items-center">
                  <Percent className="mr-2 h-5 w-5" />
                  Your Equipment Discount
                </CardTitle>
                <CardDescription>Based on your sustainability score and NFT tier</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex items-center justify-center p-6">
                  <div className="text-center">
                    <p className="text-5xl font-bold text-green-600">{discount}%</p>
                    <p className="text-sm text-muted-foreground mt-2">
                      {discount === 0
                        ? "Increase your sustainability score to earn discounts"
                        : "Discount applied automatically to all equipment rentals"}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <div>
              <h2 className="text-xl font-semibold mb-4">Available Equipment</h2>
              <div className="grid gap-6 md:grid-cols-2">
                {equipment.map((item) => (
                  <Card key={item.id}>
                    <CardHeader className="pb-2">
                      <div className="flex justify-between items-start">
                        <CardTitle>{item.name}</CardTitle>
                        {discount > 0 && (
                          <Badge className="bg-green-100 text-green-800 border-green-200">{discount}% OFF</Badge>
                        )}
                      </div>
                      <CardDescription>{item.description}</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="flex justify-center mb-4">
                        <img
                          src={`https://picsum.photos/seed/mechanical-${item.id}/200/150`}
                          alt={item.name}
                          width={200}
                          height={150}
                          className="rounded-md object-cover"
                          onError={(e) => {
                            e.currentTarget.src = "https://picsum.photos/200/150";
                          }}
                        />
                      </div>
                      <div className="flex justify-between items-center">
                        <div>
                          <p className="text-sm text-muted-foreground">Base Price</p>
                          <p className={discount > 0 ? "text-lg line-through" : "text-lg font-semibold"}>
                            {item.basePrice} MATIC/day
                          </p>
                        </div>
                        {discount > 0 && (
                          <div>
                            <p className="text-sm text-muted-foreground">Your Price</p>
                            <p className="text-lg font-semibold text-green-600">{item.discountedPrice} MATIC/day</p>
                          </div>
                        )}
                      </div>

                      <div className="mt-4">
                        <p className="text-sm font-medium mb-2">Select Rental Period</p>
                        <DatePickerWithRange value={dateRange} onChange={setDateRange} />
                      </div>
                    </CardContent>
                    <CardFooter>
                      <Button
                        className="w-full bg-green-600 hover:bg-green-700"
                        disabled={!dateRange.from || !dateRange.to}
                      >
                        Rent Equipment
                      </Button>
                    </CardFooter>
                  </Card>
                ))}
              </div>
            </div>
          </>
        )}
      </div>
    </DashboardLayout>
  )
}