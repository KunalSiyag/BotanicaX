"use client"

import { useState, useEffect } from "react"
import { useWeb3 } from "@/context/web3-context"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { Slider } from "@/components/ui/slider"
import { Loader2, AlertCircle, DollarSign, Lock } from "lucide-react"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Progress } from "@/components/ui/progress"
import { Badge } from "@/components/ui/badge"
import DashboardLayout from "@/components/dashboard-layout"

export default function LoansPage() {
  const { account } = useWeb3()
  const [score, setScore] = useState(0)
  const [loanOffers, setLoanOffers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Selected loan state
  const [selectedLoan, setSelectedLoan] = useState(null)
  const [loanAmount, setLoanAmount] = useState(0)

  useEffect(() => {
    async function fetchLoanOffers() {
      if (!account) return

      try {
        setLoading(true)
        setError(null)

        const response = await fetch(`/api/loans/${account}`)

        if (!response.ok) {
          throw new Error("Failed to fetch loan offers")
        }

        const data = await response.json()
        setScore(data.score)
        setLoanOffers(data.loanOffers)

        // Set default selected loan to the first eligible one
        const eligibleLoan = data.loanOffers.find((loan) => loan.eligible)
        if (eligibleLoan) {
          setSelectedLoan(eligibleLoan)
          setLoanAmount(eligibleLoan.maxAmount / 2) // Default to half the max amount
        }
      } catch (err) {
        console.error("Error fetching loan offers:", err)
        setError(err.message || "Failed to load loan information")
      } finally {
        setLoading(false)
      }
    }

    fetchLoanOffers()
  }, [account])

  const calculateMonthlyPayment = (amount, rate, term) => {
    const monthlyRate = rate / 100 / 12
    const payment = (amount * monthlyRate * Math.pow(1 + monthlyRate, term)) / (Math.pow(1 + monthlyRate, term) - 1)
    return payment.toFixed(2)
  }

  if (!account) {
    return (
      <DashboardLayout>
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Not connected</AlertTitle>
          <AlertDescription>Please connect your wallet to view loan offers.</AlertDescription>
        </Alert>
      </DashboardLayout>
    )
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Microfinance Loans</h1>
          <p className="text-muted-foreground">Access agricultural loans based on your sustainability score.</p>
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
                <CardTitle>Your Loan Eligibility</CardTitle>
                <CardDescription>Based on your sustainability score of {score}/1000</CardDescription>
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
                      <div className="text-sm font-medium text-muted-foreground mb-2">Basic (500+)</div>
                      <div className={`h-2 bg-gray-200 rounded-full ${score >= 500 ? "bg-green-500" : ""}`}></div>
                    </div>
                    <div className="text-center">
                      <div className="text-sm font-medium text-muted-foreground mb-2">Standard (700+)</div>
                      <div className={`h-2 bg-gray-200 rounded-full ${score >= 700 ? "bg-green-500" : ""}`}></div>
                    </div>
                    <div className="text-center">
                      <div className="text-sm font-medium text-muted-foreground mb-2">Premium (850+)</div>
                      <div className={`h-2 bg-gray-200 rounded-full ${score >= 850 ? "bg-green-500" : ""}`}></div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="grid gap-6 md:grid-cols-2">
              <div>
                <h2 className="text-xl font-semibold mb-4">Available Loan Offers</h2>
                <div className="space-y-4">
                  {loanOffers.map((loan, index) => (
                    <Card
                      key={index}
                      className={`cursor-pointer transition-all ${
                        selectedLoan === loan
                          ? "border-green-500 ring-2 ring-green-200"
                          : loan.eligible
                            ? "hover:border-green-200"
                            : "opacity-70"
                      }`}
                      onClick={() => loan.eligible && setSelectedLoan(loan)}
                    >
                      <CardHeader className="pb-2">
                        <div className="flex justify-between items-start">
                          <CardTitle className="text-lg">{loan.tier} Loan</CardTitle>
                          {loan.eligible ? (
                            <Badge className="bg-green-100 text-green-800 border-green-200">
                              {loan.interestRate}% APR
                            </Badge>
                          ) : (
                            <Badge variant="outline" className="bg-gray-100 text-gray-800 border-gray-200">
                              <Lock className="h-3 w-3 mr-1" />
                              Locked
                            </Badge>
                          )}
                        </div>
                        <CardDescription>{loan.description}</CardDescription>
                      </CardHeader>
                      <CardContent>
                        <div className="space-y-2">
                          <div className="flex justify-between text-sm">
                            <span className="text-muted-foreground">Max Amount:</span>
                            <span className="font-medium">{loan.maxAmount} MATIC</span>
                          </div>
                          <div className="flex justify-between text-sm">
                            <span className="text-muted-foreground">Term:</span>
                            <span className="font-medium">{loan.term} months</span>
                          </div>
                          {!loan.eligible && (
                            <Alert className="mt-2 py-2">
                              <AlertCircle className="h-4 w-4" />
                              <AlertDescription className="text-xs">
                                Requires a minimum score of {loan.requiredScore}
                              </AlertDescription>
                            </Alert>
                          )}
                        </div>
                      </CardContent>
                    </Card>
                  ))}

                  {loanOffers.length === 0 && (
                    <Card>
                      <CardHeader>
                        <CardTitle>No Loan Offers Available</CardTitle>
                        <CardDescription>Improve your sustainability score to qualify for loans.</CardDescription>
                      </CardHeader>
                      <CardContent>
                        <Alert>
                          <AlertCircle className="h-4 w-4" />
                          <AlertTitle>Minimum Score Required</AlertTitle>
                          <AlertDescription>
                            A minimum score of 500 is required to qualify for basic loans.
                          </AlertDescription>
                        </Alert>
                      </CardContent>
                    </Card>
                  )}
                </div>
              </div>

              {selectedLoan && (
                <div>
                  <h2 className="text-xl font-semibold mb-4">Loan Calculator</h2>
                  <Card>
                    <CardHeader>
                      <CardTitle>Calculate Your Loan</CardTitle>
                      <CardDescription>Adjust the amount to see your monthly payments</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-6">
                      <div className="space-y-2">
                        <div className="flex justify-between">
                          <Label htmlFor="loanAmount">Loan Amount</Label>
                          <span className="text-sm font-medium">{loanAmount} MATIC</span>
                        </div>
                        <Slider
                          id="loanAmount"
                          min={100}
                          max={selectedLoan.maxAmount}
                          step={100}
                          value={[loanAmount]}
                          onValueChange={(value) => setLoanAmount(value[0])}
                        />
                        <div className="flex justify-between text-xs text-muted-foreground">
                          <span>100 MATIC</span>
                          <span>{selectedLoan.maxAmount} MATIC</span>
                        </div>
                      </div>

                      <div className="space-y-4 pt-4">
                        <div className="flex justify-between">
                          <span className="text-sm text-muted-foreground">Interest Rate:</span>
                          <span className="font-medium">{selectedLoan.interestRate}% APR</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm text-muted-foreground">Loan Term:</span>
                          <span className="font-medium">{selectedLoan.term} months</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm text-muted-foreground">Monthly Payment:</span>
                          <span className="font-medium">
                            {calculateMonthlyPayment(loanAmount, selectedLoan.interestRate, selectedLoan.term)} MATIC
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm text-muted-foreground">Total Repayment:</span>
                          <span className="font-medium">
                            {(
                              calculateMonthlyPayment(loanAmount, selectedLoan.interestRate, selectedLoan.term) *
                              selectedLoan.term
                            ).toFixed(2)}{" "}
                            MATIC
                          </span>
                        </div>
                      </div>
                    </CardContent>
                    <CardFooter>
                      <Button className="w-full bg-green-600 hover:bg-green-700">
                        <DollarSign className="mr-2 h-4 w-4" />
                        Apply for Loan
                      </Button>
                    </CardFooter>
                  </Card>
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </DashboardLayout>
  )
}
