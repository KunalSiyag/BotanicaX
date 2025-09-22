import { NextResponse } from "next/server"
import { ethers } from "ethers"
import CertificationManagerABI from "@/abis/CertificationManager.json"

// Contract configuration
// Contract configuration
const CONTRACT_ADDRESS = process.env.NEXT_PUBLIC_CERTIFICATION_MANAGER_ADDRESS || ""
const PROVIDER_URL = process.env.BLOCKCHAIN_PROVIDER_URL || "https://polygon-amoy.g.alchemy.com/v2/UaYqWAunkmAmGl6fqnPxRJXUPTrGGoci"

// Initialize provider
const provider = new ethers.JsonRpcProvider(PROVIDER_URL)
const contract = new ethers.Contract(CONTRACT_ADDRESS, CertificationManagerABI, provider)

// Function to fetch score from smart contract
const getScore = async (address: string): Promise<number> => {
  try {
    // Validate address
    if (!ethers.isAddress(address)) {
      throw new Error("Invalid Ethereum address")
    }

    // Call the smart contract's getScore function
    const score = await contract.getScore(address)
    return Number(score) // Convert BigNumber to number
  } catch (error) {
    console.error("Error fetching score from blockchain:", error)
    throw new Error("Failed to fetch score from blockchain")
  }
}

export async function GET(request: Request, { params }: { params: { address: string } }) {
  try {
    const address = params.address

    if (!address || typeof address !== "string") {
      return NextResponse.json({ error: "Valid address is required" }, { status: 400 })
    }

    // Get the farmer's score
    const score = await getScore(address)

    // Define loan offers based on score thresholds
    const loanOffers = []

    // Base loan amount
    const baseLoanAmount = 1000 // MATIC

    // High tier (850+)
    if (score >= 850) {
      loanOffers.push({
        tier: "Premium",
        interestRate: 3.5,
        maxAmount: baseLoanAmount * 3,
        term: 24, // months
        description: "Premium loan with lowest interest rate and highest amount",
        eligible: true,
      })
    }

    // Medium tier (700-849)
    if (score >= 700) {
      loanOffers.push({
        tier: "Standard",
        interestRate: 5.0,
        maxAmount: baseLoanAmount * 2,
        term: 18, // months
        description: "Standard loan with competitive interest rate",
        eligible: true,
      })
    }

    // Low tier (500-699)
    if (score >= 500) {
      loanOffers.push({
        tier: "Basic",
        interestRate: 7.5,
        maxAmount: baseLoanAmount,
        term: 12, // months
        description: "Basic loan for farmers building their sustainability score",
        eligible: true,
      })
    }

    // Add ineligible offers for reference
    if (score < 850) {
      loanOffers.push({
        tier: "Premium",
        interestRate: 3.5,
        maxAmount: baseLoanAmount * 3,
        term: 24,
        description: "Premium loan with lowest interest rate and highest amount",
        eligible: false,
        requiredScore: 850,
      })
    }

    if (score < 700) {
      loanOffers.push({
        tier: "Standard",
        interestRate: 5.0,
        maxAmount: baseLoanAmount * 2,
        term: 18,
        description: "Standard loan with competitive interest rate",
        eligible: false,
        requiredScore: 700,
      })
    }

    if (score < 500) {
      loanOffers.push({
        tier: "Basic",
        interestRate: 7.5,
        maxAmount: baseLoanAmount,
        term: 12,
        description: "Basic loan for farmers building their sustainability score",
        eligible: false,
        requiredScore: 500,
      })
    }

    // Sort offers: eligible first, then by tier
    loanOffers.sort((a, b) => {
      if (a.eligible && !b.eligible) return -1
      if (!a.eligible && b.eligible) return 1
      return a.interestRate - b.interestRate
    })

    return NextResponse.json({
      score,
      loanOffers,
    })
  } catch (error) {
    console.error("Error fetching loan offers:", error)
    return NextResponse.json({ error: "Failed to fetch loan offers" }, { status: 500 })
  }
}
