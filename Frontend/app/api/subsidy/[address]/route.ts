import { NextResponse } from "next/server"
import { ethers } from "ethers"
import CertificationManagerABI from "@/abis/CertificationManager.json" // Import ABI from abis folder

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

    // Validate input
    if (!address || typeof address !== "string") {
      return NextResponse.json({ error: "Valid address is required" }, { status: 400 })
    }

    // Get the farmer's score from the blockchain
    const score = await getScore(address)

    // Define subsidy tiers based on score
    const subsidies = []

    if (score >= 700) {
      subsidies.push({
        tier: "Silver",
        discount: "10%",
        description: "Basic subsidy for sustainable farming practices",
        minScore: 700,
      })
    }

    if (score >= 850) {
      subsidies.push({
        tier: "Gold",
        discount: "25%",
        description: "Premium subsidy for exceptional sustainability metrics",
        minScore: 850,
      })
    }

    return NextResponse.json({
      score,
      subsidies,
      eligible: subsidies.length > 0,
    })
  } catch (error: any) {
    console.error("Error fetching subsidy eligibility:", error)
    return NextResponse.json(
      {
        error: "Failed to fetch subsidy eligibility",
        message: error.message || "An unexpected error occurred",
      },
      { status: 500 }
    )
  }
}