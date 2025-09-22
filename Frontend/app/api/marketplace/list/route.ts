import { NextResponse } from "next/server"
import { ethers } from "ethers"
import SmartRewardsNFTABI from "@/abis/SmartRewardsNFT.json"

// Contract configuration
const SMART_REWARDS_NFT_ADDRESS = process.env.NEXT_PUBLIC_SMART_REWARDS_NFT_ADDRESS || ""
const PROVIDER_URL = process.env.BLOCKCHAIN_PROVIDER_URL || "https://polygon-amoy.g.alchemy.com/v2/UaYqWAunkmAmGl6fqnPxRJXUPTrGGoci"

// Initialize provider and contract
const provider = new ethers.JsonRpcProvider(PROVIDER_URL)
const nftContract = new ethers.Contract(SMART_REWARDS_NFT_ADDRESS, SmartRewardsNFTABI, provider)

// Function to check if user has an NFT
const hasNFT = async (address: string): Promise<boolean> => {
  try {
    // Validate address
    if (!ethers.isAddress(address)) {
      throw new Error("Invalid Ethereum address")
    }

    // Check NFT balance
    const balance = await nftContract.balanceOf(address)
    return Number(balance) > 0
  } catch (error) {
    console.error("Error checking NFT ownership:", error)
    throw new Error("Failed to check NFT ownership")
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json()
    const { address } = body

    if (!address || typeof address !== "string") {
      return NextResponse.json({ error: "Valid address is required" }, { status: 400 })
    }

    // Check if the user has an NFT
    const hasNft = await hasNFT(address)

    return NextResponse.json({
      ok: hasNft,
      message: hasNft
        ? "You are authorized to list items in the marketplace"
        : "You need to get certified with an NFT to list items",
    })
  } catch (error: any) {
    console.error("Error checking marketplace access:", error)
    return NextResponse.json(
      {
        error: "Failed to check marketplace access",
        message: error.message || "An unexpected error occurred",
      },
      { status: 500 }
    )
  }
}