import { NextResponse } from "next/server";
import { ethers } from "ethers";
import CARBON_CREDITS_ABI from "@/abis/CarbonCredits.json";

// Environment variables
const RPC_URL = process.env.BLOCKCHAIN_RPC_URL || "https://polygon-amoy.g.alchemy.com/v2/UaYqWAunkmAmGl6fqnPxRJXUPTrGGoci";
const CARBON_CREDITS_ADDRESS = process.env.NEXT_PUBLIC_CARBON_CREDITS_ADDRESS || "0xAbE39B883651a644Eb0661229E6789ed64bA9A97";

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { address } = body;

    // Validate address
    if (!address || !ethers.isAddress(address)) {
      return NextResponse.json({ error: "Valid Ethereum address is required" }, { status: 400 });
    }

    // Initialize provider and contract (read-only)
    const provider = new ethers.JsonRpcProvider(RPC_URL);
    const carbonCreditsContract = new ethers.Contract(
      CARBON_CREDITS_ADDRESS,
      CARBON_CREDITS_ABI,
      provider
    );

    // Get balance
    const balance = await carbonCreditsContract.balanceOf(address);

    return NextResponse.json({
      success: true,
      address,
      balance: ethers.formatEther(balance),
    });
  } catch (error: any) {
    console.error("Error fetching balance:", error);
    return NextResponse.json(
      {
        error: "Failed to fetch carbon credit balance",
        message: error?.message || "Something went wrong",
      },
      { status: 500 }
    );
  }
}
