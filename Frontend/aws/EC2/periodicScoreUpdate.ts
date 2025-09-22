import { DynamoDBClient } from "@aws-sdk/client-dynamodb"
import { DynamoDBDocumentClient, ScanCommand } from "@aws-sdk/lib-dynamodb"
import { ethers } from "ethers"

// Initialize DynamoDB client
const client = new DynamoDBClient({})
const dynamo = DynamoDBDocumentClient.from(client)

// Environment variables
const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME || "SmartAgriSensorData"
const RPC_URL = process.env.BLOCKCHAIN_RPC_URL || "https://polygon-amoy.g.alchemy.com/v2/your-api-key"
const CERTIFICATION_MANAGER_ADDRESS = process.env.CERTIFICATION_MANAGER_ADDRESS || ""
const TRUSTED_FORWARDER_PRIVATE_KEY = process.env.TRUSTED_FORWARDER_PRIVATE_KEY || ""

// ABI for the CertificationManager contract
const CERTIFICATION_MANAGER_ABI = [
  "function updateScore(address farmer, uint256 score) external",
  "function getScore(address farmer) external view returns (uint256)",
]

export const handler = async (event: any) => {
  try {
    console.log("Starting periodic score update")

    // Connect to the blockchain
    const provider = new ethers.JsonRpcProvider(RPC_URL)
    const wallet = new ethers.Wallet(TRUSTED_FORWARDER_PRIVATE_KEY, provider)
    const certificationManager = new ethers.Contract(CERTIFICATION_MANAGER_ADDRESS, CERTIFICATION_MANAGER_ABI, wallet)

    // Get the latest processed scores for each farmer
    const farmerScores = await getLatestFarmerScores()
    console.log(`Found ${farmerScores.length} farmers with processed scores`)

    // Update scores on the blockchain
    const updatePromises = farmerScores.map(async (farmerData) => {
      try {
        const { farmerId, score } = farmerData

        // Get current score from blockchain
        const currentScore = await certificationManager.getScore(farmerId)

        // Only update if the new score is significantly different (>5% change)
        if (Math.abs(Number(currentScore) - score) > Number(currentScore) * 0.05) {
          console.log(`Updating score for ${farmerId}: ${currentScore} -> ${score}`)
          const tx = await certificationManager.updateScore(farmerId, score)
          await tx.wait()
          return { farmerId, success: true, oldScore: Number(currentScore), newScore: score }
        } else {
          console.log(`No significant change for ${farmerId}: ${currentScore} vs ${score}`)
          return { farmerId, success: true, noChange: true }
        }
      } catch (error) {
        console.error(`Error updating score for ${farmerData.farmerId}:`, error)
        return { farmerId: farmerData.farmerId, success: false, error: String(error) }
      }
    })

    const results = await Promise.all(updatePromises)

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Periodic score update completed",
        results,
      }),
    }
  } catch (error) {
    console.error("Error in periodic score update:", error)
    return {
      statusCode: 500,
      body: JSON.stringify({ message: "Error in periodic score update", error: String(error) }),
    }
  }
}

// Function to get the latest processed score for each farmer
async function getLatestFarmerScores(): Promise<Array<{ farmerId: string; score: number }>> {
  const farmerMap = new Map<string, { score: number; timestamp: number }>()

  let lastEvaluatedKey: any = undefined

  do {
    const params: any = {
      TableName: TABLE_NAME,
      FilterExpression: "processed = :processed",
      ExpressionAttributeValues: {
        ":processed": true,
      },
    }

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey
    }

    const response = await dynamo.send(new ScanCommand(params))

    // Process items
    if (response.Items) {
      for (const item of response.Items) {
        const farmerId = item.farmerId
        const timestamp = item.timestamp
        const score = item.score

        // Keep only the latest score for each farmer
        if (!farmerMap.has(farmerId) || farmerMap.get(farmerId)!.timestamp < timestamp) {
          farmerMap.set(farmerId, { score, timestamp })
        }
      }
    }

    lastEvaluatedKey = response.LastEvaluatedKey
  } while (lastEvaluatedKey)

  // Convert map to array
  return Array.from(farmerMap.entries()).map(([farmerId, data]) => ({
    farmerId,
    score: data.score,
  }))
}
