import { DynamoDBClient } from "@aws-sdk/client-dynamodb"
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb"
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

// Sensor data processing function
export const handler = async (event: any) => {
  try {
    // Parse the incoming sensor data
    const sensorData = JSON.parse(event.body)
    const { farmerId, timestamp, readings } = sensorData

    if (!farmerId || !readings) {
      return {
        statusCode: 400,
        body: JSON.stringify({ message: "Missing required fields: farmerId or readings" }),
      }
    }

    // Store raw sensor data in DynamoDB
    await dynamo.send(
      new PutCommand({
        TableName: TABLE_NAME,
        Item: {
          farmerId,
          timestamp: timestamp || Date.now(),
          readings,
          processed: false,
        },
      }),
    )

    // Process the sensor data to calculate sustainability score
    const score = calculateSustainabilityScore(readings)

    // Store the calculated score in DynamoDB
    await dynamo.send(
      new PutCommand({
        TableName: TABLE_NAME,
        Item: {
          farmerId,
          timestamp: Date.now(),
          score,
          processed: true,
        },
      }),
    )

    // Update the blockchain with the new score
    await updateBlockchainScore(farmerId, score)

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Sensor data processed successfully",
        farmerId,
        score,
      }),
    }
  } catch (error) {
    console.error("Error processing sensor data:", error)
    return {
      statusCode: 500,
      body: JSON.stringify({ message: "Error processing sensor data", error: String(error) }),
    }
  }
}

// Function to calculate sustainability score from sensor readings
function calculateSustainabilityScore(readings: any): number {
  // Extract relevant metrics from readings
  const {
    soilMoisture = [],
    temperature = [],
    humidity = [],
    soilNutrients = {},
    waterUsage = [],
    pesticidesUsed = [],
    organicMatterPercentage = 0,
    biodiversityIndex = 0,
    carbonFootprint = 0,
  } = readings

  // Calculate individual scores for each metric

  // Soil moisture score (optimal range: 30-60%)
  const avgSoilMoisture = soilMoisture.reduce((sum: number, val: number) => sum + val, 0) / (soilMoisture.length || 1)
  const soilMoistureScore = calculateOptimalRangeScore(avgSoilMoisture, 30, 60, 100)

  // Temperature score (depends on crop type, using generic range: 15-30°C)
  const avgTemperature = temperature.reduce((sum: number, val: number) => sum + val, 0) / (temperature.length || 1)
  const temperatureScore = calculateOptimalRangeScore(avgTemperature, 15, 30, 100)

  // Humidity score (optimal range: 40-70%)
  const avgHumidity = humidity.reduce((sum: number, val: number) => sum + val, 0) / (humidity.length || 1)
  const humidityScore = calculateOptimalRangeScore(avgHumidity, 40, 70, 100)

  // Soil nutrients score
  const nutrientLevels = {
    nitrogen: soilNutrients.nitrogen || 0,
    phosphorus: soilNutrients.phosphorus || 0,
    potassium: soilNutrients.potassium || 0,
    ph: soilNutrients.ph || 7,
  }

  const nutrientScore = calculateNutrientScore(nutrientLevels)

  // Water usage score (lower is better)
  const avgWaterUsage = waterUsage.reduce((sum: number, val: number) => sum + val, 0) / (waterUsage.length || 1)
  const waterUsageScore = 100 - Math.min(100, (avgWaterUsage / 10) * 100)

  // Pesticide usage score (lower is better)
  const totalPesticides = pesticidesUsed.reduce((sum: number, val: number) => sum + val, 0)
  const pesticideScore = 100 - Math.min(100, (totalPesticides / 5) * 100)

  // Organic matter score (higher is better, optimal: >5%)
  const organicMatterScore = Math.min(100, (organicMatterPercentage / 5) * 100)

  // Biodiversity score (higher is better, scale 0-10)
  const biodiversityScore = Math.min(100, (biodiversityIndex / 10) * 100)

  // Carbon footprint score (lower is better)
  const carbonScore = 100 - Math.min(100, (carbonFootprint / 50) * 100)

  // Calculate weighted average for final score
  const weights = {
    soilMoisture: 0.1,
    temperature: 0.05,
    humidity: 0.05,
    nutrients: 0.2,
    waterUsage: 0.15,
    pesticides: 0.15,
    organicMatter: 0.1,
    biodiversity: 0.1,
    carbon: 0.1,
  }

  const weightedScore =
    soilMoistureScore * weights.soilMoisture +
    temperatureScore * weights.temperature +
    humidityScore * weights.humidity +
    nutrientScore * weights.nutrients +
    waterUsageScore * weights.waterUsage +
    pesticideScore * weights.pesticides +
    organicMatterScore * weights.organicMatter +
    biodiversityScore * weights.biodiversity +
    carbonScore * weights.carbon

  // Scale to 0-1000 range and round to nearest integer
  return Math.round(weightedScore * 10)
}

// Helper function to calculate score based on optimal range
function calculateOptimalRangeScore(value: number, min: number, max: number, maxScore: number): number {
  if (value >= min && value <= max) {
    return maxScore
  } else if (value < min) {
    return maxScore * (value / min)
  } else {
    return maxScore * (1 - (value - max) / max)
  }
}

// Helper function to calculate nutrient score
function calculateNutrientScore(nutrients: any): number {
  // Optimal ranges for nutrients
  const optimal = {
    nitrogen: { min: 20, max: 40 },
    phosphorus: { min: 10, max: 20 },
    potassium: { min: 15, max: 30 },
    ph: { min: 6, max: 7.5 },
  }

  const nScore = calculateOptimalRangeScore(nutrients.nitrogen, optimal.nitrogen.min, optimal.nitrogen.max, 100)
  const pScore = calculateOptimalRangeScore(nutrients.phosphorus, optimal.phosphorus.min, optimal.phosphorus.max, 100)
  const kScore = calculateOptimalRangeScore(nutrients.potassium, optimal.potassium.min, optimal.potassium.max, 100)
  const phScore = calculateOptimalRangeScore(nutrients.ph, optimal.ph.min, optimal.ph.max, 100)

  return (nScore + pScore + kScore + phScore) / 4
}

// Function to update the blockchain with the new score
async function updateBlockchainScore(farmerId: string, score: number): Promise<void> {
  try {
    // Connect to the blockchain
    const provider = new ethers.JsonRpcProvider(RPC_URL)
    const wallet = new ethers.Wallet(TRUSTED_FORWARDER_PRIVATE_KEY, provider)
    const certificationManager = new ethers.Contract(CERTIFICATION_MANAGER_ADDRESS, CERTIFICATION_MANAGER_ABI, wallet)

    // Get current score from blockchain
    const currentScore = await certificationManager.getScore(farmerId)

    // Only update if the new score is significantly different (>5% change)
    if (Math.abs(Number(currentScore) - score) > Number(currentScore) * 0.05) {
      // Update the score on the blockchain
      const tx = await certificationManager.updateScore(farmerId, score)
      await tx.wait()
      console.log(`Updated score for ${farmerId} to ${score}`)
    } else {
      console.log(`Score change not significant enough to update blockchain`)
    }
  } catch (error) {
    console.error("Error updating blockchain score:", error)
    throw error
  }
}
