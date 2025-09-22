import { NextResponse } from "next/server"

// Mock function to get discount
const getDiscountMock = async (address: string): Promise<number> => {
  // This would normally come from your blockchain interaction
  const discounts: Record<string, number> = {
    "0x1234567890123456789012345678901234567890": 15,
    "0x2345678901234567890123456789012345678901": 20,
    "0x3456789012345678901234567890123456789012": 10,
    "0x4567890123456789012345678901234567890123": 5,
  }

  return discounts[address] || 0 // Default discount if address not found
}

export async function GET(request: Request, { params }: { params: { address: string } }) {
  try {
    const address = params.address

    if (!address || typeof address !== "string") {
      return NextResponse.json({ error: "Valid address is required" }, { status: 400 })
    }

    // Get the discount percentage for the address
    const discount = await getDiscountMock(address)

    // Sample equipment data
    const equipment = [
      {
        id: 1,
        name: "Precision Seeder",
        description: "High-precision seeding equipment for optimal spacing",
        basePrice: 0.5, // MATIC per day
        image: "/placeholder.svg?height=200&width=200",
      },
      {
        id: 2,
        name: "Drone Sprayer",
        description: "Automated drone for precise pesticide application",
        basePrice: 0.8, // MATIC per day
        image: "/placeholder.svg?height=200&width=200",
      },
      {
        id: 3,
        name: "Smart Irrigation System",
        description: "Water-efficient irrigation with soil moisture sensors",
        basePrice: 0.6, // MATIC per day
        image: "/placeholder.svg?height=200&width=200",
      },
      {
        id: 4,
        name: "Harvesting Robot",
        description: "Automated harvesting for delicate crops",
        basePrice: 1.0, // MATIC per day
        image: "/placeholder.svg?height=200&width=200",
      },
    ]

    // Calculate discounted prices for each equipment
    const equipmentWithPrices = equipment.map((item) => ({
      ...item,
      discountedPrice: Number.parseFloat((item.basePrice * (1 - discount / 100)).toFixed(2)),
    }))

    return NextResponse.json({
      discount,
      equipment: equipmentWithPrices,
    })
  } catch (error) {
    console.error("Error fetching rental discount:", error)
    return NextResponse.json({ error: "Failed to fetch rental discount" }, { status: 500 })
  }
}
