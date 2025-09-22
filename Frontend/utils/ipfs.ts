import axios from "axios"

// Pinata API configuration
const PINATA_API_KEY = process.env.NEXT_PUBLIC_PINATA_API_KEY
const PINATA_SECRET_API_KEY = process.env.NEXT_PUBLIC_PINATA_SECRET_API_KEY
const PINATA_JWT = process.env.NEXT_PUBLIC_PINATA_JWT

const PINATA_GATEWAY = "https://gateway.pinata.cloud/ipfs/"

/**
 * Upload JSON data to IPFS via Pinata
 * @param jsonData - The JSON data to upload
 * @returns The IPFS CID (Content Identifier)
 */
export async function uploadJsonToIPFS(jsonData: any): Promise<string> {
  try {
    const response = await axios.post("https://api.pinata.cloud/pinning/pinJSONToIPFS", jsonData, {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${PINATA_JWT}`,
      },
    })

    return response.data.IpfsHash
  } catch (error) {
    console.error("Error uploading JSON to IPFS:", error)
    throw new Error("Failed to upload to IPFS")
  }
}

/**
 * Upload a file to IPFS via Pinata
 * @param file - The file to upload
 * @returns The IPFS CID (Content Identifier)
 */
export async function uploadFileToIPFS(file: File): Promise<string> {
  try {
    const formData = new FormData()
    formData.append("file", file)

    const response = await axios.post("https://api.pinata.cloud/pinning/pinFileToIPFS", formData, {
      headers: {
        "Content-Type": "multipart/form-data",
        Authorization: `Bearer ${PINATA_JWT}`,
      },
    })

    return response.data.IpfsHash
  } catch (error) {
    console.error("Error uploading file to IPFS:", error)
    throw new Error("Failed to upload file to IPFS")
  }
}

/**
 * Get the IPFS gateway URL for a CID
 * @param cid - The IPFS CID
 * @returns The gateway URL
 */
export function getIPFSGatewayUrl(cid: string): string {
  return `${PINATA_GATEWAY}${cid}`
}

/**
 * Fetch JSON data from IPFS
 * @param cid - The IPFS CID
 * @returns The JSON data
 */
export async function fetchJsonFromIPFS(cid: string): Promise<any> {
  try {
    const response = await axios.get(getIPFSGatewayUrl(cid))
    return response.data
  } catch (error) {
    console.error("Error fetching JSON from IPFS:", error)
    throw new Error("Failed to fetch from IPFS")
  }
}

/**
 * Create marketplace product metadata
 * @param name - Product name
 * @param description - Product description
 * @param price - Product price in wei
 * @param imageCid - IPFS CID of the product image
 * @returns The product metadata object
 */
export function createProductMetadata(name: string, description: string, price: string, imageCid: string): any {
  return {
    name,
    description,
    price,
    image: getIPFSGatewayUrl(imageCid),
    attributes: [
      {
        trait_type: "Category",
        value: "Sustainable Farming",
      },
      {
        trait_type: "Created",
        value: new Date().toISOString(),
      },
    ],
  }
}
