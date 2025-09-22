"use client"

import type React from "react"
import { useState, useEffect, useRef } from "react"
import { useWeb3 } from "@/context/web3-context"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Loader2, AlertCircle, ShoppingBag, Tag, Lock, Upload, ImageIcon } from "lucide-react"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import DashboardLayout from "@/components/dashboard/layout"
import { useToast } from "@/components/ui/use-toast"
import { uploadFileToIPFS, uploadJsonToIPFS, createProductMetadata, fetchJsonFromIPFS } from "@/utils/ipfs"
import { ethers } from "ethers"

export default function MarketplacePage() {
  const { account, marketplaceAccess, userMarketplaceAccess, refreshUserData } = useWeb3()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [products, setProducts] = useState<any[]>([])
  const [isUploading, setIsUploading] = useState(false)
  const { toast } = useToast()
  const fileInputRef = useRef<HTMLInputElement>(null)

  // Form state
  const [productName, setProductName] = useState("")
  const [productDescription, setProductDescription] = useState("")
  const [productPrice, setProductPrice] = useState("")
  const [productImage, setProductImage] = useState<File | null>(null)
  const [previewUrl, setPreviewUrl] = useState<string | null>(null)

  useEffect(() => {
    if (account) {
      fetchProducts()
    }
  }, [account])

  const fetchProducts = async () => {
    if (!marketplaceAccess) return

    try {
      setLoading(true)
      setError(null)

      // Get products from the marketplace contract
      const pageSize = 10
      const products = await marketplaceAccess.getProducts(1, pageSize)

      // Fetch metadata from IPFS for each product
      const productsWithMetadata = await Promise.all(
        products.map(async (product: any) => {
          try {
            // Ensure product.price is valid
            const price = product.price ? ethers.formatEther(product.price) : "0"
            const metadata = await fetchJsonFromIPFS(product.imageURI)
            return {
              ...product,
              price,
              metadata,
            }
          } catch (err) {
            console.error(`Error fetching metadata for product ${product.id || 'unknown'}:`, err)
            return {
              ...product,
              price: "0",
              metadata: {
                name: product.name || "Unknown",
                description: product.description || "No description",
                image: "/placeholder.svg?height=200&width=200",
              },
            }
          }
        })
      )

      setProducts(productsWithMetadata)
    } catch (err: any) {
      console.error("Error fetching products:", err)
      setError(err.message || "Failed to fetch products")
    } finally {
      setLoading(false)
    }
  }

  const handleImageChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const file = e.target.files[0]
      setProductImage(file)

      // Create preview URL
      const reader = new FileReader()
      reader.onloadend = () => {
        setPreviewUrl(reader.result as string)
      }
      reader.readAsDataURL(file)
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!marketplaceAccess || !account || !productName || !productDescription || !productPrice || !productImage) {
      toast({
        title: "Missing Information",
        description: "Please fill in all fields and upload an image",
        variant: "destructive",
      })
      return
    }

    // Validate productPrice
    const priceValue = parseFloat(productPrice)
    if (isNaN(priceValue) || priceValue <= 0) {
      toast({
        title: "Invalid Price",
        description: "Please enter a valid price greater than 0",
        variant: "destructive",
      })
      return
    }

    try {
      setIsUploading(true)

      // 1. Upload image to IPFS
      const imageCid = await uploadFileToIPFS(productImage)

      // 2. Create and upload metadata to IPFS
      const metadata = createProductMetadata(
        productName,
        productDescription,
        ethers.parseEther(productPrice).toString(),
        imageCid
      )
      const metadataCid = await uploadJsonToIPFS(metadata)

      // 3. List product on the blockchain
      const priceInWei = ethers.parseEther(productPrice)
      const tx = await marketplaceAccess.listProduct(
        productName,
        productDescription,
        priceInWei,
        metadataCid
      )

      await tx.wait()

      toast({
        title: "Product Listed",
        description: `Successfully listed "${productName}" on the marketplace`,
      })

      // Reset form
      setProductName("")
      setProductDescription("")
      setProductPrice("")
      setProductImage(null)
      setPreviewUrl(null)

      // Refresh products
      await fetchProducts()
    } catch (err: any) {
      console.error("Error listing product:", err)
      toast({
        title: "Error",
        description: err.message || "Failed to list product",
        variant: "destructive",
      })
    } finally {
      setIsUploading(false)
    }
  }

  const handlePurchase = async (productId: number, price: string) => {
    if (!marketplaceAccess) return

    try {
      setLoading(true)

      // Validate price
      const priceValue = parseFloat(price)
      if (isNaN(priceValue) || priceValue <= 0) {
        throw new Error("Invalid product price")
      }

      const priceInWei = ethers.parseEther(price)
      const tx = await marketplaceAccess.purchaseProduct(productId, {
        value: priceInWei,
      })

      await tx.wait()

      toast({
        title: "Purchase Successful",
        description: "You have successfully purchased this product",
      })

      // Refresh products
      await fetchProducts()
    } catch (err: any) {
      console.error("Error purchasing product:", err)
      toast({
        title: "Error",
        description: err.message || "Failed to purchase product",
        variant: "destructive",
      })
    } finally {
      setLoading(false)
    }
  }

  if (!account) {
    return (
      <DashboardLayout>
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Not connected</AlertTitle>
          <AlertDescription>Please connect your wallet to access the marketplace.</AlertDescription>
        </Alert>
      </DashboardLayout>
    )
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Sustainable Farming Marketplace</h1>
          <p className="text-muted-foreground">Buy and sell sustainable farming products and services.</p>
        </div>

        {loading && !products.length ? (
          <div className="flex justify-center p-12">
            <Loader2 className="h-8 w-8 animate-spin text-green-600" />
          </div>
        ) : error ? (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : !userMarketplaceAccess ? (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <Lock className="mr-2 h-5 w-5" />
                Marketplace Access Restricted
              </CardTitle>
              <CardDescription>You need to be certified to list items in the marketplace.</CardDescription>
            </CardHeader>
            <CardContent>
              <Alert className="mb-4">
                <AlertCircle className="h-4 w-4" />
                <AlertTitle>Get Certified</AlertTitle>
                <AlertDescription>
                  Increase your sustainability score to at least 700 or mint an NFT to gain marketplace access.
                </AlertDescription>
              </Alert>
              <p className="text-sm text-muted-foreground mb-4">
                Certification requires a minimum sustainability score of 700. Once certified, you'll receive an NFT that
                grants you access to the marketplace.
              </p>
            </CardContent>
            <CardFooter>
              <Button onClick={() => refreshUserData()} className="w-full bg-green-600 hover:bg-green-700">
                Check Eligibility
              </Button>
            </CardFooter>
          </Card>
        ) : (
          <>
            <Card>
              <CardHeader>
                <CardTitle>List a New Product</CardTitle>
                <CardDescription>Share your sustainable farming products with the community.</CardDescription>
              </CardHeader>
              <CardContent>
                <form onSubmit={handleSubmit} className="space-y-4">
                  <div className="space-y-2">
                    <Label htmlFor="productName">Product Name</Label>
                    <Input
                      id="productName"
                      value={productName}
                      onChange={(e) => setProductName(e.target.value)}
                      placeholder="Organic Fertilizer"
                      required
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="productDescription">Description</Label>
                    <Textarea
                      id="productDescription"
                      value={productDescription}
                      onChange={(e) => setProductDescription(e.target.value)}
                      placeholder="Describe your product..."
                      required
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="productPrice">Price (MATIC)</Label>
                    <Input
                      id="productPrice"
                      type="number"
                      min="0"
                      step="0.01"
                      value={productPrice}
                      onChange={(e) => setProductPrice(e.target.value)}
                      placeholder="0.00"
                      required
                    />
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="productImage">Product Image</Label>
                    <div className="flex items-center gap-4">
                      <Button
                        type="button"
                        variant="outline"
                        onClick={() => fileInputRef.current?.click()}
                        className="flex-1"
                      >
                        <Upload className="mr-2 h-4 w-4" />
                        Upload Image
                      </Button>
                      <input
                        ref={fileInputRef}
                        type="file"
                        id="productImage"
                        accept="image/*"
                        onChange={handleImageChange}
                        className="hidden"
                      />
                      {previewUrl && (
                        <div className="relative w-16 h-16 rounded-md overflow-hidden border">
                          <img
                            src={previewUrl || "/placeholder.svg"}
                            alt="Preview"
                            className="w-full h-full object-cover"
                          />
                        </div>
                      )}
                    </div>
                  </div>

                  <Button type="submit" className="w-full bg-green-600 hover:bg-green-700" disabled={isUploading}>
                    {isUploading ? (
                      <>
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        Uploading to IPFS...
                      </>
                    ) : (
                      <>
                        <Tag className="mr-2 h-4 w-4" />
                        List Product
                      </>
                    )}
                  </Button>
                </form>
              </CardContent>
            </Card>

            <div>
              <h2 className="text-xl font-semibold mb-4">Marketplace Listings</h2>
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {products.length > 0 ? (
                  products.map((product, index) => (
                    <Card key={product.id != null ? product.id.toString() : `product-${index}`}>
                      <div className="aspect-square w-full overflow-hidden">
                        <img
                          src={product.metadata?.image || "/placeholder.svg?height=300&width=300"}
                          alt={product.metadata?.name || product.name}
                          className="h-full w-full object-cover"
                        />
                      </div>
                      <CardHeader>
                        <CardTitle>{product.metadata?.name || product.name}</CardTitle>
                        <CardDescription>{product.metadata?.description || product.description}</CardDescription>
                      </CardHeader>
                      <CardContent>
                        <p className="font-bold text-lg">{product.price} MATIC</p>
                        <p className="text-sm text-muted-foreground">
                          Seller: {product.seller && typeof product.seller === "string" ? 
                            `${product.seller.slice(0, 6)}...${product.seller.slice(-4)}` : 
                            "Unknown Seller"}
                        </p>
                      </CardContent>
                      <CardFooter>
                        <Button
                          className="w-full"
                          disabled={!product.active || loading}
                          onClick={() => handlePurchase(product.id, product.price)}
                        >
                          {loading ? (
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                          ) : (
                            <ShoppingBag className="mr-2 h-4 w-4" />
                          )}
                          {!product.active ? "Sold" : "Buy Now"}
                        </Button>
                      </CardFooter>
                    </Card>
                  ))
                ) : (
                  <Card className="col-span-full">
                    <CardHeader>
                      <CardTitle>No Products Listed</CardTitle>
                      <CardDescription>Be the first to list a product in the marketplace!</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="flex justify-center p-6">
                        <ImageIcon className="h-16 w-16 text-muted-foreground/30" />
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>
            </div>
          </>
        )}
      </div>
    </DashboardLayout>
  )
}