"use client"
// Carbon Credits Claiming Page
import { useState, useEffect } from "react"
import { useWeb3 } from "@/context/web3-context"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Loader2, AlertCircle, Leaf } from "lucide-react"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import DashboardLayout from "@/components/dashboard-layout"
import CARBON_CREDITS_ABI from "@/abis/CarbonCredits.json";
import { ethers } from "ethers"

export default function CarbonPage() {
  const { account } = useWeb3()
  const [credits, setCredits] = useState("")
  const [currentCredits, setCurrentCredits] = useState(0)
  const [maxCredits, setMaxCredits] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [success, setSuccess] = useState(null)
  const [currentCreditsINR, setCurrentCreditsINR] = useState(0)
  const [maxCreditsINR, setMaxCreditsINR] = useState(0)
  const [lastClaimTime, setLastClaimTime] = useState(null)
  const [waitTime, setWaitTime] = useState(null)

  const CARBON_CREDITS_ADDRESS = process.env.NEXT_PUBLIC_CARBON_CREDITS_ADDRESS || "0xAbE39B883651a644Eb0661229E6789ed64bA9A97";
  const EXCHANGE_RATE = 80; // Example: 1 token = 80 INR (adjust accordingly)

  // Fetch current balance, max credits, and last claim time on component load
  useEffect(() => {
    if (account) {
      fetchCurrentCredits();
      fetchMaxCredits();
      fetchLastClaimTime();
    }
  }, [account]);

  // Fetch current carbon credits for the account
  const fetchCurrentCredits = async () => {
    if (!account) return;
    try {
      const provider = new ethers.BrowserProvider(window.ethereum);
      const signer = await provider.getSigner();
      const contract = new ethers.Contract(CARBON_CREDITS_ADDRESS, CARBON_CREDITS_ABI, signer);
      const balance = await contract.balanceOf(account);
      const formattedBalance = ethers.formatUnits(balance, 18); // Assuming the token has 18 decimals
      setCurrentCredits(formattedBalance);
      setCurrentCreditsINR(formattedBalance * EXCHANGE_RATE); // Convert to INR
    } catch (err) {
      console.error("Error fetching current credits:", err);
    }
  }

  // Fetch max credits the farmer can claim
  const fetchMaxCredits = async () => {
    if (!account) return;
    try {
      const provider = new ethers.BrowserProvider(window.ethereum);
      const signer = await provider.getSigner();
      const contract = new ethers.Contract(CARBON_CREDITS_ADDRESS, CARBON_CREDITS_ABI, signer);
      const maxCredits = await contract.getMaxCredits(account);
      const formattedMaxCredits = ethers.formatUnits(maxCredits, 18); // Assuming the token has 18 decimals
      setMaxCredits(formattedMaxCredits);
      setMaxCreditsINR(formattedMaxCredits * EXCHANGE_RATE); // Convert to INR
    } catch (err) {
      console.error("Error fetching max credits:", err);
    }
  }

  // Fetch the last claim time
  const fetchLastClaimTime = async () => {
    if (!account) return;
    try {
      const provider = new ethers.BrowserProvider(window.ethereum);
      const signer = await provider.getSigner();
      const contract = new ethers.Contract(CARBON_CREDITS_ADDRESS, CARBON_CREDITS_ABI, signer);
      
      // Fetch last claim time (Unix timestamp)
      const lastClaim = await contract.lastClaimTime(account);
      
      // Convert to a string to handle BigNumber cases
      const lastClaimTime = lastClaim.toString();
      setLastClaimTime(lastClaimTime);

      // Calculate the wait time (in days) from last claim time
      const currentTime = Date.now();
      const timeDiff = currentTime - (lastClaimTime * 1000); // Convert to milliseconds
      const daysPassed = timeDiff / (1000 * 60 * 60 * 24); // Convert milliseconds to days

      if (daysPassed < 30) {
        setWaitTime(Math.ceil(30 - daysPassed)); // Show how many days left to wait
      } else {
        setWaitTime(0); // No wait time, claimable
      }
    } catch (err) {
      console.error("Error fetching last claim time:", err);
    }
  }

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!account || !credits || Number(credits) <= 0 || Number(credits) > maxCredits) return;

    try {
      setLoading(true);
      setError(null);
      setSuccess(null);

      // Get browser wallet signer
      const provider = new ethers.BrowserProvider(window.ethereum);
      const signer = await provider.getSigner();

      const contract = new ethers.Contract(CARBON_CREDITS_ADDRESS, CARBON_CREDITS_ABI, signer);

      const tx = await contract.claimCredits(ethers.parseEther(credits));
      await tx.wait();

      setSuccess(`Successfully claimed ${credits} carbon credits!`);
      setCredits("");
      fetchCurrentCredits(); // Refresh current credits
      fetchLastClaimTime(); // Refresh last claim time
    } catch (err) {
      console.error("Error claiming carbon credits:", err);
      setError(err.message || "Failed to claim carbon credits");
    } finally {
      setLoading(false);
    }
  };

  if (!account) {
    return (
      <DashboardLayout>
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Not connected</AlertTitle>
          <AlertDescription>Please connect your wallet to claim carbon credits.</AlertDescription>
        </Alert>
      </DashboardLayout>
    )
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Carbon Credits</h1>
          <p className="text-muted-foreground">Claim carbon credits for your sustainable farming practices.</p>
        </div>

        <div className="grid gap-6 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Claim Carbon Credits</CardTitle>
              <CardDescription>
                Enter the amount of carbon credits you want to claim based on your sustainable practices.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {error && (
                <Alert variant="destructive" className="mb-4">
                  <AlertCircle className="h-4 w-4" />
                  <AlertTitle>Error</AlertTitle>
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}

              {success && (
                <Alert className="mb-4 bg-green-50 text-green-800 border-green-200">
                  <Leaf className="h-4 w-4" />
                  <AlertTitle>Success</AlertTitle>
                  <AlertDescription>{success}</AlertDescription>
                </Alert>
              )}

              <div className="mb-4">
                <span>Current Credits: {currentCredits} ({currentCreditsINR} INR)</span>
                <br />
                <span>Max Credits Available: {maxCredits} ({maxCreditsINR} INR)</span>
                <br />
                {waitTime > 0 ? (
                  <span className="text-red-500">You must wait {waitTime} more days to claim.</span>
                ) : (
                  <span className="text-green-500">You can claim now!</span>
                )}
              </div>

              <form onSubmit={handleSubmit} className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="credits">Carbon Credits Amount (INR)</Label>
                  <Input
                    id="credits"
                    type="number"
                    min="1"
                    step="1"
                    value={credits}
                    onChange={(e) => setCredits(e.target.value)}
                    placeholder="Enter amount in INR"
                    required
                  />
                </div>
              </form>
            </CardContent>
            <CardFooter>
              <Button
                onClick={handleSubmit}
                disabled={loading || !credits || Number(credits) <= 0 || Number(credits) > maxCreditsINR || waitTime > 0}
                className="w-full bg-green-600 hover:bg-green-700"
              >
                {loading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Processing...
                  </>
                ) : (
                  <>
                    <Leaf className="mr-2 h-4 w-4" />
                    Claim Carbon Credits
                  </>
                )}
              </Button>
            </CardFooter>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>About Carbon Credits</CardTitle>
              <CardDescription>Understanding carbon credits in sustainable agriculture</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <h3 className="font-medium">What are Carbon Credits?</h3>
                <p className="text-sm text-muted-foreground">
                  Carbon credits represent the reduction, avoidance, or removal of greenhouse gas emissions from the
                  atmosphere. In agriculture, these credits are earned through sustainable farming practices.
                </p>
              </div>

              <div>
                <h3 className="font-medium">How to Earn Credits</h3>
                <ul className="text-sm text-muted-foreground list-disc pl-5 space-y-1">
                  <li>Implement no-till or reduced tillage practices</li>
                  <li>Plant cover crops to sequester carbon</li>
                  <li>Improve livestock management to reduce methane emissions</li>
                  <li>Use precision agriculture to optimize fertilizer application</li>
                  <li>Restore degraded farmland</li>
                </ul>
              </div>

              <div>
                <h3 className="font-medium">Benefits</h3>
                <p className="text-sm text-muted-foreground">
                  Carbon credits can be sold on carbon markets, providing additional income for farmers while
                  contributing to climate change mitigation.
                </p>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </DashboardLayout>
  )
}
