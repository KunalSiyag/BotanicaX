"use client"

import { createContext, useContext, useState, useEffect, type ReactNode } from "react"
import { ethers } from "ethers"
import CertificationManagerABI from "@/abis/CertificationManager.json"
import SmartRewardsNFTABI from "@/abis/SmartRewardsNFT.json"
import RewardTokenABI from "@/abis/RewardToken.json"
import SubsidyManagerABI from "@/abis/SubsidyManager.json"
import MarketplaceAccessABI from "@/abis/MarketplaceAccess.json"
import EquipmentRentalABI from "@/abis/EquipmentRental.json"
import CarbonCreditsABI from "@/abis/CarbonCredits.json"
import LoanManagerABI from "@/abis/LoanManager.json"

// Contract addresses - these would come from environment variables in production
const CONTRACT_ADDRESSES = {
  certificationManager: process.env.NEXT_PUBLIC_CERTIFICATION_MANAGER_ADDRESS || "",
  smartRewardsNFT: process.env.NEXT_PUBLIC_SMART_REWARDS_NFT_ADDRESS || "",
  rewardToken: process.env.NEXT_PUBLIC_REWARD_TOKEN_ADDRESS || "",
  subsidyManager: process.env.NEXT_PUBLIC_SUBSIDY_MANAGER_ADDRESS || "",
  marketplaceAccess: process.env.NEXT_PUBLIC_MARKETPLACE_ACCESS_ADDRESS || "",
  equipmentRental: process.env.NEXT_PUBLIC_EQUIPMENT_RENTAL_ADDRESS || "",
  carbonCredits: process.env.NEXT_PUBLIC_CARBON_CREDITS_ADDRESS || "",
  loanManager: process.env.NEXT_PUBLIC_LOAN_MANAGER_ADDRESS || "",
}

interface Web3ContextType {
  account: string | null
  isOwner: boolean
  isConnecting: boolean
  isLoading: boolean
  error: string | null
  connectWallet: () => Promise<void>
  disconnectWallet: () => void
  certificationManager: ethers.Contract | null
  smartRewardsNFT: ethers.Contract | null
  rewardToken: ethers.Contract | null
  subsidyManager: ethers.Contract | null
  marketplaceAccess: ethers.Contract | null
  equipmentRental: ethers.Contract | null
  carbonCredits: ethers.Contract | null
  loanManager: ethers.Contract | null
  refreshUserData: () => Promise<void>
  userScore: number | null
  userTier: number | null
  userNFTBalance: number | null
  userTokenBalance: string | null
  userCarbonCredits: string | null
  userEligibleSubsidies: any[] | null
  userMarketplaceAccess: boolean
  userRentalDiscount: number | null
  userLoanEligibility: any[] | null
}

const Web3Context = createContext<Web3ContextType | undefined>(undefined)

export function Web3Provider({ children }: { children: ReactNode }) {
  const [account, setAccount] = useState<string | null>(null)
  const [isOwner, setIsOwner] = useState(false)
  const [isConnecting, setIsConnecting] = useState(false)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [provider, setProvider] = useState<ethers.BrowserProvider | null>(null)
  const [signer, setSigner] = useState<ethers.Signer | null>(null)

  // Contract instances
  const [certificationManager, setCertificationManager] = useState<ethers.Contract | null>(null)
  const [smartRewardsNFT, setSmartRewardsNFT] = useState<ethers.Contract | null>(null)
  const [rewardToken, setRewardToken] = useState<ethers.Contract | null>(null)
  const [subsidyManager, setSubsidyManager] = useState<ethers.Contract | null>(null)
  const [marketplaceAccess, setMarketplaceAccess] = useState<ethers.Contract | null>(null)
  const [equipmentRental, setEquipmentRental] = useState<ethers.Contract | null>(null)
  const [carbonCredits, setCarbonCredits] = useState<ethers.Contract | null>(null)
  const [loanManager, setLoanManager] = useState<ethers.Contract | null>(null)

  // User data
  const [userScore, setUserScore] = useState<number | null>(null)
  const [userTier, setUserTier] = useState<number | null>(null)
  const [userNFTBalance, setUserNFTBalance] = useState<number | null>(null)
  const [userTokenBalance, setUserTokenBalance] = useState<string | null>(null)
  const [userCarbonCredits, setUserCarbonCredits] = useState<string | null>(null)
  const [userEligibleSubsidies, setUserEligibleSubsidies] = useState<any[] | null>(null)
  const [userMarketplaceAccess, setUserMarketplaceAccess] = useState<boolean>(false)
  const [userRentalDiscount, setUserRentalDiscount] = useState<number | null>(null)
  const [userLoanEligibility, setUserLoanEligibility] = useState<any[] | null>(null)

  // Initialize provider from window.ethereum
  useEffect(() => {
    const init = async () => {
      try {
        if (typeof window !== "undefined" && window.ethereum) {
          const provider = new ethers.BrowserProvider(window.ethereum)
          setProvider(provider)
        }
      } catch (error) {
        console.error("Error initializing provider:", error)
        setError("Failed to initialize provider")
      } finally {
        setIsLoading(false)
      }
    }

    init()
  }, [])

  // Handle account changes
  useEffect(() => {
    if (!provider) return

    const handleAccountsChanged = (accounts: string[]) => {
      if (accounts.length === 0) {
        // User disconnected their wallet
        disconnectWallet()
      } else if (accounts[0] !== account) {
        setAccount(accounts[0])
        initializeContracts(accounts[0])
      }
    }

    const handleChainChanged = () => {
      window.location.reload()
    }

    if (window.ethereum) {
      window.ethereum.on("accountsChanged", handleAccountsChanged)
      window.ethereum.on("chainChanged", handleChainChanged)
    }

    return () => {
      if (window.ethereum) {
        window.ethereum.removeListener("accountsChanged", handleAccountsChanged)
        window.ethereum.removeListener("chainChanged", handleChainChanged)
      }
    }
  }, [provider, account])

  // Initialize contracts when account changes
  const initializeContracts = async (address: string) => {
    if (!provider) return

    try {
      setIsLoading(true)
      const signer = await provider.getSigner()
      setSigner(signer)

      // Initialize core contracts
      const certManager = new ethers.Contract(CONTRACT_ADDRESSES.certificationManager, CertificationManagerABI, signer)
      setCertificationManager(certManager)

      const nftContract = new ethers.Contract(CONTRACT_ADDRESSES.smartRewardsNFT, SmartRewardsNFTABI, signer)
      setSmartRewardsNFT(nftContract)

      const tokenContract = new ethers.Contract(CONTRACT_ADDRESSES.rewardToken, RewardTokenABI, signer)
      setRewardToken(tokenContract)

      // Initialize new contracts
      const subsidyContract = new ethers.Contract(CONTRACT_ADDRESSES.subsidyManager, SubsidyManagerABI, signer)
      setSubsidyManager(subsidyContract)

      const marketplaceContract = new ethers.Contract(
        CONTRACT_ADDRESSES.marketplaceAccess,
        MarketplaceAccessABI,
        signer,
      )
      setMarketplaceAccess(marketplaceContract)

      const rentalContract = new ethers.Contract(CONTRACT_ADDRESSES.equipmentRental, EquipmentRentalABI, signer)
      setEquipmentRental(rentalContract)

      const carbonContract = new ethers.Contract(CONTRACT_ADDRESSES.carbonCredits, CarbonCreditsABI, signer)
      setCarbonCredits(carbonContract)

      const loanContract = new ethers.Contract(CONTRACT_ADDRESSES.loanManager, LoanManagerABI, signer)
      setLoanManager(loanContract)

      // Check if the connected account is the owner
      const owner = await certManager.owner()
      setIsOwner(owner.toLowerCase() === address.toLowerCase())

      // Fetch user data
      await fetchUserData(
        address,
        certManager,
        nftContract,
        tokenContract,
        subsidyContract,
        marketplaceContract,
        rentalContract,
        carbonContract,
        loanContract,
      )
    } catch (error) {
      console.error("Error initializing contracts:", error)
      setError("Failed to initialize contracts")
    } finally {
      setIsLoading(false)
    }
  }

  // Fetch user data from contracts
  const fetchUserData = async (
    address: string,
    certManager: ethers.Contract,
    nftContract: ethers.Contract,
    tokenContract: ethers.Contract,
    subsidyContract: ethers.Contract,
    marketplaceContract: ethers.Contract,
    rentalContract: ethers.Contract,
    carbonContract: ethers.Contract,
    loanContract: ethers.Contract,
  ) => {
    try {
      // Get user's sustainability score
      const score = await certManager.getScore(address)
      setUserScore(Number(score))

      // Get user's NFT balance and tier
      const nftBalance = await nftContract.balanceOf(address)
      setUserNFTBalance(Number(nftBalance))

      if (Number(nftBalance)) {
        const tier = await nftContract.determineTier(score)
        setUserTier(Number(tier))
      } else {
        setUserTier(0)
      }

      // Get user's token balance
      const tokenBalance = await tokenContract.balanceOf(address)
      setUserTokenBalance(ethers.formatEther(tokenBalance))

      // Get user's eligible subsidies
      const eligibleSubsidies = await subsidyContract.getEligibleSubsidies(address)
      setUserEligibleSubsidies(eligibleSubsidies)

      // Check marketplace access
      const hasMarketplaceAccess = await marketplaceContract.canListInMarketplace(address)
      setUserMarketplaceAccess(hasMarketplaceAccess)

      // Get rental discount
      const rentalDiscount = await rentalContract.getDiscount(address)
      setUserRentalDiscount(Number(rentalDiscount))
      
      // Get carbon credits balance
      const carbonBalance = await carbonContract.balanceOf(address)
      setUserCarbonCredits(ethers.formatEther(carbonBalance))

      // Get loan eligibility
      const loanEligibility = await loanContract.getEligibleLoanTiers(address)
      setUserLoanEligibility(loanEligibility)
    } catch (error) {
      console.error("Error fetching user data:", error)
    }
  }

  // Refresh user data
  const refreshUserData = async () => {
    if (
      !account ||
      !certificationManager ||
      !smartRewardsNFT ||
      !rewardToken ||
      !subsidyManager ||
      !marketplaceAccess ||
      !equipmentRental ||
      !carbonCredits ||
      !loanManager
    )
      return

    try {
      await fetchUserData(
        account,
        certificationManager,
        smartRewardsNFT,
        rewardToken,
        subsidyManager,
        marketplaceAccess,
        equipmentRental,
        carbonCredits,
        loanManager,
      )
    } catch (error) {
      console.error("Error refreshing user data:", error)
    }
  }

  const connectWallet = async () => {
    if (!provider) {
      setError("No Ethereum provider found. Please install MetaMask.")
      return
    }

    try {
      setIsConnecting(true)
      setError(null)

      // Request account access
      const accounts = await provider.send("eth_requestAccounts", [])

      if (accounts.length === 0) {
        throw new Error("No accounts found")
      }

      setAccount(accounts[0])
      await initializeContracts(accounts[0])
    } catch (error: any) {
      console.error("Error connecting wallet:", error)
      setError(error.message || "Failed to connect wallet")
    } finally {
      setIsConnecting(false)
    }
  }

  const disconnectWallet = () => {
    setAccount(null)
    setSigner(null)
    setCertificationManager(null)
    setSmartRewardsNFT(null)
    setRewardToken(null)
    setSubsidyManager(null)
    setMarketplaceAccess(null)
    setEquipmentRental(null)
    setCarbonCredits(null)
    setLoanManager(null)
    setIsOwner(false)
    setUserScore(null)
    setUserTier(null)
    setUserNFTBalance(null)
    setUserTokenBalance(null)
    setUserCarbonCredits(null)
    setUserEligibleSubsidies(null)
    setUserMarketplaceAccess(false)
    setUserRentalDiscount(null)
    setUserLoanEligibility(null)
  }

  return (
    <Web3Context.Provider
      value={{
        account,
        isOwner,
        isConnecting,
        isLoading,
        error,
        connectWallet,
        disconnectWallet,
        certificationManager,
        smartRewardsNFT,
        rewardToken,
        subsidyManager,
        marketplaceAccess,
        equipmentRental,
        carbonCredits,
        loanManager,
        refreshUserData,
        userScore,
        userTier,
        userNFTBalance,
        userTokenBalance,
        userCarbonCredits,
        userEligibleSubsidies,
        userMarketplaceAccess,
        userRentalDiscount,
        userLoanEligibility,
      }}
    >
      {children}
    </Web3Context.Provider>
  )
}

export function useWeb3() {
  const context = useContext(Web3Context)
  if (context === undefined) {
    throw new Error("useWeb3 must be used within a Web3Provider")
  }
  return context
}
