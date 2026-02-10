package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	baseURL       = "http://localhost:3068/v2"
	ledgerName    = "default"
	currency      = "USD"
	concurrency   = 10 // Reduced from 50
	initBalance   = 5000
	debitAmount   = 50
	totalAttempts = 200
	httpTimeout   = 30 * time.Second // Increased timeout
)

var client = &http.Client{
	Timeout: httpTimeout,
}

type CreateWalletRequest struct {
	UserID   string `json:"userID"`
	Currency string `json:"currency"`
}

type WalletTransactionRequest struct {
	Amount    int64             `json:"amount"`
	Reference string            `json:"reference"`
	Metadata  map[string]string `json:"metadata"`
}

func main() {
	fmt.Println("Starting Concurrent Debit Test (Drain Wallet)...")

	// 1. Create Wallet
	userID := "user-drain-" + uuid.NewString()[:8]
	walletID := createWallet(userID, currency)
	if walletID == "" {
		panic("Failed to create wallet")
	}
	fmt.Printf("Created Wallet: %s\n", walletID)

	// 2. Fund Wallet
	if !creditWallet(walletID, initBalance, "init-"+uuid.NewString()) {
		panic("Failed to fund wallet")
	}
	fmt.Printf("Funded Wallet with %d %s\n", initBalance, currency)

	// 3. Concurrent Debits
	var successCount uint64
	var failCount uint64

	tasks := make(chan int, totalAttempts)
	for i := 0; i < totalAttempts; i++ {
		tasks <- i
	}
	close(tasks)

	var wg sync.WaitGroup
	start := time.Now()

	fmt.Printf("Launching %d workers to execute %d debits of %d %s each...\n", concurrency, totalAttempts, debitAmount, currency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tasks {
				ref := uuid.NewString()
				if debitWallet(walletID, debitAmount, ref) {
					atomic.AddUint64(&successCount, 1)
				} else {
					atomic.AddUint64(&failCount, 1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	// 4. Analysis
	fmt.Printf("\nTest Completed in %v\n", elapsed)
	fmt.Printf("Total Attempts: %d\n", totalAttempts)
	fmt.Printf("Success: %d\n", successCount)
	fmt.Printf("Fail: %d\n", failCount)

	expectedSuccess := uint64(initBalance / debitAmount)
	fmt.Printf("Expected Success: %d\n", expectedSuccess)

	if successCount != expectedSuccess {
		fmt.Printf("ERROR: Success count mismatch! Got %d, want %d\n", successCount, expectedSuccess)
	} else {
		fmt.Println("SUCCESS: Success count matches expected drain count.")
	}

	// 5. Verify Balance
	balance := getBalance(userID, currency)
	fmt.Printf("Final Balance: %d\n", balance)

	if balance == 0 {
		fmt.Println("SUCCESS: Final balance is 0.")
	} else {
		fmt.Printf("ERROR: Final balance is %d (Expected 0)\n", balance)
	}

	if balance < 0 {
		fmt.Println("CRITICAL FAILURE: Negative balance detected!")
	}
}

func createWallet(userID, currency string) string {
	url := fmt.Sprintf("%s/%s/wallets", baseURL, ledgerName)
	reqBody := CreateWalletRequest{UserID: userID, Currency: currency}
	body, _ := json.Marshal(reqBody)

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("CreateWallet Error: %v\n", err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		respBytes, _ := io.ReadAll(resp.Body)
		fmt.Printf("CreateWallet Failed: %d %s\n", resp.StatusCode, string(respBytes))
		return ""
	}

	var res map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&res)

	if data, ok := res["data"].(map[string]interface{}); ok {
		if id, ok := data["walletID"].(string); ok {
			return id
		}
	}
	if id, ok := res["walletID"].(string); ok {
		return id
	}
	return ""
}

func creditWallet(walletID string, amount int64, ref string) bool {
	url := fmt.Sprintf("%s/%s/wallets/%s/credit", baseURL, ledgerName, walletID)
	reqBody := WalletTransactionRequest{Amount: amount, Reference: ref}
	body, _ := json.Marshal(reqBody)

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("Credit Error: %v\n", err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusCreated
}

func debitWallet(walletID string, amount int64, ref string) bool {
	url := fmt.Sprintf("%s/%s/wallets/%s/debit", baseURL, ledgerName, walletID)
	reqBody := WalletTransactionRequest{Amount: amount, Reference: ref}
	body, _ := json.Marshal(reqBody)

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("Debit Transport Error: %v\n", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		// Log only non-400 errors or specific 400 errors?
		// We expect 400 Insufficient Funds eventually.
		// But if we get 500 or something else, we want to know.
		if resp.StatusCode != http.StatusBadRequest {
			respBytes, _ := io.ReadAll(resp.Body)
			fmt.Printf("Debit Failed: %d %s\n", resp.StatusCode, string(respBytes))
		}
		return false
	}
	return true
}

func getBalance(userID, currency string) int64 {
	// Query Ledger Account directly
	// Address: users:{userID}:wallets:{currency}:available
	accountAddr := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)
	// URL encode the address (replace : with %3A) just in case
	encodedAddr := strings.ReplaceAll(accountAddr, ":", "%3A")
	url := fmt.Sprintf("%s/%s/accounts/%s", baseURL, ledgerName, encodedAddr)

	fmt.Printf("Debug: Checking Balance at %s\n", url)

	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("GetBalance Error: %v\n", err)
		return -999999
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		fmt.Printf("GetBalance Failed: %d %s\n", resp.StatusCode, string(respBytes))
		return -999999
	}

	var res struct {
		Data struct {
			Balances map[string]int64 `json:"balances"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		fmt.Printf("GetBalance Decode Error: %v\n", err)
		return -999999
	}

	return res.Data.Balances[currency]
}
