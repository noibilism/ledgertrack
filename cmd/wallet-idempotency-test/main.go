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
	baseURL     = "http://localhost:3068/v2"
	ledgerName  = "default"
	currency    = "USD"
	concurrency = 10
	initBalance = 500
	debitAmount = 50
	httpTimeout = 30 * time.Second
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
	fmt.Println("Starting Idempotency Tests...")

	testSameIdempotencyKey()
	testDifferentIdempotencyKey()
}

func testSameIdempotencyKey() {
	fmt.Println("\n--- Test 1: Concurrent Debits with SAME Idempotency Key ---")

	// 1. Create Wallet
	userID := "user-idem-same-" + uuid.NewString()[:8]
	walletID := createWallet(userID, currency)
	if walletID == "" {
		panic("Failed to create wallet")
	}
	fmt.Printf("Created Wallet: %s\n", walletID)

	// 2. Fund Wallet
	if !creditWallet(walletID, initBalance, "init-"+uuid.NewString(), "") {
		panic("Failed to fund wallet")
	}
	fmt.Printf("Funded Wallet with %d %s\n", initBalance, currency)

	// 3. Concurrent Debits with SAME Key
	ik := "ik-" + uuid.NewString()
	ref := "ref-" + uuid.NewString() // Reference should also be same if we want exact same input?
	// Note: If reference is different, but IK is same, Ledger might reject with ErrIdempotencyKeyConflict (Input mismatch).
	// To test "idempotency replay", we MUST send EXACT same payload.
	// So same Reference too.

	var successCount uint64
	var failCount uint64
	var txIDs sync.Map

	var wg sync.WaitGroup
	start := time.Now()

	fmt.Printf("Launching %d workers with IK: %s\n", concurrency, ik)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			txID, status := debitWallet(walletID, debitAmount, ref, ik)
			if status == http.StatusCreated || status == http.StatusOK {
				atomic.AddUint64(&successCount, 1)
				if txID != "" {
					txIDs.Store(txID, true)
				}
			} else {
				atomic.AddUint64(&failCount, 1)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Test 1 Completed in %v\n", elapsed)
	fmt.Printf("Success (200/201): %d\n", successCount)
	fmt.Printf("Fail: %d\n", failCount)

	// Check unique TX IDs
	count := 0
	txIDs.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	fmt.Printf("Unique Transaction IDs returned: %d\n", count)

	if count == 1 {
		fmt.Println("SUCCESS: Only 1 unique transaction created.")
	} else {
		fmt.Printf("FAILURE: %d transactions created (Expected 1)\n", count)
	}

	// Verify Balance
	// Should have debited ONLY ONCE (50). Remaining: 450.
	balance := getBalance(userID, currency)
	fmt.Printf("Final Balance: %d (Expected: %d)\n", balance, initBalance-debitAmount)

	if balance == initBalance-debitAmount {
		fmt.Println("SUCCESS: Balance is correct.")
	} else {
		fmt.Println("FAILURE: Balance mismatch.")
	}
}

func testDifferentIdempotencyKey() {
	fmt.Println("\n--- Test 2: Concurrent Debits with DIFFERENT Idempotency Keys ---")

	// 1. Create Wallet
	userID := "user-idem-diff-" + uuid.NewString()[:8]
	walletID := createWallet(userID, currency)
	if walletID == "" {
		panic("Failed to create wallet")
	}
	fmt.Printf("Created Wallet: %s\n", walletID)

	// 2. Fund Wallet with 500
	if !creditWallet(walletID, initBalance, "init-"+uuid.NewString(), "") {
		panic("Failed to fund wallet")
	}
	fmt.Printf("Funded Wallet with %d %s\n", initBalance, currency)

	// 3. Concurrent Debits with DIFFERENT Keys
	// We launch 11 workers. 10 should succeed (500/50). 1 should fail.
	workers := 11

	var successCount uint64
	var failCount uint64
	var insufficientFundsCount uint64

	var wg sync.WaitGroup
	start := time.Now()

	fmt.Printf("Launching %d workers with unique IKs...\n", workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ik := "ik-" + uuid.NewString()
			ref := "ref-" + uuid.NewString()

			_, status := debitWallet(walletID, debitAmount, ref, ik)
			if status == http.StatusCreated || status == http.StatusOK {
				atomic.AddUint64(&successCount, 1)
			} else {
				atomic.AddUint64(&failCount, 1)
				// Check if it failed due to insufficient funds (400)
				// We can't check body here easily without changing helper signature,
				// but verify status is 400.
				if status == http.StatusBadRequest {
					atomic.AddUint64(&insufficientFundsCount, 1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Test 2 Completed in %v\n", elapsed)
	fmt.Printf("Success: %d (Expected 10)\n", successCount)
	fmt.Printf("Fail: %d (Expected 1)\n", failCount)
	fmt.Printf("Insufficient Funds Errors: %d\n", insufficientFundsCount)

	if successCount == 10 && failCount == 1 {
		fmt.Println("SUCCESS: Exact number of transactions succeeded.")
	} else {
		fmt.Println("FAILURE: Counts mismatch.")
	}

	// Verify Balance should be 0
	balance := getBalance(userID, currency)
	fmt.Printf("Final Balance: %d (Expected: 0)\n", balance)

	if balance == 0 {
		fmt.Println("SUCCESS: Balance is 0.")
	} else {
		fmt.Println("FAILURE: Balance is not 0.")
	}
}

// Helpers

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

func creditWallet(walletID string, amount int64, ref string, ik string) bool {
	url := fmt.Sprintf("%s/%s/wallets/%s/credit", baseURL, ledgerName, walletID)
	reqBody := WalletTransactionRequest{Amount: amount, Reference: ref}
	body, _ := json.Marshal(reqBody)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	if ik != "" {
		req.Header.Set("Idempotency-Key", ik)
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Credit Error: %v\n", err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusCreated
}

func debitWallet(walletID string, amount int64, ref string, ik string) (string, int) {
	url := fmt.Sprintf("%s/%s/wallets/%s/debit", baseURL, ledgerName, walletID)
	reqBody := WalletTransactionRequest{Amount: amount, Reference: ref}
	body, _ := json.Marshal(reqBody)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	if ik != "" {
		req.Header.Set("Idempotency-Key", ik)
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Debit Transport Error: %v\n", err)
		return "", 0
	}
	defer resp.Body.Close()

	var txID string
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		var res map[string]interface{}
		json.Unmarshal(respBytes, &res)

		// Debug print
		// fmt.Printf("Success Response: %s\n", string(respBytes))

		if data, ok := res["data"].(map[string]interface{}); ok {
			// Check if 'transaction' field exists and use it
			if tx, ok := data["transaction"].(map[string]interface{}); ok {
				data = tx
			}

			// Check for 'id' or 'txid'
			if id, ok := data["id"].(float64); ok {
				txID = fmt.Sprintf("%.0f", id)
			} else if id, ok := data["txid"].(float64); ok {
				txID = fmt.Sprintf("%.0f", id)
			} else if id, ok := data["id"].(string); ok {
				txID = id
			}
		}
	} else {
		if resp.StatusCode == http.StatusBadRequest {
			// Read body to see if it is insufficient funds
			respBytes, _ := io.ReadAll(resp.Body)
			if strings.Contains(string(respBytes), "INSUFFICIENT_FUND") {
				// OK
			} else {
				fmt.Printf("Debit Failed (Not InsufficientFunds?): %d %s\n", resp.StatusCode, string(respBytes))
			}
		} else {
			respBytes, _ := io.ReadAll(resp.Body)
			fmt.Printf("Debit Failed: %d %s\n", resp.StatusCode, string(respBytes))
		}
	}

	return txID, resp.StatusCode
}

func getBalance(userID, currency string) int64 {
	accountAddr := fmt.Sprintf("users:%s:wallets:%s:available", userID, currency)
	encodedAddr := strings.ReplaceAll(accountAddr, ":", "%3A")
	url := fmt.Sprintf("%s/%s/accounts/%s", baseURL, ledgerName, encodedAddr)

	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("GetBalance Error: %v\n", err)
		return -999999
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
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

	// Debug
	// fmt.Printf("GetBalance Response for %s: %+v\n", encodedAddr, res)

	if val, ok := res.Data.Balances[currency]; ok {
		return val
	}
	// If currency not in map, it might be 0?
	// But if map is empty, something is wrong if we expect funds.
	// fmt.Printf("GetBalance: Currency %s not found in balances: %+v\n", currency, res.Data.Balances)
	return 0
}
