import json
import time
import os
from web3 import Web3

# === CONFIGURATION ===
INFURA_URL = "https://mainnet.infura.io/v3/3921fc62a7ce4cda98926f47409b3d19"
CONTRACT_ADDRESS = "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD"
ABI_FILE = "WETH_WBTC_pool.json"  # Contract ABI file
PROCESSED_BLOCKS_FILE = "processed_blocks.json"  # Logs storage
MINT_CALLS_FILE = "mint_calls.json"  # Processed mint calls file
BATCH_SIZE = 1000  # Max number of transactions to process before writing

# === CONNECT WEB3 INSTANCE ===
w3 = Web3(Web3.HTTPProvider(INFURA_URL))


# === LOAD CONTRACT ABI ===
def load_contract_abi(file_path):
    """Load ABI from JSON file."""
    with open(file_path, "r") as f:
        return json.load(f)


contract_abi = load_contract_abi(ABI_FILE)
contract = w3.eth.contract(address=CONTRACT_ADDRESS, abi=contract_abi)

# ✅ Compute `mint()` Function Selector Using Keccak (No Dummy Args)
mint_function_signature = "mint(address,int24,int24,uint128,bytes)"
mint_selector = Web3.keccak(text=mint_function_signature)[:4].hex()

print(f"✅ Extracted Mint() Function Selector: {mint_selector}", flush=True)


# ✅ Load Already Processed Transactions (Checkpoints for Fast Resumption)
def load_processed_tx():
    """Returns a set of already processed transaction hashes to prevent duplicates."""
    if os.path.exists(MINT_CALLS_FILE):
        with open(MINT_CALLS_FILE, "r") as f:
            try:
                data = json.load(f)
                return set(entry["transactionHash"] for entry in data)
            except json.JSONDecodeError:
                print(
                    f"⚠️ Warning: {MINT_CALLS_FILE} is corrupted. Resetting.", flush=True
                )
                return set()
    return set()


processed_transactions = load_processed_tx()


# ✅ Load Transactions from Processed Blocks
def load_transactions(file_path):
    """Load stored transaction logs from `processed_blocks.json`."""
    with open(file_path, "r") as f:
        data = json.load(f)
        return data["logs"]


logs = load_transactions(PROCESSED_BLOCKS_FILE)


# ✅ Retry Mechanism for `429 Too Many Requests`
def get_transaction_with_retry(tx_hash, max_retries=5, base_wait=5):
    """Fetch transaction with retry & exponential backoff for 429 errors."""
    retries = 0
    while retries < max_retries:
        try:
            return w3.eth.get_transaction(tx_hash)  # Make request
        except Exception as e:
            if "429" in str(e):
                wait_time = base_wait * (2**retries)
                print(
                    f"⚠️ 429 Too Many Requests! Retrying in {wait_time}s...", flush=True
                )
                time.sleep(wait_time)
                retries += 1
            else:
                print(f"⚠️ Skipping transaction {tx_hash} due to error: {e}", flush=True)
                return None

    print(f"❌ Skipping {tx_hash} after {max_retries} failed attempts.", flush=True)
    return None


# ✅ Paginated Mint Calls Extraction to Prevent Memory Overload & Reduce API Calls
def extract_mint_calls():
    """Extracts all transactions where `mint()` was called using pagination."""

    mint_calls = []

    for index, log in enumerate(logs)[:1000]:
        tx_hash = log.get("transactionHash")

        # 🌟 Skip transactions already processed (Checkpointing)
        if tx_hash in processed_transactions:
            print(f"⚡ Skipping already processed transaction {tx_hash}", flush=True)
            continue

        tx = get_transaction_with_retry(tx_hash)  # Fetch transaction with retry
        if tx is None:
            continue

        if tx.to and tx.to.lower() == CONTRACT_ADDRESS.lower():
            input_data = tx.input

            if input_data.startswith(mint_selector):  # Check if function is `mint()`
                decoded_inputs = contract.decode_function_input(input_data)
                mint_calls.append(
                    {
                        "blockNumber": log["blockNumber"],
                        "transactionHash": tx_hash,
                        "from": tx["from"],
                        "decodedParams": decoded_inputs[1],
                    }
                )
                processed_transactions.add(tx_hash)

        # 🌟 Write single transaction to file immediately, don't wait for 1000 txs
        save_results(mint_calls)
        mint_calls = []


# ✅ Write Results to Disk & Save Progress **Every Transaction**
def save_results(mint_calls):
    """Append processed `mint()` calls to file incrementally, per transaction."""

    if not mint_calls:
        return

    if os.path.exists(MINT_CALLS_FILE):
        with open(MINT_CALLS_FILE, "r") as f:
            try:
                existing_data = json.load(f)
            except json.JSONDecodeError:
                print(
                    f"⚠️ Warning: {MINT_CALLS_FILE} is corrupted. Resetting.", flush=True
                )
                existing_data = []
    else:
        existing_data = []

    existing_data.extend(mint_calls)

    with open(MINT_CALLS_FILE, "w") as f:
        json.dump(existing_data, f, indent=2)

    print(
        f"✅ Saved {len(mint_calls)} transaction(s) to disk (Total: {len(existing_data)} analyzed).",
        flush=True,
    )


# ✅ Extract & Save Mint Calls Incrementally
extract_mint_calls()

print(f"✅ Extraction completed successfully!", flush=True)
