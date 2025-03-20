import json
from web3 import Web3
from hexbytes import HexBytes

# === CONFIGURATION ===
INFURA_URL = "https://mainnet.infura.io/v3/3921fc62a7ce4cda98926f47409b3d19"
CONTRACT_ADDRESS = "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD"
ABI_FILE = "WETH_WBTC_pool.json"  # Load your contract ABI file
PROCESSED_BLOCKS_FILE = "processed_blocks.json"  # Logs storage

# === CONNECT TO ETHEREUM NODE ===
w3 = Web3(Web3.HTTPProvider(INFURA_URL))


# === LOAD CONTRACT ABI ===
def load_contract_abi(file_path):
    """Load the contract ABI from a JSON file."""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise Exception(f"⚠️ ABI file '{file_path}' not found!")
    except json.JSONDecodeError:
        raise Exception(f"❌ Error parsing ABI JSON from '{file_path}'!")


contract_abi = load_contract_abi(ABI_FILE)
contract = w3.eth.contract(address=CONTRACT_ADDRESS, abi=contract_abi)


# === FUNCTION: LOAD PROCESSED TRANSACTIONS ===
def load_transactions(file_path):
    """Load transactions from processed_blocks.json."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            return data.get("logs", [])
    except FileNotFoundError:
        raise Exception(f"⚠️ File '{file_path}' not found!")
    except json.JSONDecodeError:
        raise Exception(f"❌ Error parsing JSON from '{file_path}'!")


logs = load_transactions(PROCESSED_BLOCKS_FILE)

# === FIND FUNCTION SELECTOR FOR `mint()` ===
mint_function = contract.get_function_by_name("mint")
mint_selector = mint_function.selector.hex()  # First 4 bytes of input data


# === FUNCTION: FIND ALL TRANSACTIONS CALLING `MINT` ===
def extract_mint_calls():
    """Extracts all transactions where `mint()` was called."""
    mint_calls = []

    for log in logs:
        tx_hash = log.get("transactionHash")

        try:
            tx = w3.eth.get_transaction(tx_hash)  # Get full Tx data
        except Exception as e:
            print(f"⚠️ Skipping transaction {tx_hash}: {e}")
            continue  # Skip failed transactions

        if tx.to and tx.to.lower() == CONTRACT_ADDRESS.lower():
            input_data = tx.input  # Raw transaction input data

            if input_data.startswith(mint_selector):  # Check if call is `mint()`
                decoded_inputs = mint_function.decode_input(input_data)  # Decode params
                mint_calls.append(
                    {
                        "blockNumber": log["blockNumber"],
                        "transactionHash": tx_hash,
                        "from": tx["from"],  # Sender of the transaction
                        "decodedParams": decoded_inputs[1],  # Parameters of `mint()`
                    }
                )

    return mint_calls


# === EXTRACT MINT CALLS ===
mint_calls = extract_mint_calls()

# === SAVE RESULTS TO FILE ===
with open("mint_calls.json", "w") as f:
    json.dump(mint_calls, f, indent=2)
    print(f"✅ Found {len(mint_calls)} Mint calls. Saved to `mint_calls.json`.")

# === PRINT SAMPLE OUTPUT ===
print("🔹 Sample Mint Calls:")
for mint in mint_calls[:5]:  # Print first 5 for preview
    print(mint)
