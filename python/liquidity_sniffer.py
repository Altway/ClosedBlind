# %% IMPORT
import json
import os
import random
import requests
import time
import threading

from concurrent.futures import ThreadPoolExecutor
from hexbytes import HexBytes
from web3 import Web3, AsyncWeb3

# Determine number of CPU cores
cpu_count = os.cpu_count() or 1

# Set mode based on your task type
task_type = "io"  # Change to "cpu" for CPU-bound tasks

if task_type.lower() == "cpu":
    max_workers = cpu_count
else:
    # For I/O-bound tasks, you might use a higher number, for example 5 * cpu_count
    max_workers = cpu_count * 5

print(
    f"Using {max_workers} threads in the ThreadPoolExecutor for {task_type.upper()}-bound tasks."
)


def to_dict(obj):
    """Recursively converts AttributeDict and HexBytes to standard Python types."""
    if isinstance(obj, dict):
        return {k: to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_dict(i) for i in obj]
    elif isinstance(obj, HexBytes):
        return obj.hex()  # Convert HexBytes to hex string
    return obj  # Return the value unchanged if it's not a special type


w3 = Web3(
    # Web3.HTTPProvider("https://mainnet.infura.io/v3/3921fc62a7ce4cda98926f47409b3d19")
    Web3.HTTPProvider("http://127.0.0.1:8545")
)

# w3 = await AsyncWeb3(AsyncWeb3.WebSocketProvider("ws://127.0.0.1:8545"))
assert w3.is_connected()
CONTRACT_ADDRESS = "0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95"
CONTRACT_ADDRESS = contract_address = w3.to_checksum_address(CONTRACT_ADDRESS)
ETHERSCAN_API_KEY = "F7K9BTHSSB9EQT9WEGHMG3VFJ54KA8RM1K"
EVENTS_FILE = "../out/deployer_v1_events.json"
ABI_FILE = "../out/Uniswap_deployer_v1.json"
LP_CARTOGRAPHY_FILE = "../out/lp_cartography.json"
GENESIS_CONTRACT_BLOCK = 6627956
CHUNK_SIZE = 10000
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 3  # in seconds
print(w3.eth.get_block("latest"))


def to_dict(obj):
    """Recursively converts AttributeDict and HexBytes to standard Python types."""
    if hasattr(obj, "items"):
        return {k: to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_dict(i) for i in obj]
    elif hasattr(obj, "hex"):
        return obj.hex()  # Convert HexBytes to string
    return obj


def fetch_and_save_abi(contract_address, output_path):
    """Fetch the latest ABI of a contract from Etherscan and save it to a file."""
    response = requests.get(
        f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract_address}&apikey={ETHERSCAN_API_KEY}"
    )
    data = response.json()

    if data["status"] == "1":
        abi = json.loads(data["result"])
        with open(output_path, "w") as f:
            json.dump(abi, f, indent=4)
        print(f"ABI successfully saved to {output_path}")
        return abi
    else:
        raise Exception(f"Failed to fetch ABI: {data['message']}")


fetch_and_save_abi(CONTRACT_ADDRESS, ABI_FILE)
with open(ABI_FILE, "r") as f:
    contract_abi = json.load(f)  # Load JSON data


contract = w3.eth.contract(address=contract_address, abi=contract_abi)


# %%
# -----------------------
# Extract Factory Contract Address from deployer_v1_events.json
# -----------------------
def extract_factory_address(events_file):
    try:
        with open(events_file, "r") as f:
            for line in f:
                if line.strip():
                    event = json.loads(line)
                    # Assume the event structure includes "address" at top-level.
                    factory_address = event.get("address")
                    if factory_address:
                        return factory_address
    except Exception as e:
        print(f"Error reading {events_file}: {e}")
    raise Exception("No factory address found in events file.")


# Extract the factory contract address from the events file.
factory_address = extract_factory_address(EVENTS_FILE)

# Fetch the factory ABI from Etherscan
factory_abi = fetch_and_save_abi(factory_address, f"../out/{factory_address}.json")
with open(f"../out/{factory_address}.json", "r") as f:
    factory_abi = json.load(f)  # Load JSON data
print("Factory ABI fetched from Etherscan.")

# Create factory contract instance
factory_contract = w3.eth.contract(address=factory_address, abi=factory_abi)

# -----------------------
# ERC20 Transfer ABI (to reconstruct LP balances)
# -----------------------
erc20_transfer_abi = json.loads(
    """
[
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true, "name": "from", "type": "address"},
      {"indexed": true, "name": "to", "type": "address"},
      {"indexed": false, "name": "value", "type": "uint256"}
    ],
    "name": "Transfer",
    "type": "event"
  }
]
"""
)


# -----------------------
# Helper Functions for Events and Pagination
# -----------------------
def event_to_dict(event):
    d = dict(event)
    if "args" in d:
        d["args"] = dict(d["args"])
    if "transactionHash" in d:
        d["transactionHash"] = d["transactionHash"].hex()
    if "blockHash" in d:
        d["blockHash"] = d["blockHash"].hex()
    return d


def fetch_events_with_retry(contract_obj, event_name, from_block, to_block):
    attempts = 0
    delay = INITIAL_RETRY_DELAY
    while attempts < MAX_RETRIES:
        try:
            # Access the event object without calling it (no parentheses)
            event_obj = getattr(contract_obj.events, event_name)
            # Create the filter using the new create_filter syntax with snake_case parameters
            event_filter = event_obj.create_filter(
                from_block=from_block, to_block=to_block
            )
            events = event_filter.get_all_entries()
            return events
        except Exception as e:
            if "429" in str(e):
                jitter = random.uniform(3, delay)
                total_delay = delay + jitter
                print(
                    f"HTTP 429 for {event_name} in blocks {from_block}-{to_block}. Retrying in {total_delay:.2f} sec..."
                )
                time.sleep(total_delay)
                delay *= 2  # exponential backoff
                attempts += 1
            else:
                print(
                    f"Error fetching {event_name} events for blocks {from_block}-{to_block}: {e}"
                )
                return []
    print(
        f"Failed to fetch {event_name} events for blocks {from_block}-{to_block} after {MAX_RETRIES} retries."
    )
    return []


def paginate_logs(
    contract_obj, event_name, start_block, end_block, chunk_size=CHUNK_SIZE
):
    events = []
    current_start = start_block
    while current_start <= end_block:
        current_end = min(current_start + chunk_size - 1, end_block)
        new_events = fetch_events_with_retry(
            contract_obj, event_name, current_start, current_end
        )
        print(
            f"  {event_name}: blocks {current_start}-{current_end} -> {len(new_events)} events"
        )
        events.extend(new_events)
        current_start = current_end + 1
    return events


# -----------------------
# Load NewExchange Events from deployer_v1_events.json
# -----------------------
def load_newexchange_events(events_file):
    events = []
    try:
        with open(events_file, "r") as f:
            for line in f:
                if line.strip():
                    try:
                        event = json.loads(line)
                        events.append(event)
                    except Exception as e:
                        print(f"Error reading an event: {e}")
    except FileNotFoundError:
        print(f"File {events_file} not found.")
    return events


# -----------------------
# Reconstruct LP Balances for a given Exchange Contract
# -----------------------
def reconstruct_lp_balances(
    exchange_address, start_block=GENESIS_CONTRACT_BLOCK, end_block="latest"
):
    erc20_contract = w3.eth.contract(address=exchange_address, abi=erc20_transfer_abi)
    if end_block == "latest":
        end_block = w3.eth.block_number
    print(
        f"Reconstructing LP balances for exchange {exchange_address} from blocks {start_block} to {end_block}..."
    )
    transfer_events = paginate_logs(erc20_contract, "Transfer", start_block, end_block)
    balances = {}
    for event in transfer_events:
        data = event["args"]
        from_addr = data.get("from")
        to_addr = data.get("to")
        value = int(data.get("value"))
        if from_addr != "0x0000000000000000000000000000000000000000":
            balances[from_addr] = balances.get(from_addr, 0) - value
        if to_addr != "0x0000000000000000000000000000000000000000":
            balances[to_addr] = balances.get(to_addr, 0) + value
    final_balances = {addr: bal for addr, bal in balances.items() if bal > 0}
    return final_balances


# -----------------------
# Process Each Exchange and Build Liquidity Cartography
# -----------------------
def process_all_exchanges():
    newexchange_events = load_newexchange_events(EVENTS_FILE)
    print(f"Loaded {len(newexchange_events)} NewExchange events.")
    results = {}
    for event in newexchange_events:
        args = event.get("args", {})
        token_addr = args.get("token")
        exchange_addr = args.get("exchange")
        if not exchange_addr:
            continue
        print(f"\nProcessing exchange {exchange_addr} for token {token_addr}...")
        lp_balances = reconstruct_lp_balances(
            exchange_addr, start_block=GENESIS_CONTRACT_BLOCK, end_block="latest"
        )
        results[exchange_addr] = {"token": token_addr, "lp_balances": lp_balances}
    # Save the cartography to a JSON file
    with open(LP_CARTOGRAPHY_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"LP cartography saved to {LP_CARTOGRAPHY_FILE}")


# -----------------------
# Run the Processing
# -----------------------
# %%
process_all_exchanges()
