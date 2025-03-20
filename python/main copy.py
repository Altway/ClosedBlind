# %% IMPORT
from web3 import Web3
import requests
import json
import time
from hexbytes import HexBytes
from ethscan_main import POOL_ADDRESS
from concurrent.futures import ThreadPoolExecutor
import threading


def to_dict(obj):
    """Recursively converts AttributeDict and HexBytes to standard Python types."""
    if isinstance(obj, dict):
        return {k: to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_dict(i) for i in obj]
    elif isinstance(obj, HexBytes):
        return obj.hex()  # Convert HexBytes to hex string
    return obj  # Return the value unchanged if it's not a special type


# Example usage:
from web3.datastructures import AttributeDict  # Assuming data comes from Web3

# %% CONFIGURATION
w3 = Web3(
    Web3.HTTPProvider("https://mainnet.infura.io/v3/3921fc62a7ce4cda98926f47409b3d19")
)
CONTRACT_ADDRESS = "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD"
CONTRACT_ADDRESS = contract_address = POOL_ADDRESS = w3.to_checksum_address(
    CONTRACT_ADDRESS
)
ETHERSCAN_API_KEY = "F7K9BTHSSB9EQT9WEGHMG3VFJ54KA8RM1K"
EVENTS_FILE = "../out/events.json"
ABI_FILE = "../out/WETH_WBTC_pool.json"
CHUNK_SIZE = 10000
MAX_RETRIES = 15
INITIAL_RETRY_DELAY = 10  # in seconds

# %% CONNECTION
w3.eth.get_block("latest")


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


# %%
fetch_and_save_abi(CONTRACT_ADDRESS, ABI_FILE)
with open(ABI_FILE, "r") as f:
    contract_abi = json.load(f)  # Load JSON data

contract = w3.eth.contract(address=contract_address, abi=contract_abi)

# A global lock to protect file writes
file_lock = threading.Lock()


def fetch_events_with_retry(event_type, start_block, end_block):
    """Fetch events with retry logic on HTTP 429 errors."""
    attempts = 0
    delay = INITIAL_RETRY_DELAY
    while attempts < MAX_RETRIES:
        try:
            # Use getattr to get the proper event method (Mint or Burn)
            event_filter = getattr(contract.events, event_type).create_filter(
                from_block=start_block, to_block=end_block
            )
            events = event_filter.get_all_entries()
            return events
        except Exception as e:
            if "429" in str(e):
                print(
                    f"HTTP 429 encountered for {event_type} events in blocks {start_block}-{end_block}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)
                delay *= 2  # exponential backoff
                attempts += 1
            else:
                print(
                    f"Error fetching {event_type} events for blocks {start_block}-{end_block}: {e}"
                )
                return []
    print(
        f"Failed to fetch {event_type} events for blocks {start_block}-{end_block} after {MAX_RETRIES} retries."
    )
    return []


def load_processed_event_ids():
    """Load already downloaded events from file into a set of unique identifiers."""
    processed_ids = set()
    try:
        with open(EVENTS_FILE, "r") as f:
            for line in f:
                if line.strip():
                    try:
                        event = json.loads(line)
                        # Construct a unique ID from transaction hash and log index (adjust keys as needed)
                        uid = (
                            event.get("transactionHash", "")
                            + "_"
                            + str(event.get("logIndex", ""))
                        )
                        processed_ids.add(uid)
                    except Exception as e:
                        continue
    except FileNotFoundError:
        pass
    return processed_ids


def process_chunk(start_block, end_block, processed_event_ids, lock):
    """Process a block chunk:
    - Fetch Mint and Burn events (with retries)
    - Filter out already processed events
    - Append new events to file (using a lock to avoid race conditions)
    """
    print(f"Processing blocks {start_block} to {end_block} ...")
    new_events = []
    for event_type in ["Mint", "Burn"]:
        events = fetch_events_with_retry(event_type, start_block, end_block)
        print(f"  {event_type} events found: {len(events)}")
        for event in events:
            event_dict = to_dict(event)
            # Construct a unique event id (assumes each event has these fields)
            uid = (
                event_dict.get("transactionHash", "")
                + "_"
                + str(event_dict.get("logIndex", ""))
            )
            # Only add events that were not already downloaded
            if uid not in processed_event_ids:
                new_events.append(event_dict)
                processed_event_ids.add(uid)
    if new_events:
        # Append new events to the file in a thread-safe manner.
        with lock:
            with open(EVENTS_FILE, "a") as f:
                for event in new_events:
                    f.write(json.dumps(event) + "\n")
    return len(new_events)


def process_events():
    # Determine the block range to process (adjust start_block as needed)
    latest_block = w3.eth.block_number
    start_block = 12369820  # or compute based on latest_block if desired

    # Build list of independent block chunks
    chunks = []
    current_start = start_block
    while current_start <= latest_block:
        current_end = min(current_start + CHUNK_SIZE - 1, latest_block)
        chunks.append((current_start, current_end))
        current_start = current_end + 1

    # Load already downloaded events
    processed_event_ids = load_processed_event_ids()
    print(f"Loaded {len(processed_event_ids)} processed events from file.")

    total_new_events = 0
    # Use a ThreadPoolExecutor to process each chunk concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_chunk, s, e, processed_event_ids, file_lock)
            for s, e in chunks
        ]
        for future in futures:
            try:
                total_new_events += future.result()
            except Exception as e:
                print(f"Error processing a chunk: {e}")

    print(f"Total new events processed: {total_new_events}")


# To run the processing:
process_events()
# %%
import statistics

# Suppose mint_events is your list of Mint events (each is an AttributeDict with an "args" field)
liquidity_by_owner = {}

for event in mint_events:
    # Extract data from the event
    args = event["args"]
    owner = args["owner"]
    liquidity = args["amount"]  # 'amount' is the liquidity minted in this event
    amount0 = args["amount0"]  # token0 contributed
    amount1 = args["amount1"]  # token1 contributed
    tickLower = args["tickLower"]
    tickUpper = args["tickUpper"]

    # Initialize dictionary for the owner if not present
    if owner not in liquidity_by_owner:
        liquidity_by_owner[owner] = {
            "total_liquidity": 0,
            "total_amount0": 0,
            "total_amount1": 0,
            "positions": [],  # we'll store each tick range as a tuple (tickLower, tickUpper)
        }

    liquidity_by_owner[owner]["total_liquidity"] += liquidity
    liquidity_by_owner[owner]["total_amount0"] += amount0
    liquidity_by_owner[owner]["total_amount1"] += amount1
    liquidity_by_owner[owner]["positions"].append((tickLower, tickUpper))

# Now, print out a summary for each owner.
for owner, data in liquidity_by_owner.items():
    total_liquidity = data["total_liquidity"]
    total_amount0 = data["total_amount0"]
    total_amount1 = data["total_amount1"]
    positions = data["positions"]
    # Calculate average tick range (upper - lower) for this owner's positions
    tick_ranges = [upper - lower for (lower, upper) in positions]
    avg_tick_range = statistics.mean(tick_ranges) if tick_ranges else 0
    count_positions = len(positions)

    print(f"Owner: {owner}")
    print(f"  Positions count: {count_positions}")
    print(f"  Total Liquidity: {total_liquidity}")
    print(f"  Total Token0 Amount: {total_amount0}")
    print(f"  Total Token1 Amount: {total_amount1}")
    print(f"  Average Tick Range: {avg_tick_range}")
    print("-" * 50)


# %%
# Load the ABI JSON file
with open("WETH_WBTC_pool.json", "r") as f:
    contract_abi = json.load(f)  # Load JSON data

pool_contract = w3.eth.contract(address=CONTRACT_ADDRESS, abi=contract_abi)

# Extract events and functions
events = [entry for entry in contract_abi if entry.get("type") == "event"]
functions = [entry for entry in contract_abi if entry.get("type") == "function"]

# Print all available events
print("\n🔹 Available Events:")
for event in events:
    print(f"- {event['name']}")

# Print all available functions
print("\n🔹 Available Functions:")
for function in functions:
    print(f"- {function['name']}")
