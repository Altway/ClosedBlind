# %% IMPORT
import json
import os
import random
import requests
import time
import threading

from concurrent.futures import ThreadPoolExecutor
from hexbytes import HexBytes
from web3 import Web3

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
    Web3.HTTPProvider("https://mainnet.infura.io/v3/3921fc62a7ce4cda98926f47409b3d19")
)
CONTRACT_ADDRESS = "0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95"
CONTRACT_ADDRESS = contract_address = w3.to_checksum_address(CONTRACT_ADDRESS)
ETHERSCAN_API_KEY = "F7K9BTHSSB9EQT9WEGHMG3VFJ54KA8RM1K"
EVENTS_FILE = "../out/deployer_v1_events.json"
ABI_FILE = "../out/Uniswap_deployer_v1.json"
GENESIS_CONTRACT_BLOCK = 6627956
CHUNK_SIZE = 10000
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 3  # in seconds
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


fetch_and_save_abi(CONTRACT_ADDRESS, ABI_FILE)
with open(ABI_FILE, "r") as f:
    contract_abi = json.load(f)  # Load JSON data

contract = w3.eth.contract(address=contract_address, abi=contract_abi)

# -----------------------------
# Querying All Logs for the Contract
# -----------------------------
# Define filter parameters for the entire blockchain.
filter_params = {"address": contract_address, "fromBlock": 0, "toBlock": "latest"}


def find_contract_creation_block(contract_address, start=0, end=None):
    """
    Returns the block number at which the contract code first appears (i.e. its creation block)
    using a binary search. 'start' is the lower bound block number (usually 0), and 'end'
    defaults to the latest block.
    """
    if end is None:
        end = w3.eth.block_number
    low, high = start, end
    creation_block = None
    while low <= high:
        mid = (low + high) // 2
        code = w3.eth.get_code(contract_address, block_identifier=mid)
        if code != b"":
            creation_block = mid
            high = mid - 1  # search earlier blocks
        else:
            low = mid + 1  # contract not created yet
    return creation_block


GENESIS_CONTRACT_BLOCK = find_contract_creation_block(CONTRACT_ADDRESS) - 1
print(f"Contract was created at block: {GENESIS_CONTRACT_BLOCK}")


# %%
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
                # Add jitter: a random value between 3 and delay
                jitter = random.uniform(3, delay)
                total_delay = delay + jitter
                print(
                    f"HTTP 429 encountered for {event_type} events in blocks {start_block}-{end_block}. Retrying in {total_delay:.2f} seconds..."
                )
                time.sleep(total_delay)
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


def process_chunk(event_type_list, start_block, end_block, processed_event_ids, lock):
    """Process a block chunk:
    - Fetch Mint and Burn events (with retries)
    - Filter out already processed events
    - Append new events to file (using a lock to avoid race conditions)
    """
    print(f"Processing blocks {start_block} to {end_block} ...")
    new_events = []
    for event_type in event_type_list:
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


def process_events(event_type_list):
    # Determine the block range to process (adjust start_block as needed)
    latest_block = w3.eth.block_number
    start_block = (
        GENESIS_CONTRACT_BLOCK - 1
    )  # or compute based on latest_block if desired

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
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                process_chunk, event_type_list, s, e, processed_event_ids, file_lock
            )
            for s, e in chunks
        ]
        for future in futures:
            try:
                total_new_events += future.result()
            except Exception as e:
                print(f"Error processing a chunk: {e}")

    print(f"Total new events processed: {total_new_events}")


# To run the processing:
# process_events(["Mint", "Burn"])
process_events(["NewExchange"])


# %%
# -----------------------
# Metadata Computation from Events File
# -----------------------
def load_metadata_from_events():
    """
    Load metadata directly from EVENTS_FILE.
    Returns a dict with keys as chunk keys (e.g. "0-9999") and values as dicts of event type counts.
    """
    metadata = {}
    try:
        with open(EVENTS_FILE, "r") as f:
            for line in f:
                if line.strip():
                    try:
                        event = json.loads(line)
                        block_number = int(event.get("blockNumber", 0))
                        event_type = event.get("event", "Unknown")
                        chunk_start = (block_number // CHUNK_SIZE) * CHUNK_SIZE
                        chunk_end = chunk_start + CHUNK_SIZE - 1
                        chunk_key = f"{chunk_start}-{chunk_end}"
                        if chunk_key not in metadata:
                            metadata[chunk_key] = {}
                        metadata[chunk_key][event_type] = (
                            metadata[chunk_key].get(event_type, 0) + 1
                        )
                    except Exception as e:
                        print(f"Error processing a line in events file: {e}")
                        continue
    except FileNotFoundError:
        pass
    return metadata


# -----------------------
# Retry with Jitter for Fetching Events
# -----------------------
def fetch_events_with_retry(event_type, start_block, end_block):
    attempts = 0
    delay = INITIAL_RETRY_DELAY
    while attempts < MAX_RETRIES:
        try:
            event_filter = getattr(contract.events, event_type).createFilter(
                fromBlock=start_block, toBlock=end_block
            )
            events = event_filter.get_all_entries()
            return events
        except Exception as e:
            if "429" in str(e):
                jitter = random.uniform(3, delay)
                total_delay = delay + jitter
                print(
                    f"HTTP 429 for {event_type} in blocks {start_block}-{end_block}. Retrying in {total_delay:.2f} sec..."
                )
                time.sleep(total_delay)
                delay *= 2
                attempts += 1
            else:
                print(
                    f"Error fetching {event_type} in blocks {start_block}-{end_block}: {e}"
                )
                return []
    print(
        f"Failed to fetch {event_type} events for blocks {start_block}-{end_block} after {MAX_RETRIES} retries."
    )
    return []


# -----------------------
# Helper: Convert event to dict
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


# -----------------------
# Process Chunk Function
# -----------------------
file_lock = threading.Lock()


def process_chunk(event_type_list, start_block, end_block):
    """
    Process a block chunk:
    - Check if this chunk is already present in the EVENTS_FILE (by computing metadata from events file)
    - If not, fetch events for each event type, append them to the EVENTS_FILE, and return metadata counts.
    """
    chunk_key = (
        f"{start_block}-{min(start_block + CHUNK_SIZE - 1, w3.eth.block_number)}"
    )
    current_metadata = load_metadata_from_events()
    if chunk_key in current_metadata:
        print(f"Chunk {chunk_key} already processed (metadata found). Skipping.")
        return 0, {chunk_key: current_metadata[chunk_key]}

    print(f"Processing chunk {chunk_key} ...")
    new_events = []
    event_counts = {etype: 0 for etype in event_type_list}

    for event_type in event_type_list:
        events = fetch_events_with_retry(event_type, start_block, end_block)
        print(f"  {event_type}: found {len(events)} events.")
        event_counts[event_type] = len(events)
        for event in events:
            event_dict = event_to_dict(event)
            new_events.append(event_dict)

    if new_events:
        with file_lock:
            with open(EVENTS_FILE, "a") as f:
                for event in new_events:
                    f.write(json.dumps(event) + "\n")

    return len(new_events), {chunk_key: event_counts}


# -----------------------
# Integrity Check Function
# -----------------------
def integrity_check(event_type_list, chunks):
    """
    For each chunk, re-query the node for event counts and compare with metadata computed from EVENTS_FILE.
    If discrepancy found, reprocess that chunk.
    """
    print("Starting integrity check...")
    total_fixed = 0
    metadata = load_metadata_from_events()

    for s, e in chunks:
        chunk_key = f"{s}-{e}"
        node_counts = {}
        for etype in event_type_list:
            events = fetch_events_with_retry(etype, s, e)
            node_counts[etype] = len(events)
        stored_counts = metadata.get(chunk_key, {})
        if stored_counts != node_counts:
            print(
                f"Integrity check failed for chunk {chunk_key}. Node: {node_counts} | Stored: {stored_counts}"
            )
            count_fixed, meta_fixed = process_chunk(event_type_list, s, e)
            metadata[chunk_key] = meta_fixed.get(chunk_key, {})
            total_fixed += 1
        else:
            print(f"Chunk {chunk_key} passed integrity check.")
    print(f"Integrity check complete. {total_fixed} chunks reprocessed.")


# -----------------------
# Process All Events Function
# -----------------------
def process_events(event_type_list):
    latest_block = w3.eth.block_number
    print(f"Latest block: {latest_block}")
    chunks = []
    current_start = 0
    while current_start <= latest_block:
        current_end = min(current_start + CHUNK_SIZE - 1, latest_block)
        chunks.append((current_start, current_end))
        current_start = current_end + 1

    total_new_events = 0
    new_metadata = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_chunk, event_type_list, s, e) for s, e in chunks
        ]
        for future in futures:
            try:
                count, meta = future.result()
                total_new_events += count
                new_metadata.update(meta)
            except Exception as e:
                print(f"Error processing a chunk: {e}")

    print(f"Total new events processed: {total_new_events}")

    # Run integrity check on all chunks
    integrity_check(event_type_list, [(s, e) for s, e in chunks])


# -----------------------
# Run the Processing
# -----------------------
process_events(["NewExchange"])
