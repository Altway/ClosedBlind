# %% IMPORT
from web3 import Web3
import requests
import json
import time
from hexbytes import HexBytes
from ethscan_main import POOL_ADDRESS


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
EVENTS_FILE = "mint_burn_events.json"
# %% CONNECTION
w3.eth.get_block("latest")


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
fetch_and_save_abi(CONTRACT_ADDRESS, "WETH_WBTC_pool.json")

# %%
with open("WETH_WBTC_pool.json", "r") as f:
    contract_abi = json.load(f)  # Load JSON data

contract = w3.eth.contract(address=contract_address, abi=contract_abi)

# Define a block range to search for events (e.g., last 10,000 blocks)
latest_block = w3.eth.block_number
start_block = latest_block - 10000 if latest_block > 10000 else 0
start_block = 12369820
# latest_block = "latest"
chunk_size = 10000
end_block = start_block + chunk_size - 1
print(f"Fetching events from block {start_block} to {latest_block}...")

# Aggregate liquidity by owner:
# Add liquidity from Mint events and subtract liquidity from Burn events.
owner_liquidity = {}


def to_dict(obj):
    """Recursively converts AttributeDict and HexBytes to standard Python types."""
    if isinstance(obj, AttributeDict):
        return {k: to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_dict(i) for i in obj]
    elif isinstance(obj, HexBytes):
        return obj.hex()  # Convert HexBytes to string
    return obj  # Return normal values unchanged


# Function to process events from a list of events (add for Mint, subtract for Burn)
def process_events(events):
    all_events = [to_dict(event) for event in events]
    with open(EVENTS_FILE, "a+") as f:
        json.dump(
            all_events,
            f,
        )


while start_block <= latest_block:
    print(f"Processing blocks {start_block} to {end_block} ...")
    try:
        # Fetch Mint events for this chunk (note: use event object without parentheses)
        mint_filter = contract.events.Mint.create_filter(
            from_block=start_block, to_block=end_block
        )
        mint_events = mint_filter.get_all_entries()
        print(f"  Mint events found: {len(mint_events)}")
        process_events(mint_events)
    except Exception as e:
        print(f"  Error fetching Mint events from {start_block} to {end_block}: {e}")

    try:
        # Fetch Burn events for this chunk
        burn_filter = contract.events.Burn.create_filter(
            from_block=start_block, to_block=end_block
        )
        burn_events = burn_filter.get_all_entries()
        print(f"  Burn events found: {len(burn_events)}")
        process_events(burn_events)
    except Exception as e:
        print(f"  Error fetching Burn events from {start_block} to {end_block}: {e}")

    # Move to next block chunk
    start_block += chunk_size
    end_block = min(start_block + chunk_size - 1, latest_block)
    # Optional sleep to avoid rate limits
    time.sleep(0.2)
"""
# Fetch Mint events (create an instance by calling Mint())
mint_filter = contract.events.Mint.create_filter(
    from_block=start_block, to_block=latest_block
)
mint_events = mint_filter.get_all_entries()

# Fetch Burn events similarly
burn_filter = contract.events.Burn.create_filter(
    from_block=start_block, to_block=latest_block
)
burn_events = burn_filter.get_all_entries()



# Process Mint events
for event in mint_events:
    owner = event["args"]["owner"]
    liquidity = event["args"]["liquidity"]
    owner_liquidity[owner] = owner_liquidity.get(owner, 0) + liquidity

# Process Burn events
for event in burn_events:
    owner = event["args"]["owner"]
    liquidity = event["args"]["liquidity"]
    owner_liquidity[owner] = owner_liquidity.get(owner, 0) - liquidity

# Optionally, filter out owners with zero or negative net liquidity
owner_liquidity = {
    owner: liquidity for owner, liquidity in owner_liquidity.items() if liquidity > 0
}

# --------------------------------------
# Write Aggregated Results to a CSV File
# --------------------------------------

output_file = "lp_net_liquidity.csv"
with open(output_file, mode="w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Owner Address", "Net Liquidity"])
    for owner, liquidity in owner_liquidity.items():
        writer.writerow([owner, liquidity])

print(f"Aggregated liquidity data written to {output_file}")"
"""
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
# %%
# Event signatures
mint_event_signature = w3.keccak(text="Mint(address,uint256)").hex()
burn_event_signature = w3.keccak(text="Burn(address,uint256)")
collect_event_signature = w3.keccak(text="Collect(address,uint256)")


def get_logs(event_signature, offset=1000):
    latest_block = w3.eth.block_number
    from_block = max(0, latest_block - offset)

    if isinstance(event_signature, bytes):  # Check if it's in bytes format
        event_signature = "0x" + event_signature.hex()
    # Fetch logs for the specified event signature (topic)
    logs = w3.eth.get_logs(
        {
            # "fromBlock": from_block,
            "fromBlock": "earliest",
            "toBlock": "latest",
            "address": POOL_ADDRESS,
            "topics": [event_signature],  # Filter by the event signature (topic)
        }
    )

    return logs


# Extract unique addresses
def extract_addresses():
    addresses = set()

    for event_signature in [
        mint_event_signature,
        burn_event_signature,
        collect_event_signature,
    ]:
        logs = get_logs(event_signature)
        for log in logs:
            decoded = pool_contract.events[event_signature.split("(")[0]]().process_log(
                log
            )
            addresses.add(decoded.args["owner"])  # For Mint/Burn
            if "recipient" in decoded.args:
                addresses.add(decoded.args["recipient"])  # For Collect

    return list(addresses)


# Retrieve liquidity for each address
def get_liquidity(address):
    position_key = w3.keccak(text=f"{address}").hex()  # Compute position hash
    position = pool_contract.functions.positions(position_key).call()
    return position[0]  # liquidity amount


def parse_logs(logs):
    for log in logs:
        # Decode log topics (usually indexed parameters)
        # For Mint(address,uint256), the first topic is the address, second is the value
        decoded = {
            "address": Web3.toChecksumAddress(log["topics"][1]),
            "value": int(log["data"], 16),  # Convert the hexadecimal data to an integer
        }
        print(decoded)


# %%
if __name__ == "__main__":
    print(w3.is_connected())
