# %%
# optimized_liquidity_sniffer.py
# Adapted for a local archive node, fully parallelized, no rate limits.

import os
import json
import math
from web3 import Web3
from concurrent.futures import ThreadPoolExecutor, as_completed

# %% Configuration
NODE_URL = os.getenv("NODE_URL", "http://127.0.0.1:8545")
FACTORY_ADDRESS = Web3.to_checksum_address("0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95")
GENESIS_BLOCK = 6627956
CHUNK_SIZE = 100_000  # blocks per log-query chunk
MAX_WORKERS = (os.cpu_count() or 1) * 5  # I/O bound tasks
OUTPUT_DIR = "./out"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# %% ABIs (minimal)
UNISWAP_FACTORY_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "token", "type": "address"},
            {"indexed": True, "name": "exchange", "type": "address"},
        ],
        "name": "NewExchange",
        "type": "event",
    }
]

ERC20_TRANSFER_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    }
]

# %% Initialize Web3
w3 = Web3(Web3.HTTPProvider(NODE_URL))
print(w3.eth.get_block("latest"))
assert w3.is_connected(), f"Unable to connect to node at {NODE_URL}"
factory = w3.eth.contract(address=FACTORY_ADDRESS, abi=UNISWAP_FACTORY_ABI)


def chunk_ranges(start: int, end: int, size: int):
    """Yield (from_block, to_block) tuples."""
    cursor = start
    while cursor <= end:
        yield (cursor, min(cursor + size - 1, end))
        cursor += size


# %% Fetch logs in parallel
def fetch_logs_parallel(filter_params: dict, chunks: list, max_workers: int):
    """Fetch logs for each chunk in parallel and return consolidated list."""
    logs = []
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = {
            exe.submit(
                w3.eth.get_logs, {**filter_params, "fromBlock": fr, "toBlock": tr}
            ): (fr, tr)
            for fr, tr in chunks
        }
        for f in as_completed(futures):
            fr, tr = futures[f]
            try:
                batch = f.result()
                print(f"Fetched {len(batch)} logs from blocks {fr}-{tr}")
                logs.extend(batch)
            except Exception as e:
                print(f"Error fetching blocks {fr}-{tr}: {e}")
    return logs


# %% List all Uniswap V1 pools
def list_pools(genesis: int, latest: int) -> list:
    """
    Return list of dicts: {exchange, token, blockNumber}
    """
    chunks = list(chunk_ranges(genesis, latest, CHUNK_SIZE))
    topic0 = factory.events.NewExchange().abi["signature"]
    filter_params = {"address": FACTORY_ADDRESS, "topics": [topic0]}
    raw_logs = fetch_logs_parallel(filter_params, chunks, MAX_WORKERS)
    pools = []
    for lg in raw_logs:
        evt = factory.events.NewExchange().processLog(lg)
        pools.append(
            {
                "exchange": evt["args"]["exchange"],
                "token": evt["args"]["token"],
                "block": lg["blockNumber"],
            }
        )
    return pools


# %% Reconstruct LP token balances
def reconstruct_balances(exchange: str, start_block: int, latest: int) -> dict:
    """
    Return dict of {holder: balance} for the given exchange (LP token contract)
    """
    erc20 = w3.eth.contract(address=exchange, abi=ERC20_TRANSFER_ABI)
    chunks = list(chunk_ranges(start_block, latest, CHUNK_SIZE))
    topic0 = erc20.events.Transfer().abi["signature"]
    filter_params = {"address": exchange, "topics": [topic0]}
    logs = fetch_logs_parallel(filter_params, chunks, MAX_WORKERS)
    balances = {}
    for lg in logs:
        tx = erc20.events.Transfer().processLog(lg)
        frm = tx["args"]["from"]
        to = tx["args"]["to"]
        val = tx["args"]["value"]
        if frm != "0x0000000000000000000000000000000000000000":
            balances[frm] = balances.get(frm, 0) - val
        if to != "0x0000000000000000000000000000000000000000":
            balances[to] = balances.get(to, 0) + val
    # Keep only positive balances
    return {addr: bal for addr, bal in balances.items() if bal > 0}


# %% Main execution cell
if __name__ == "__main__":
    latest_block = w3.eth.block_number

    # Step 1: List all pools
    print("Fetching Uniswap V1 pools...")
    pools = list_pools(GENESIS_BLOCK, latest_block)
    with open(os.path.join(OUTPUT_DIR, "uniswap_v1_pools.json"), "w") as f:
        json.dump(pools, f, indent=2)
    print(f"Found {len(pools)} pools. Saved to uniswap_v1_pools.json")

    # Step 2: Reconstruct LP holders for each pool
    print("Reconstructing LP holders for each pool...")
    cartography = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                reconstruct_balances, p["exchange"], p["block"], latest_block
            ): p
            for p in pools
        }
        for future in as_completed(futures):
            meta = futures[future]
            exch = meta["exchange"]
            token = meta["token"]
            try:
                holders = future.result()
                cartography[exch] = {
                    "token": token,
                    "creation_block": meta["block"],
                    "holders": holders,
                }
                print(f"{exch}: {len(holders)} holders")
            except Exception as e:
                print(f"Error processing {exch}: {e}")

    # Save full cartography
    out_path = os.path.join(OUTPUT_DIR, "lp_cartography.json")
    with open(out_path, "w") as f:
        json.dump(cartography, f, indent=2)
    print(f"LP cartography saved to {out_path}")
