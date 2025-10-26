#!/usr/bin/env python
import json
import os
import time
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()

INFURA_URL = os.getenv("INFURA_URL_HEARTHQUAKE")
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
FACTORY_DEPLOYMENT_BLOCK = 12369621

FACTORY_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "internalType": "address",
                "name": "token0",
                "type": "address",
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "token1",
                "type": "address",
            },
            {
                "indexed": True,
                "internalType": "uint24",
                "name": "fee",
                "type": "uint24",
            },
            {
                "indexed": False,
                "internalType": "int24",
                "name": "tickSpacing",
                "type": "int24",
            },
            {
                "indexed": False,
                "internalType": "address",
                "name": "pool",
                "type": "address",
            },
        ],
        "name": "PoolCreated",
        "type": "event",
    }
]

w3 = Web3(Web3.HTTPProvider(INFURA_URL))
assert w3.is_connected(), "Failed to connect to Ethereum"
print(f"✓ Connected. Latest block: {w3.eth.block_number:,}")

factory_contract = w3.eth.contract(address=UNISWAP_V3_FACTORY, abi=FACTORY_ABI)

current_block = w3.eth.block_number
chunk_size = 10000
pools = {}

start = FACTORY_DEPLOYMENT_BLOCK
end = current_block

print(f"\nScanning for PoolCreated events from block {start:,} to {end:,}")
print(f"Total blocks to scan: {end-start:,}\n")

for from_block in range(start, end + 1, chunk_size):
    to_block = min(from_block + chunk_size - 1, end)

    retries = 0
    max_retries = 5

    while retries < max_retries:
        try:
            logs = factory_contract.events.PoolCreated.get_logs(
                from_block=from_block, to_block=to_block
            )

            for log in logs:
                pool_address = log.args.pool
                pools[pool_address] = {
                    "token0": log.args.token0,
                    "token1": log.args.token1,
                    "fee": log.args.fee,
                    "created_block": log.blockNumber,
                }

            if len(logs) > 0:
                print(
                    f"[{from_block:,} - {to_block:,}] Found {len(logs)} pools | Total: {len(pools):,}"
                )

            time.sleep(0.2)
            break

        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                retries += 1
                wait_time = 2**retries
                print(
                    f"Rate limited at block {from_block:,}, waiting {wait_time}s... (attempt {retries}/{max_retries})"
                )
                time.sleep(wait_time)
            else:
                print(f"\n❌ Error at block {from_block:,}: {e}")
                break

print(f"\n\n✓ Found {len(pools):,} total Uniswap V3 pools")

os.makedirs("out/V3", exist_ok=True)
output_file = "out/V3/uniswap_v3_pairs_events.json"

with open(output_file, "w") as f:
    json.dump(pools, f, indent=2)

print(f"✓ Saved to {output_file}")

print("\nSample pools:")
for i, (addr, info) in enumerate(list(pools.items())[:5]):
    print(f"  {addr}: fee={info['fee']} block={info['created_block']}")
