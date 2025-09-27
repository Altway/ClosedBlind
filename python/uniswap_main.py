import hexbytes
import concurrent.futures
import sys
import threading
import traceback
import json
import time
import os
import random
import logging
import signal
import requests
from pprint import pprint
from web3.logs import STRICT, IGNORE, DISCARD, WARN
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
from decimal import Decimal
from hexbytes import HexBytes
from pathlib import Path
import plotly.graph_objects as go
from requests.exceptions import HTTPError
from datetime import datetime, timezone
from web3 import Web3
from web3.exceptions import Web3RPCError
from collections import defaultdict, OrderedDict
from web3.exceptions import TransactionNotFound, BlockNotFound
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps

# === CONFIGURATION ===
# ETHERSCAN_API_KEY ARE SUS, it's just Infura keys
ETHERSCAN_API_KEY_DICT = {
    "hearthquake": {
        "INFURA_URL": "https://mainnet.infura.io/v3/d28e1ba493794e6aadf097e4964279fb",
        "ETHERSCAN_API_KEY": "d28e1ba493794e6aadf097e4964279fb",
    },
    "opensee": {
        "INFURA_URL": "https://mainnet.infura.io/v3/3921fc62a7ce4cda98926f47409b3d19",
        "ETHERSCAN_API_KEY": "F7K9BTHSSB9EQT9WEGHMG3VFJ54KA8RM1K",
    },
    "eco": {
        "INFURA_URL": "https://mainnet.infura.io/v3/7738948ee4a74652ba0e0bb6c5767857",
        "ETHERSCAN_API_KEY": "7738948ee4a74652ba0e0bb6c5767857",
    },
}

INFURA_URL = ETHERSCAN_API_KEY_DICT["eco"]["INFURA_URL"]
ETHERSCAN_API_KEY = ETHERSCAN_API_KEY_DICT["eco"]["ETHERSCAN_API_KEY"]
CONTRACT_ADDRESS = POOL_ADDRESS = "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD"

# === CONNECT TO ETHEREUM NODE ===
w3 = Web3(
    Web3.HTTPProvider(INFURA_URL)
    # Web3.HTTPProvider("http://127.0.0.1:8545")
)

assert w3.is_connected(), "Web3 provider connection failed"
w3.eth.get_block("latest").number


# --------------------
# Helper Function: Get ABI from Etherscan or Disk
# --------------------
def get_abi(contract_address: str, api_key: str) -> list:
    """
    Retrieves the ABI for a given contract address.
    Checks if the ABI is available in the local 'ABI' folder.
    If not, it fetches the ABI from Etherscan using the provided API key,
    then saves it to disk for future use.

    Parameters:
        contract_address (str): The contract address (checksum not required here).
        api_key (str): Your Etherscan API key.

    Returns:
        list: The ABI loaded as a Python list.
    """
    # Ensure the ABI folder exists.
    abi_folder = "ABI"
    if not os.path.exists(abi_folder):
        os.makedirs(abi_folder)

    # Save ABI with filename based on contract address.
    filename = os.path.join(abi_folder, f"{contract_address}.json")

    # If file exists, load and return the ABI.
    if os.path.exists(filename):
        with open(filename, "r") as file:
            abi = json.load(file)
    else:
        # Construct the Etherscan API URL.
        url = f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract_address}&apikey={api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data["status"] == "1":
                # Parse the ABI and save it for later use.
                abi = json.loads(data["result"])
                with open(filename, "w") as file:
                    json.dump(abi, file)
            else:
                raise Exception(
                    f"Error fetching ABI for contract {contract_address}: {data['result']}"
                )
        else:
            raise Exception(
                "Error connecting to the Etherscan API. Status code: "
                + str(response.status_code)
            )
    return abi


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
# ETHERSCAN VERSION
# Used to find at which block 1 contract has been deployed
# Might be useful later, put it in JSON in the end
# -----------------------
def get_contract_creation_block_etherscan(
    contract_address: str, etherscan_api_key: str
) -> int:
    """
    Retrieves the contract creation block from Etherscan.
    Returns the block number as an integer.
    """
    url = (
        f"https://api.etherscan.io/api?module=contract&action=getcontractcreation"
        f"&contractaddresses={contract_address}&apikey={etherscan_api_key}"
    )
    response = requests.get(url)
    data = response.json()

    if data.get("status") == "1":
        results = data.get("result", [])
        if results and len(results) > 0:
            return int(results[0]["blockNumber"])
        else:
            raise Exception("No contract creation data found.")
    else:
        raise Exception(
            "Error fetching creation block: " + data.get("result", "Unknown error")
        )


# -----------------------
# Used to find at which block 1 contract has been deployed
# Might be useful later, put it in JSON in the end
# -----------------------
def get_contract_creation_block_custom(start_block=0, end_block=100000):

    def get_contract_deployments(start_block, end_block, max_workers=8):
        deployments = []

        def process_block(block_number):
            block = w3.eth.get_block(block_number, full_transactions=True)
            block_deployments = []
            for tx in block.transactions:
                if tx.to is None:
                    try:
                        receipt = w3.eth.get_transaction_receipt(tx.hash)
                        contract_address = receipt.contractAddress
                        if contract_address:
                            block_deployments.append(
                                {
                                    "block_number": block_number,
                                    "contract_address": contract_address,
                                }
                            )
                    except:
                        print(tx.hash)
            return block_deployments

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_block = {
                executor.submit(process_block, bn): bn
                for bn in range(start_block, end_block + 1)
            }
            for future in as_completed(future_to_block):
                block_deployments = future.result()
                deployments.extend(block_deployments)

        return deployments

    deployments = get_contract_deployments(start_block, end_block)

    # Save the results to a JSON file
    with open("contract_deployments.json", "w") as f:
        json.dump(deployments, f, indent=4)


# -- Step 2: Reconstruct an Event’s Signature --
def get_event_signature(event_name: str, abi: list) -> str:
    """
    Given an event name and an ABI, find the event definition and reconstruct its signature.
    For example, for event Transfer(address,address,uint256) this returns its keccak256 hash.
    """
    from eth_utils import keccak, encode_hex

    for item in abi:
        if item.get("type") == "event" and item.get("name") == event_name:
            # Build the signature string: "Transfer(address,address,uint256)"
            types = ",".join([inp["type"] for inp in item.get("inputs", [])])
            signature = f"{event_name}({types})"
            return encode_hex(keccak(text=signature))
    raise ValueError(f"Event {event_name} not found in ABI.")


def block_to_utc(block_number):
    """
    Convert a block number into its UTC timestamp.

    Parameters:
        w3 (Web3): A Web3 instance
        block_number (int): The block number

    Returns:
        datetime: The block timestamp in UTC
    """
    block = w3.eth.get_block(block_number)
    timestamp = block["timestamp"]
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def read_and_sort_jsonl(file_path):
    """
    Reads a JSONL file, each line being a JSON object with a field `blockNumber`,
    and returns a list of those objects sorted by blockNumber (ascending).
    """
    data = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                # Handle bad JSON if needed, e.g., log or skip
                print(line)
                print(f"Skipping bad JSON line: {e}")
                continue
            # Optionally, you could check that 'blockNumber' exists, is int, etc.
            if "blockNumber" not in obj:
                print(f"Skipping line with no blockNumber: {obj}")
                continue
            data.append(obj)
    # Now sort by blockNumber ascending
    # If blockNumber in file is already int, fine; else convert
    sorted_data = sorted(data, key=lambda o: int(o["blockNumber"]))
    return sorted_data


def get_address_abi_contract(contract_address, etherscan_api_key=ETHERSCAN_API_KEY):
    address = w3.to_checksum_address(contract_address)
    contract_abi = get_abi(address, etherscan_api_key)
    contract = w3.eth.contract(address=contract_address, abi=contract_abi)

    return address, contract_abi, contract


# Find the amount of token depending on the contract at the very specific block_number
# but it use ETHERSCAN API (to go further: explorer the reconstruct from all the Transfer event but slow)
# Not super useful for the moment
def get_erc20_balance_at_block(user_address, token_address, block_number):
    """
    Query ERC-20 balance of an address at a specific block.

    user_address = "0xe2dFC8F41DB4169A24e7B44095b9E92E20Ed57eD"
    token_address = "0x514910771AF9Ca656af840dff83E8264EcF986CA"
    block_number = 23405236
    balance = get_erc20_balance_at_block(user_address, token_address, block_number)

    Parameters:
        user_address: string, account to check
        token_address: Web3 contract instance for the ERC-20 token
        block_number: int, historical block

    Returns:
        int: token balance
        None if contract is a proxy
    """
    token_address, token_abi, token_contract = get_address_abi_contract(token_address)
    user_address = w3.to_checksum_address(user_address)
    token_name = None
    token_symbol = None
    try:
        token_name = token_contract.functions.name().call()
        token_symbol = token_contract.functions.symbol().call()
    except Exception as e:
        print(f"Error {e}")
        print(f"{token_address}")
        return None
    balance = token_contract.functions.balanceOf(user_address).call(
        block_identifier=block_number
    )
    print(
        f"Address {user_address} had {w3.from_wei(balance, "ether")} of {token_symbol} at block {block_number}"
    )
    return balance


def get_token_name_by_contract(token_address):
    token_address, token_abi, token_contract = get_address_abi_contract(token_address)
    return token_contract.functions.name().call()


from web3.providers.rpc.utils import (
    ExceptionRetryConfiguration,
    REQUEST_RETRY_ALLOWLIST,
)
from web3.exceptions import Web3Exception
from requests.exceptions import HTTPError

w3 = Web3(
    Web3.HTTPProvider(
        endpoint_uri=INFURA_URL,
        request_kwargs={"timeout": 30},  # adjust as needed
        exception_retry_configuration=ExceptionRetryConfiguration(
            errors=(ConnectionError, HTTPError, TimeoutError),
            retries=5,
            backoff_factor=1,
            method_allowlist=REQUEST_RETRY_ALLOWLIST,
        ),
    )
)


def append_jsonl(OUTPUT_FILE, txs):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for tx in txs:
            f.write(json.dumps(tx, ensure_ascii=False) + "\n")


def load_state(STATE_FILE):
    if not os.path.exists(STATE_FILE):
        return set()
    with open(STATE_FILE, "r") as f:
        data = json.load(f)
    return set({tuple(pair) for pair in data})


def load_state(STATE_FILE):
    if not os.path.exists(STATE_FILE):
        return []

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f, object_pairs_hook=OrderedDict)

    return [tuple(pair) for pair in data]


def save_state(interval, STATE_FILE):
    data = sorted([[l, r] for (l, r) in interval])
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, STATE_FILE)


def decode_topics(log):
    _, abi, contract = get_address_abi_contract(log["address"])
    # Try matching this log against the ABI events
    for item in abi:
        if item.get("type") == "event":
            event_signature = (
                f'{item["name"]}({",".join(i["type"] for i in item["inputs"])})'
            )
            event_hash = w3.keccak(text=event_signature).hex()

            if log["topics"][0].hex() == event_hash:
                # Found matching event
                decoded = contract.events[item["name"]]().process_log(log)
                return {
                    "event": item["name"],
                    "args": dict(decoded["args"]),
                }

    return {}  # no matching event in ABI


OUTPUT_FILE = "real/all_found_tx.jsonl"
STATE_FILE = "real/all_scan_state.json"


START_BLOCK = 0
END_BLOCK = "latest"
CHUNK_SIZE = 10000

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:  # avoid duplicate logs
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)


token_filter = [
    "0x2C4BD064B998838076FA341A83D007FC2FA50957",  # MKR
    "0x255E60C9D597DCAA66006A904ED36424F7B26286",  # BNB
    "0xE8E45431B93215566BA923a7E611B7342EA954DF",
    "0xF173214C720F58E03E194085B1DB28B50ACDEEAD",
    "0xC6581CE3A005E2801C1E0903281BBD318EC5B5C2",
    "0x494D82667C3ED3AC859CCA94B1BE65B0540EE3BB",
    "0x077D52B047735976DFDA76FEF74D4D988AC25196",
    "0xC4A1C45D5546029FD57128483AE65B56124BFA6A",
    "0x7DC095A5CF7D6208CC680FA9866F80A53911041A",
    "0x2135D193BF81ABBEAD93906166F2BE32B2492C04",
    "0x4D2F5CFBA55AE412221182D8475BC85799A5644B",
    "0x87D80DBD37E551F58680B4217B23AF6A752DA83F",
    "0x060A0D4539623B6AA28D9FC39B9D6622AD495F41",
    "0x6B4540F5EE32DDD5616C792F713435E6EE4F24AB",
    "0xB99A23B1A4585FC56D0EC3B76528C27CAD427473",
    "0x04045481B044534ED3CB1E24254B471CFADDEB3D",
    "0xC0E77CDD039A3F731AE0F5C6C9F4A91D4BC28880",
]
token_filter = [Web3.to_checksum_address(k) for k in token_filter]
FULL_EVENT_BY_CONTRACTS = json.load(open(r"real/FULL_EVENT_BY_CONTRACTS.json"))
FULL_EVENT_BY_CONTRACTS = {
    Web3.to_checksum_address(k): v for k, v in FULL_EVENT_BY_CONTRACTS.items()
}

subdict = {
    k: FULL_EVENT_BY_CONTRACTS[k] for k in token_filter if k in FULL_EVENT_BY_CONTRACTS
}
EVENTS = FULL_EVENT_BY_CONTRACTS

END_BLOCK = w3.eth.get_block(END_BLOCK).number
validated_interval = load_state(STATE_FILE)
delay = 3
try:
    logging.info(f"Scanning blocks {START_BLOCK} to {END_BLOCK}")
    l_current_block = START_BLOCK
    r_current_block = min(l_current_block + CHUNK_SIZE, END_BLOCK)
    if validated_interval:
        l_current_block, r_current_block = validated_interval.pop()
    while l_current_block < END_BLOCK:
        txs = []
        if (l_current_block, r_current_block) not in validated_interval:
            logging.info(f"Processing blocks [{l_current_block}, {r_current_block}]")
            try:
                params = {
                    "fromBlock": l_current_block,
                    "toBlock": r_current_block,
                    "address": list(EVENTS.keys()),
                }
                logs = w3.eth.get_logs(params)
                for log in logs:
                    transaction = {
                        "transactionHash": w3.to_hex(log["transactionHash"]),
                        "blockNumber": log["blockNumber"],
                        "address": log["address"],
                        "data": w3.to_hex(log["data"]),
                    }
                    topics = decode_topics(log)
                    transaction.update(topics)
                    txs.append(transaction)
                append_jsonl(OUTPUT_FILE, txs)
                validated_interval.append((l_current_block, r_current_block))
                save_state(validated_interval, STATE_FILE)
                l_current_block = r_current_block + 1
                r_current_block = min(l_current_block + CHUNK_SIZE, END_BLOCK)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    jitter = random.uniform(3, delay)
                    total_delay = (delay + jitter) / 10
                    logging.warning(f"Delay: {total_delay}: {e}")
                    time.sleep(total_delay)
                    delay *= 2  # exponential backoff
                    continue
                elif e.response.status_code == 402:
                    logging.critical(
                        f"Payment Required Code {e.response.status_code}, Stopping Blocks [{l_current_block}, {r_current_block}]: {e}"
                    )
                    break
                else:
                    logging.error(f"HTTP error occurred: {e}")
                    break
            except Web3RPCError as e:
                logging.warning(f"{e}")
                r_current_block = (l_current_block + r_current_block) // 2
            except Exception as e:
                logging.error(
                    f"Exception when processing interval [{l_current_block}, {r_current_block}]: {e}"
                )
                logging.error(print(traceback.format_exc()))
                break
except KeyboardInterrupt:
    validated_interval.pop()
    save_state(validated_interval, STATE_FILE)
    logging.info(f"Interrupted by user — exiting loop.")
except Exception as e:
    logging.fatal(f"Unexpected fatal error in main: {e}")
logging.info(f"Finished parsing the last block")

sorted_transactions = read_and_sort_jsonl("real/all_found_tx.jsonl")
DISTINCT_PROVIDER = set()
D_BLOCK_TOTAL_LIQUIDITY_BY_CONTRACT_BY_BLOCK = defaultdict(
    lambda: defaultdict(lambda: Decimal("0"))
)
L_LIQUIDITY_BOOK = []
L_UNI_LIQUIDITY = []
FAILED_EVENTS = []


def analyze_transaction(transaction):
    block = transaction["blockNumber"]
    event = transaction["event"]
    event_args = transaction["args"]
    address = transaction["address"]
    tx_hash = transaction["transactionHash"]
    if address not in token_filter:
        return
    # Transaction data in case we need to investigate  further (but we could directly store them in jsonl from the sniffer)
    # transaction_information = w3.eth.get_transaction(transaction["txHash"])
    # d_transaction_information = event_to_dict(transaction_information)
    # In case we need ABI and retrieve token information
    # token_address, contract_abi, contract = get_address_abi_contract(
    #    transaction["contract"]
    # )

    # Event part
    if event == "AddLiquidity":
        provider = event_args["provider"]
        token_amount = event_args["token_amount"]
        eth_amount = event_args["eth_amount"]
        # Provider are liquidity participant, we track all of them to count
        DISTINCT_PROVIDER.add(provider)
        L_LIQUIDITY_BOOK.append(
            {
                "block": block,
                "address": address,
                "event": event,
                "provider": provider,
                "token_amount": w3.from_wei(token_amount, "ether"),
                "eth_amount": w3.from_wei(eth_amount, "ether"),
            }
        )
    elif event == "RemoveLiquidity":
        provider = event_args["provider"]
        token_amount = event_args["token_amount"]
        eth_amount = event_args["eth_amount"]
        L_LIQUIDITY_BOOK.append(
            {
                "block": block,
                "address": address,
                "event": event,
                "provider": provider,
                "token_amount": -w3.from_wei(token_amount, "ether"),
                "eth_amount": -w3.from_wei(eth_amount, "ether"),
            }
        )

    elif event == "Transfer":
        _from = event_args["_from"]
        _to = event_args["_to"]
        _value = event_args["_value"]  # UniswapV1-TOKEN-Liquidity-debt
        if _from == w3.to_checksum_address(
            "0x0000000000000000000000000000000000000000"
        ):
            # We Mint Liquidity
            D_BLOCK_TOTAL_LIQUIDITY_BY_CONTRACT_BY_BLOCK[block][address] += w3.from_wei(
                _value, "ether"
            )
            L_UNI_LIQUIDITY.append(
                {
                    "block": block,
                    "address": address,
                    "event": event,
                    "provider": _to,
                    "value": w3.from_wei(_value, "ether"),
                }
            )

        if _to == w3.to_checksum_address(
            w3.to_checksum_address("0x0000000000000000000000000000000000000000")
        ):
            # We Burn Liquidity
            D_BLOCK_TOTAL_LIQUIDITY_BY_CONTRACT_BY_BLOCK[block][address] -= w3.from_wei(
                _value, "ether"
            )
            L_UNI_LIQUIDITY.append(
                {
                    "block": block,
                    "address": address,
                    "event": event,
                    "provider": _from,
                    "value": -w3.from_wei(_value, "ether"),
                }
            )
    # Pursechase will be used to compute SWAP (Volume, Fees) Later
    elif event == "TokenPurchase":
        buyer = event_args["buyer"]
        eth_sold = event_args["eth_sold"]
        tokens_bought = event_args["tokens_bought"]
    elif event == "EthPurchase":
        buyer = event_args["buyer"]
        tokens_sold = event_args["tokens_sold"]
        eth_bought = event_args["eth_bought"]
    # Not that useful for the moment
    elif event == "Approval":
        _owner = event_args["_owner"]
        _spender = event_args["_spender"]
        _value = event_args["_value"]
    else:
        FAILED_EVENTS.append(event)
        # print(f"Event not Known: {event}")


result = []
print(len(sorted_transactions))
for tx in sorted_transactions:
    analyze_transaction(tx)

print(len(DISTINCT_PROVIDER))

print(len(L_LIQUIDITY_BOOK) + len(L_UNI_LIQUIDITY))
df_liquidity_book = pd.DataFrame(L_LIQUIDITY_BOOK)
df_uni_liquidity = pd.DataFrame(L_UNI_LIQUIDITY)
df = pd.concat([df_liquidity_book, df_uni_liquidity], ignore_index=False)

# Sort and Clean the DF
numeric_cols = ["token_amount", "eth_amount", "value"]
df[numeric_cols] = df[numeric_cols].fillna(0)

# 3. Group by address and provider, summing numeric columns
grouped = df.groupby(["address", "provider"], as_index=False).agg(
    {
        "token_amount": "sum",
        "eth_amount": "sum",
        "value": "sum",
    }
)
first_block = df.groupby(["address", "provider"], as_index=False)["block"].first()
result = pd.merge(first_block, grouped, on=["address", "provider"])
result = result.sort_values("block")

# Create a pandas Series of keys (block, address) to map to supply_dict
# One way is to make a MultiIndex or tuple key.
result["_key"] = list(zip(result["block"], result["address"]))


# Define a helper function to map the tuple to dict value (with fallback Decimal(0)).
def _lookup(key):
    blk, addr = key
    return D_BLOCK_TOTAL_LIQUIDITY_BY_CONTRACT_BY_BLOCK.get(blk, {}).get(
        addr, Decimal("0")
    )


# Use Series.map
result["total_liquidity"] = result["_key"].map(_lookup)

result["share_pct"] = (
    result["value"] / result["total_liquidity"].replace({0: np.nan})
).fillna(0)

# 4. provider_amount: essentially a copy of value
result["provider_amount"] = result["value"]

df = result
result


def plot_total_liquidity_over_time_and_save(
    df,
    address_list=None,
    block_min=None,
    block_max=None,
    image_path=None,
    html_path=None,
    image_format="png",
):
    """
    Plot total liquidity over time and save to disk.

    Arguments:
      df: DataFrame with ['block','address','total_liquidity']
      address_list: optional list of addresses to include (or None for all)
      block_min, block_max: optional integer bounds for blocks
      image_path: if not None, path (filename) to save image (png, svg, pdf, etc.)
      html_path: if not None, path to save interactive HTML
      image_format: format for image, e.g. 'png', 'svg', 'pdf'
    """
    if address_list is not None:
        df_plot = df[df["address"].isin(address_list)].copy()
    else:
        df_plot = df.copy()

    df_grp = df_plot.groupby(["address", "block"], as_index=False).agg(
        {"total_liquidity": "first"}
    )

    if block_min is None:
        block_min = df_grp["block"].min()
    if block_max is None:
        block_max = df_grp["block"].max()
    all_blocks = pd.RangeIndex(start=block_min, stop=block_max + 1)

    dfs = []
    for addr, d in df_grp.groupby("address"):
        d2 = d.set_index("block").reindex(all_blocks)
        d2["address"] = addr
        d2["total_liquidity"] = d2["total_liquidity"].ffill().fillna(0)
        d2 = d2.reset_index().rename(columns={"index": "block"})
        dfs.append(d2)

    df_ffill = pd.concat(dfs, ignore_index=True)

    fig = px.line(
        df_ffill,
        x="block",
        y="total_liquidity",
        color="address",
        title="Total Liquidity over Blocks by Address",
    )

    fig.update_xaxes(tickformat="d")
    fig.update_layout(
        xaxis_title="Block",
        yaxis_title="Total Liquidity",
        legend_title="Address",
        template="plotly_white",
    )

    # Save to image if requested
    if image_path is not None:
        # plotly.io.write_image is documented: it accepts format inferred from filename or explicit `format` parameter :contentReference[oaicite:2]{index=2}
        fig.write_image(image_path, format=image_format)

    # Save interactive html if requested
    if html_path is not None:
        fig.write_html(html_path)

    # Optionally also display
    # fig.show()

    return fig


# token_filter
# after you built your `df` with total_liquidity etc.
# fig = plot_total_liquidity_over_time_and_save(
#     df,
#     address_list=[token_filter[0]],
#     image_path="total_liquidity.png",
#     html_path="total_liquidity.html",
# )


def plot_total_liquidity_over_time_and_save_matplotlib(
    df,
    address_list=None,
    block_min=None,
    block_max=None,
    image_path=None,
    image_format="png",
    figsize=(10, 6),
    dpi=300,
):
    """
    Plot total liquidity over time (for each address) using seaborn/matplotlib, and save to disk.

    Arguments:
      df: DataFrame with columns ['block','address','total_liquidity']
      address_list: optional list of addresses to include (or None for all)
      block_min, block_max: optional integer bounds for blocks
      image_path: if not None, path (filename) to save image (png, svg, pdf, etc.)
      image_format: format for image, e.g. 'png', 'svg', 'pdf'
      figsize: tuple for figure size
      dpi: dots per inch for saving
    Returns:
      fig, ax: the matplotlib figure and axes
    """
    # Filter addresses if needed
    if address_list is not None:
        df_plot = df[df["address"].isin(address_list)].copy()
    else:
        df_plot = df.copy()

    # Aggregate (taking first per block per address) — similar to your original
    df_grp = df_plot.groupby(["address", "block"], as_index=False).agg(
        {"total_liquidity": "first"}
    )

    # Determine block_min / block_max
    if block_min is None:
        block_min = df_grp["block"].min()
    if block_max is None:
        block_max = df_grp["block"].max()
    all_blocks = pd.RangeIndex(start=block_min, stop=block_max + 1)

    # Forward fill missing blocks per address
    dfs = []
    for addr, d in df_grp.groupby("address"):
        d2 = d.set_index("block").reindex(all_blocks)
        d2["address"] = addr
        d2["total_liquidity"] = d2["total_liquidity"].ffill().fillna(0)
        d2 = d2.reset_index().rename(columns={"index": "block"})
        dfs.append(d2)

    df_ffill = pd.concat(dfs, ignore_index=True)

    # Plotting
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=figsize)

    # Use seaborn lineplot with hue=address
    sns.lineplot(
        data=df_ffill,
        x="block",
        y="total_liquidity",
        hue="address",
        ax=ax,
        legend="full",
        estimator=None,  # don't aggregate, just plot the raw
    )

    ax.set_title("Total Liquidity over Blocks by Address")
    ax.set_xlabel("Block")
    ax.set_ylabel("Total Liquidity")

    # Optionally format x-axis tick labels as integers
    ax.xaxis.get_major_locator().set_params(integer=True)

    # Tight layout
    fig.tight_layout()

    # Save image if requested
    if image_path is not None:
        # Add extension if not present
        # matplotlib’s savefig infers format from the filename’s extension
        fig.savefig(image_path, format=image_format, dpi=dpi)

    return fig, ax


fig = plot_total_liquidity_over_time_and_save_matplotlib(
    df,
    address_list=[
        token_filter[0],
        token_filter[1],
        token_filter[2],
    ],
    image_path="total_liquidity.png",
)
