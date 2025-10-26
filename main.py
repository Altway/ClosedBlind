#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import time
import traceback
import datetime
import tempfile
import threading
import duckdb
import requests
from datetime import timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import numpy as np
import pandas as pd

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dotenv import load_dotenv
from hexbytes import HexBytes
from requests.exceptions import HTTPError, ConnectionError

from web3 import Web3
from web3.providers.rpc.utils import (
    ExceptionRetryConfiguration,
    REQUEST_RETRY_ALLOWLIST,
)
from web3.exceptions import Web3RPCError

from python.ethscan_main import POOL_ADDRESS

# Configuration
load_dotenv()
pd.options.display.float_format = "{:20,.4f}".format

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)

ETHERSCAN_API_KEY_DICT = {
    "hearthquake": {
        "INFURA_URL": os.getenv("INFURA_URL_HEARTHQUAKE"),
        "ETHERSCAN_API_KEY": os.getenv("ETHERSCAN_API_KEY"),
    },
    "opensee": {
        "INFURA_URL": os.getenv("INFURA_URL_OPENSEE"),
        "ETHERSCAN_API_KEY": os.getenv("ETHERSCAN_API_KEY"),
    },
    "eco": {
        "INFURA_URL": os.getenv("INFURA_URL_ECO"),
        "ETHERSCAN_API_KEY": os.getenv("ETHERSCAN_API_KEY"),
    },
}

ETHERSCAN_API_KEY = ETHERSCAN_API_KEY_DICT["hearthquake"]["ETHERSCAN_API_KEY"]
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
STATE_FILE = "out/V3/V3_final_scan_state.json"
TOKEN_NAME_FILE = "out/V3/V3_token_name.json"
V3_EVENT_BY_CONTRACTS = "out/V3/uniswap_v3_pairs_events.json"
DB_PATH = "out/V3/uniswap_v3.duckdb"
ABI_CACHE_FOLDER = "ABI"


class ProviderPool:
    def __init__(self, api_key_dict):
        self.providers = []
        self.provider_names = []
        self.index = 0
        self.lock = threading.Lock()

        for name, config in api_key_dict.items():
            provider = Web3(
                Web3.HTTPProvider(
                    endpoint_uri=config["INFURA_URL"],
                    request_kwargs={"timeout": 30},
                    exception_retry_configuration=ExceptionRetryConfiguration(
                        errors=(ConnectionError, HTTPError, TimeoutError),
                        retries=5,
                        backoff_factor=1,
                        method_allowlist=REQUEST_RETRY_ALLOWLIST,
                    ),
                )
            )
            if provider.is_connected():
                self.providers.append(provider)
                self.provider_names.append(name)
                logging.info(f"✓ Provider '{name}' connected")
            else:
                logging.warning(f"✗ Provider '{name}' failed to connect")

        if not self.providers:
            raise Exception("No providers connected!")

    def get_provider(self):
        with self.lock:
            provider = self.providers[self.index]
            name = self.provider_names[self.index]
            self.index = (self.index + 1) % len(self.providers)
            return provider, name


class ABICache:
    def __init__(self):
        self.cache = {}
        self.lock = threading.Lock()

    def get_contract(self, address, provider):
        address = provider.to_checksum_address(address)

        with self.lock:
            if address in self.cache:
                return self.cache[address]

        try:
            abi = get_abi(address)
            contract = provider.eth.contract(address=address, abi=abi)

            with self.lock:
                self.cache[address] = (abi, contract)

            return (abi, contract)

        except ABINotVerified:
            logging.debug(f"Contract {address[:10]} not verified, no ABI available")
            return (None, None)

        except (ABIRateLimited, ABINetworkError, ABIFetchError) as e:
            logging.warning(f"Cannot fetch ABI for {address[:10]}: {e}")
            return (None, None)

    def clear(self):
        with self.lock:
            self.cache.clear()


class DuckDBConnectionPool:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.connections = {}
        self.lock = threading.Lock()

    def get_connection(self):
        thread_id = threading.get_ident()

        with self.lock:
            if thread_id not in self.connections:
                conn = duckdb.connect(self.db_pool)
                self.connections[thread_id] = conn
                logging.debug(f"Created DB connection for thread {thread_id}")

            return self.connections[thread_id]

    def close_all(self):
        with self.lock:
            for thread_id, conn in self.connections.items():
                try:
                    conn.close()
                    logging.debug(f"Closed DB connection for thread {thread_id}")
                except Exception as e:
                    logging.warning(
                        f"Error closing connection for thread {thread_id}: {e}"
                    )

            self.connections.clear()

    def close_current_thread(self):
        thread_id = threading.get_ident()

        with self.lock:
            if thread_id in self.connections:
                try:
                    self.connections[thread_id].close()
                    del self.connections[thread_id]
                    logging.debug(f"Closed DB connection for thread {thread_id}")
                except Exception as e:
                    logging.warning(
                        f"Error closing connection for thread {thread_id}: {e}"
                    )


class TokenCache:
    def __init__(self, cache_file=TOKEN_NAME_FILE):
        self.cache_file = cache_file
        self.cache = {}
        self.lock = threading.Lock()

        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r") as f:
                    self.cache = json.load(f)
                logging.info(f"Loaded {len(self.cache)} tokens from cache")
            except Exception as e:
                logging.warning(f"Failed to load token cache: {e}")

    def get(self, token_address):
        with self.lock:
            return self.cache.get(token_address)

    def set(self, token_address, name, symbol):
        with self.lock:
            self.cache[token_address] = {
                "name": name,
                "symbol": symbol,
                "address": token_address,
            }
            self._save_to_disk()

    def _save_to_disk(self):
        try:
            dirn = os.path.dirname(self.cache_file) or "."
            os.makedirs(dirn, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=dirn, text=True)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.cache_file)
        except Exception as e:
            logging.warning(f"Failed to save token cache: {e}")


TOKEN_CACHE = TokenCache()

DB_POOL = None
ABI_CACHE = ABICache()

PROVIDER_POOL = ProviderPool(ETHERSCAN_API_KEY_DICT)
w3, _ = PROVIDER_POOL.get_provider()
assert w3.is_connected(), "Web3 provider connection failed"
print(f"✓ Connected to Ethereum. Latest block: {w3.eth.block_number:,}")


class ABIFetchError(Exception):
    pass


class ABINotVerified(ABIFetchError):
    pass


class ABIRateLimited(ABIFetchError):
    pass


class ABINetworkError(ABIFetchError):
    pass


def get_abi(contract_address, api_key=ETHERSCAN_API_KEY, abi_folder=ABI_CACHE_FOLDER):
    os.makedirs(abi_folder, exist_ok=True)
    filename = os.path.join(abi_folder, f"{contract_address}.json")

    if os.path.exists(filename):
        try:
            with open(filename, "r") as f:
                abi = json.load(f)
                if abi is None or abi == []:
                    raise ABINotVerified(
                        f"Contract {contract_address} not verified (cached)"
                    )
                return abi
        except json.JSONDecodeError as e:
            logging.warning(
                f"Corrupted ABI cache for {contract_address}: {e}, re-fetching..."
            )

    try:
        url = f"https://api.etherscan.io/v2/api?chainid=1&module=contract&action=getabi&address={contract_address}&apikey={api_key}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data["status"] == "1":
            abi = json.loads(data["result"])

            if not isinstance(abi, list) or len(abi) == 0:
                logging.warning(f"Empty ABI for {contract_address}")
                raise ABINotVerified(f"Empty ABI returned")

            with open(filename, "w") as f:
                json.dump(abi, f, indent=2)
            return abi

        else:
            error_msg = data.get("result", "Unknown error")

            if "not verified" in error_msg.lower():
                with open(filename, "w") as f:
                    json.dump(None, f)
                raise ABINotVerified(f"Contract not verified: {error_msg}")

            elif (
                "rate limit" in error_msg.lower()
                or "max rate limit" in error_msg.lower()
            ):
                raise ABIRateLimited(f"Etherscan rate limit: {error_msg}")

            else:
                logging.error(
                    f"Etherscan API error for {contract_address}: {error_msg}"
                )
                raise ABIFetchError(f"Etherscan error: {error_msg}")

    except requests.Timeout:
        raise ABINetworkError(f"Timeout fetching ABI for {contract_address}")

    except requests.ConnectionError as e:
        raise ABINetworkError(f"Connection error: {e}")

    except requests.RequestException as e:
        raise ABINetworkError(f"Request failed: {e}")

    except (json.JSONDecodeError, KeyError) as e:
        raise ABIFetchError(f"Invalid response format: {e}")


MINIMAL_ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
]

MINIMAL_UNISWAP_V3_POOL_ABI = [
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "fee",
        "outputs": [{"type": "uint24"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "factory",
        "outputs": [{"type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]


def get_contract_with_fallback(
    contract_address, provider=None, contract_type="generic"
):
    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    contract_address = provider.to_checksum_address(contract_address)

    try:
        abi = get_abi(contract_address)
        return provider.eth.contract(address=contract_address, abi=abi)

    except ABINotVerified:
        logging.info(
            f"Contract {contract_address[:10]} not verified, using minimal ABI"
        )

        if contract_type == "erc20":
            return provider.eth.contract(
                address=contract_address, abi=MINIMAL_ERC20_ABI
            )
        elif contract_type == "uniswap_v3_pool":
            return provider.eth.contract(
                address=contract_address, abi=MINIMAL_UNISWAP_V3_POOL_ABI
            )
        else:
            raise ValueError(f"No fallback ABI for type: {contract_type}")

    except ABIRateLimited as e:
        logging.warning(f"Rate limited, cannot fetch ABI: {e}")
        raise

    except (ABINetworkError, ABIFetchError) as e:
        logging.error(f"Cannot get contract {contract_address[:10]}: {e}")
        raise


def validate_abi_has_functions(abi, required_functions):
    if not abi or not isinstance(abi, list):
        return False, "Invalid ABI structure"

    available_functions = [
        item.get("name") for item in abi if item.get("type") == "function"
    ]

    missing = [fn for fn in required_functions if fn not in available_functions]

    if missing:
        return False, f"Missing functions: {missing}"

    return True, available_functions


def decode_topics(log):
    provider, _ = PROVIDER_POOL.get_provider()
    abi, contract = ABI_CACHE.get_contract(log["address"], provider)

    if not abi or not contract:
        return {}

    for item in abi:
        if item.get("type") == "event":
            event_signature = (
                f'{item["name"]}({",".join(i["type"] for i in item["inputs"])})'
            )
            event_hash = w3.keccak(text=event_signature).hex()
            if log["topics"][0].hex() == event_hash:
                try:
                    decoded = contract.events[item["name"]]().process_log(log)
                    return {
                        "event": item["name"],
                        "args": dict(decoded["args"]),
                    }
                except Exception as e:
                    logging.debug(f"Failed to decode event {item['name']}: {e}")
                    return {}

    return {}


_thread_local = threading.local()


def setup_database(db_path, db_pool):
    conn = duckdb.connect(db_pool)
    with open(db_path, "r") as f:
        schema_sql = f.read()
    conn.execute(schema_sql)
    conn.close()

    db_pool = DuckDBConnectionPool(db_pool)

    logging.info("✓ Database schema created successfully")

    return db_pool


def batch_insert_events(events, db_pool, worker_id="main"):
    if not events:
        return 0

    transfers = []
    swaps = []
    mints = []
    burns = []
    collects = []
    flashes = []
    approvals = []

    for e in events:
        event_type = e.get("event")
        args = e.get("args", {})

        if event_type == "Transfer":
            transfers.append(
                (
                    e["transactionHash"],
                    e.get("logIndex", 0),
                    e["blockNumber"],
                    e["address"],
                    args.get("from", ""),
                    args.get("to", ""),
                    int(args.get("value", 0)),
                )
            )

        elif event_type == "Swap":
            swaps.append(
                (
                    e["transactionHash"],
                    e.get("logIndex", 0),
                    e["blockNumber"],
                    e["address"],
                    args.get("sender", ""),
                    args.get("recipient", ""),
                    int(args.get("amount0", 0)),
                    int(args.get("amount1", 0)),
                    int(args.get("sqrtPriceX96", 0)),
                    int(args.get("liquidity", 0)),
                    int(args.get("tick", 0)),
                )
            )

        elif event_type == "Mint":
            mints.append(
                (
                    e["transactionHash"],
                    e.get("logIndex", 0),
                    e["blockNumber"],
                    e["address"],
                    args.get("owner", ""),
                    int(args.get("tickLower", 0)),
                    int(args.get("tickUpper", 0)),
                    args.get("sender", ""),
                    int(args.get("amount", 0)),
                    int(args.get("amount0", 0)),
                    int(args.get("amount1", 0)),
                )
            )

        elif event_type == "Burn":
            burns.append(
                (
                    e["transactionHash"],
                    e.get("logIndex", 0),
                    e["blockNumber"],
                    e["address"],
                    args.get("owner", ""),
                    int(args.get("tickLower", 0)),
                    int(args.get("tickUpper", 0)),
                    int(args.get("amount", 0)),
                    int(args.get("amount0", 0)),
                    int(args.get("amount1", 0)),
                )
            )

        elif event_type == "Collect":
            collects.append(
                (
                    e["transactionHash"],
                    e.get("logIndex", 0),
                    e["blockNumber"],
                    e["address"],
                    args.get("owner", ""),
                    args.get("recipient", ""),
                    int(args.get("tickLower", 0)),
                    int(args.get("tickUpper", 0)),
                    int(args.get("amount0", 0)),
                    int(args.get("amount1", 0)),
                )
            )

        elif event_type == "Flash":
            flashes.append(
                (
                    e["transactionHash"],
                    e.get("logIndex", 0),
                    e["blockNumber"],
                    e["address"],
                    args.get("sender", ""),
                    args.get("recipient", ""),
                    int(args.get("amount0", 0)),
                    int(args.get("amount1", 0)),
                    int(args.get("paid0", 0)),
                    int(args.get("paid1", 0)),
                )
            )

        elif event_type == "Approval":
            approvals.append(
                (
                    e["transactionHash"],
                    e.get("logIndex", 0),
                    e["blockNumber"],
                    e["address"],
                    args.get("owner", ""),
                    args.get("spender", ""),
                    int(args.get("value", 0)),
                )
            )

    conn = db_pool.get_connection()
    conn.execute("BEGIN TRANSACTION")

    try:
        if transfers:
            conn.executemany(
                """
                INSERT INTO transfer 
                (transaction_hash, log_index, block_number, pair_address, from_address, to_address, value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_hash, log_index) DO NOTHING
                """,
                transfers,
            )

        if swaps:
            conn.executemany(
                """
                INSERT INTO swap 
                (transaction_hash, log_index, block_number, pair_address, sender, recipient, 
                 amount0, amount1, sqrt_price_x96, liquidity, tick)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_hash, log_index) DO NOTHING
                """,
                swaps,
            )

        if mints:
            conn.executemany(
                """
                INSERT INTO mint 
                (transaction_hash, log_index, block_number, pair_address, owner, tick_lower, tick_upper,
                 sender, amount, amount0, amount1)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_hash, log_index) DO NOTHING
                """,
                mints,
            )

        if burns:
            conn.executemany(
                """
                INSERT INTO burn 
                (transaction_hash, log_index, block_number, pair_address, owner, tick_lower, tick_upper,
                 amount, amount0, amount1)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_hash, log_index) DO NOTHING
                """,
                burns,
            )

        if collects:
            conn.executemany(
                """
                INSERT INTO collect 
                (transaction_hash, log_index, block_number, pair_address, owner, recipient,
                 tick_lower, tick_upper, amount0, amount1)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_hash, log_index) DO NOTHING
                """,
                collects,
            )

        if flashes:
            conn.executemany(
                """
                INSERT INTO flash 
                (transaction_hash, log_index, block_number, pair_address, sender, recipient,
                 amount0, amount1, paid0, paid1)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_hash, log_index) DO NOTHING
                """,
                flashes,
            )

        if approvals:
            conn.executemany(
                """
                INSERT INTO approval 
                (transaction_hash, log_index, block_number, pair_address, owner, spender, value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_hash, log_index) DO NOTHING
                """,
                approvals,
            )

        conn.execute("COMMIT")

        total = (
            len(transfers)
            + len(swaps)
            + len(mints)
            + len(burns)
            + len(collects)
            + len(flashes)
            + len(approvals)
        )

        logging.info(
            f"[{worker_id}] Inserted {total} events "
            f"(T:{len(transfers)} S:{len(swaps)} M:{len(mints)} B:{len(burns)} "
            f"C:{len(collects)} F:{len(flashes)} A:{len(approvals)})"
        )

        return total

    except Exception as e:
        conn.execute("ROLLBACK")
        logging.error(f"[{worker_id}] batch_insert_events failed: {e}", exc_info=True)
        raise


def mark_range_completed(start_block, end_block, db_pool, worker_id="main"):
    conn = db_pool.get_connection()
    conn.execute(
        """
        INSERT INTO processing_state (start_block, end_block, status, worker_id, updated_at)
        VALUES (?, ?, 'completed', ?, NOW())
        ON CONFLICT (start_block, end_block) 
        DO UPDATE SET 
            status = 'completed', 
            worker_id = ?,
            updated_at = NOW()
    """,
        (start_block, end_block, worker_id, worker_id),
    )


def mark_range_processing(start_block, end_block, db_pool, worker_id="main"):
    conn = db_pool.get_connection()
    conn.execute(
        """
        INSERT INTO processing_state (start_block, end_block, status, worker_id, updated_at)
        VALUES (?, ?, 'processing', ?, NOW())
        ON CONFLICT (start_block, end_block) 
        DO UPDATE SET 
            status = 'processing',
            worker_id = ?,
            updated_at = NOW()
    """,
        (start_block, end_block, worker_id, worker_id),
    )


def get_completed_ranges(db_pool):
    conn = db_pool.get_connection()
    result = conn.execute(
        """
        SELECT start_block, end_block 
        FROM processing_state 
        WHERE status = 'completed'
    """
    ).fetchall()
    return set((r[0], r[1]) for r in result)


def get_database_stats(db_pool):
    conn = db_pool.get_connection()
    result = conn.execute(
        """
        SELECT 
            (SELECT COUNT(*) FROM transfer) as total_transfers,
            (SELECT COUNT(*) FROM swap) as total_swaps,
            (SELECT COUNT(*) FROM mint) as total_mints,
            (SELECT COUNT(*) FROM burn) as total_burns,
            (SELECT COUNT(*) FROM collect) as total_collects,
            (SELECT COUNT(*) FROM flash) as total_flashes,
            (SELECT COUNT(*) FROM approval) as total_approvals,
            (SELECT COUNT(*) FROM processing_state WHERE status = 'completed') as completed_ranges,
            (SELECT COUNT(*) FROM pair_metadata) as total_pairs,
            (SELECT COUNT(*) FROM block_metadata) as total_blocks
        """
    ).fetchone()

    return {
        "total_transfers": result[0],
        "total_swaps": result[1],
        "total_mints": result[2],
        "total_burns": result[3],
        "total_collects": result[4],
        "total_flashes": result[5],
        "total_approvals": result[6],
        "completed_ranges": result[7],
        "total_pairs": result[8],
        "total_blocks": result[9],
    }


def insert_pair_metadata(
    pair_address,
    token0_address,
    token1_address,
    db_pool,
    token0_symbol=None,
    token1_symbol=None,
    token0_decimals=None,
    token1_decimals=None,
    fee_tier=None,
    tick_spacing=None,
    created_block=None,
):
    conn = db_pool.get_connection()
    conn.execute(
        """
        INSERT INTO pair_metadata 
        (pair_address, token0_address, token1_address, token0_symbol, token1_symbol,
         token0_decimals, token1_decimals, fee_tier, tick_spacing, created_block)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (pair_address) DO UPDATE SET
            token0_symbol = COALESCE(EXCLUDED.token0_symbol, pair_metadata.token0_symbol),
            token1_symbol = COALESCE(EXCLUDED.token1_symbol, pair_metadata.token1_symbol),
            token0_decimals = COALESCE(EXCLUDED.token0_decimals, pair_metadata.token0_decimals),
            token1_decimals = COALESCE(EXCLUDED.token1_decimals, pair_metadata.token1_decimals),
            fee_tier = COALESCE(EXCLUDED.fee_tier, pair_metadata.fee_tier),
            tick_spacing = COALESCE(EXCLUDED.tick_spacing, pair_metadata.tick_spacing),
            last_updated = NOW()
        """,
        (
            pair_address,
            token0_address,
            token1_address,
            token0_symbol,
            token1_symbol,
            token0_decimals,
            token1_decimals,
            fee_tier,
            tick_spacing,
            created_block,
        ),
    )


def get_pair_metadata(pair_address, db_pool):
    conn = db_pool.get_connection()
    result = conn.execute(
        """
        SELECT token0_address, token1_address, token0_symbol, token1_symbol,
               token0_decimals, token1_decimals, fee_tier, tick_spacing, created_block
        FROM pair_metadata
        WHERE pair_address = ?
        """,
        (pair_address,),
    ).fetchone()

    if result:
        return {
            "token0_address": result[0],
            "token1_address": result[1],
            "token0_symbol": result[2],
            "token1_symbol": result[3],
            "token0_decimals": result[4],
            "token1_decimals": result[5],
            "fee_tier": result[6],
            "tick_spacing": result[7],
            "created_block": result[8],
        }
    return None


def batch_insert_block_metadata(blocks_data, db_pool):
    if not blocks_data:
        return 0

    conn = db_pool.get_connection()
    conn.executemany(
        """
        INSERT INTO block_metadata (block_number, block_timestamp, block_hash)
        VALUES (?, ?, ?)
        ON CONFLICT (block_number) DO NOTHING
    """,
        blocks_data,
    )
    return len(blocks_data)


def normalize_values_for_pair(pair_address, db_pool):
    metadata = get_pair_metadata(pair_address, db_pool)

    if (
        not metadata
        or metadata["token0_decimals"] is None
        or metadata["token1_decimals"] is None
    ):
        logging.warning(f"Cannot normalize values for {pair_address}: missing decimals")
        return

    lp_decimals = 18
    token0_decimals = metadata["token0_decimals"]
    token1_decimals = metadata["token1_decimals"]

    conn = db_pool.get_connection()

    conn.execute(
        """
        UPDATE transfer
        SET value_normalized = value::DECIMAL / POWER(10, ?)
        WHERE pair_address = ? AND value_normalized IS NULL
        """,
        (lp_decimals, pair_address),
    )

    conn.execute(
        """
        UPDATE mint
        SET amount0_normalized = amount0::DECIMAL / POWER(10, ?),
            amount1_normalized = amount1::DECIMAL / POWER(10, ?)
        WHERE pair_address = ? AND amount0_normalized IS NULL
        """,
        (token0_decimals, token1_decimals, pair_address),
    )

    conn.execute(
        """
        UPDATE burn
        SET amount0_normalized = amount0::DECIMAL / POWER(10, ?),
            amount1_normalized = amount1::DECIMAL / POWER(10, ?)
        WHERE pair_address = ? AND amount0_normalized IS NULL
        """,
        (token0_decimals, token1_decimals, pair_address),
    )

    conn.execute(
        """
        UPDATE swap
        SET amount0_normalized = amount0::DECIMAL / POWER(10, ?),
            amount1_normalized = amount1::DECIMAL / POWER(10, ?)
        WHERE pair_address = ? AND amount0_normalized IS NULL
        """,
        (token0_decimals, token1_decimals, pair_address),
    )

    conn.execute(
        """
        UPDATE collect
        SET amount0_normalized = amount0::DECIMAL / POWER(10, ?),
            amount1_normalized = amount1::DECIMAL / POWER(10, ?)
        WHERE pair_address = ? AND amount0_normalized IS NULL
        """,
        (token0_decimals, token1_decimals, pair_address),
    )

    logging.debug(f"✓ Normalized values for pair {pair_address}")


def validate_uniswap_v3_pool(
    pool_address, provider=None, factory_address=UNISWAP_V3_FACTORY
):
    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    pool_address = provider.to_checksum_address(pool_address)

    # Layer 1: Check if address has code
    code = provider.eth.get_code(pool_address)
    if code == b"" or code == "0x":
        return False, "Address has no contract code"

    # Layer 2: Check if contract has required functions
    required_functions = ["factory", "token0", "token1", "fee"]
    abi = get_abi(pool_address)

    if abi is None:
        return False, "Could not retrieve ABI"

    function_names = [
        item.get("name") for item in abi if item.get("type") == "function"
    ]

    missing_functions = [fn for fn in required_functions if fn not in function_names]
    if missing_functions:
        return False, f"Missing required functions: {missing_functions}"

    # Layer 3: Verify factory deployed this pool
    try:
        pool_contract = provider.eth.contract(address=pool_address, abi=abi)

        reported_factory = pool_contract.functions.factory().call()
        token0 = pool_contract.functions.token0().call()
        token1 = pool_contract.functions.token1().call()
        fee = pool_contract.functions.fee().call()

        # Cross-check with factory
        factory_contract = get_contract_with_fallback(
            factory_address, provider, contract_type="erc20"
        )
        expected_pool = factory_contract.functions.getPool(token0, token1, fee).call()

        if expected_pool.lower() != pool_address.lower():
            return (
                False,
                f"Factory verification failed: expected {expected_pool}, got {pool_address}",
            )

        if reported_factory.lower() != factory_address.lower():
            return False, f"Pool reports wrong factory: {reported_factory}"

        return True, {"token0": token0, "token1": token1, "fee": fee}

    except Exception as e:
        return False, f"Validation call failed: {str(e)}"


def fetch_uniswap_pair_metadata(pair_address, provider=None):
    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    pair_address = provider.to_checksum_address(pair_address)

    try:
        pair_contract = get_contract_with_fallback(
            pair_address, provider, contract_type="uniswap_v3_pool"
        )

        token0_address = pair_contract.functions.token0().call()
        token1_address = pair_contract.functions.token1().call()

        token0_contract = get_contract_with_fallback(
            token0_address, provider, contract_type="erc20"
        )
        token1_contract = get_contract_with_fallback(
            token1_address, provider, contract_type="erc20"
        )

        metadata = {
            "pair_address": pair_address,
            "token0_address": token0_address,
            "token1_address": token1_address,
        }

        try:
            metadata["fee_tier"] = pair_contract.functions.fee().call()
        except:
            metadata["fee_tier"] = None

        try:
            metadata["tick_spacing"] = pair_contract.functions.tickSpacing().call()
        except:
            metadata["tick_spacing"] = None

        try:
            metadata["token0_symbol"] = token0_contract.functions.symbol().call()
        except:
            metadata["token0_symbol"] = None

        try:
            metadata["token0_decimals"] = token0_contract.functions.decimals().call()
        except:
            metadata["token0_decimals"] = None

        try:
            metadata["token1_symbol"] = token1_contract.functions.symbol().call()
        except:
            metadata["token1_symbol"] = None

        try:
            metadata["token1_decimals"] = token1_contract.functions.decimals().call()
        except:
            metadata["token1_decimals"] = None

        return metadata

    except Exception as e:
        logging.debug(f"Failed to fetch metadata for {pair_address[:10]}: {e}")
        return None


def fetch_block_metadata(block_number, provider=None, retry_count=0, max_retries=3):
    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    try:
        block = provider.eth.get_block(block_number)
        return (block_number, block["timestamp"], block["hash"].hex())

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            if retry_count < max_retries:
                wait_time = 2**retry_count
                logging.warning(
                    f"Rate limit (429) for block {block_number}, waiting {wait_time}s..."
                )
                time.sleep(wait_time)
                provider, _ = PROVIDER_POOL.get_provider()
                return fetch_block_metadata(
                    block_number, provider, retry_count + 1, max_retries
                )
            else:
                raise Exception(f"Max retries exceeded for block {block_number}")

        elif e.response.status_code == 402:
            raise Exception(f"Payment required (402) - Infura credits exhausted")

        else:
            logging.error(
                f"HTTP {e.response.status_code} for block {block_number}: {e}"
            )
            raise

    except requests.exceptions.Timeout:
        if retry_count < max_retries:
            logging.warning(f"Timeout for block {block_number}, retrying...")
            provider, _ = PROVIDER_POOL.get_provider()
            return fetch_block_metadata(
                block_number, provider, retry_count + 1, max_retries
            )
        else:
            logging.error(
                f"Timeout after {max_retries} retries for block {block_number}"
            )
            raise

    except requests.exceptions.ConnectionError as e:
        if retry_count < max_retries:
            logging.warning(f"Connection error for block {block_number}, retrying...")
            provider, _ = PROVIDER_POOL.get_provider()
            return fetch_block_metadata(
                block_number, provider, retry_count + 1, max_retries
            )
        else:
            logging.error(
                f"Connection failed after {max_retries} retries for block {block_number}"
            )
            raise

    except Exception as e:
        logging.error(f"Unexpected error fetching block {block_number}: {e}")
        return None


def fetch_and_store_uniswap_pair_metadata(
    pair_address, db_pool, created_block=None, provider=None, max_retries=3
):
    existing = get_pair_metadata(pair_address, db_pool)
    if existing and existing["token0_decimals"] is not None:
        logging.debug(f"Using cached metadata for {pair_address[:10]}")
        return existing

    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    is_valid, validation_result = validate_uniswap_v3_pool(pair_address, provider)
    if not is_valid:
        logging.error(f"Skipping invalid pool {pair_address}: {validation_result}")
        insert_pair_metadata(
            pair_address=pair_address,
            token0_address=None,
            token1_address=None,
            db_pool=db_pool,
            created_block=created_block,
        )
        return None

    # Retry logic
    for attempt in range(max_retries):
        try:
            metadata = fetch_uniswap_pair_metadata(pair_address, provider)

            if metadata:
                insert_pair_metadata(
                    pair_address=metadata["pair_address"],
                    token0_address=metadata["token0_address"],
                    token1_address=metadata["token1_address"],
                    db_pool=db_pool,
                    token0_symbol=metadata.get("token0_symbol"),
                    token1_symbol=metadata.get("token1_symbol"),
                    token0_decimals=metadata.get("token0_decimals"),
                    token1_decimals=metadata.get("token1_decimals"),
                    fee_tier=metadata.get("fee_tier"),
                    tick_spacing=metadata.get("tick_spacing"),
                    created_block=created_block,
                )
                return metadata
            else:
                return None

        except ABIRateLimited:
            if attempt < max_retries - 1:
                wait_time = min(2**attempt, 60)
                logging.warning(
                    f"Rate limited for {pair_address[:10]}, waiting {wait_time}s... (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                provider, _ = PROVIDER_POOL.get_provider()
            else:
                logging.error(f"Max retries exceeded for {pair_address[:10]}")
                return None

        except (ABINetworkError, ABIFetchError) as e:
            if attempt < max_retries - 1:
                wait_time = 2**attempt
                logging.warning(
                    f"Network error for {pair_address[:10]}, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                provider, _ = PROVIDER_POOL.get_provider()
            else:
                logging.error(f"Failed after {max_retries} retries: {e}")
                return None

        except Exception as e:
            logging.error(
                f"Unexpected error fetching metadata for {pair_address[:10]}: {e}"
            )
            return None

    return None


def fetch_and_store_block_metadata(
    block_numbers, db_pool, provider=None, max_workers=8
):
    provider_name = "provided"
    if provider is None:
        provider, provider_name = PROVIDER_POOL.get_provider()

    blocks_data = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_block = {
            executor.submit(fetch_block_metadata, block_num, provider): block_num
            for block_num in block_numbers
        }

        for future in as_completed(future_to_block):
            block_num = future_to_block[future]
            try:
                block_data = future.result()
                if block_data:
                    blocks_data.append(block_data)
            except Exception as e:
                logging.error(f"Failed to fetch block {block_num}: {e}")

    if blocks_data:
        batch_insert_block_metadata(blocks_data, db_pool)
        logging.info(
            f"✓ Stored metadata for {len(blocks_data)} blocks using {provider_name}"
        )

    return len(blocks_data)


def collect_missing_pair_metadata(
    db_pool, batch_size=100, provider=None, max_workers=12
):
    conn = db_pool.get_connection()

    all_pairs = conn.execute(
        """
        SELECT DISTINCT pair_address FROM transfer
        """
    ).fetchall()
    all_pairs = [r[0] for r in all_pairs]

    existing_pairs = conn.execute(
        """
        SELECT pair_address FROM pair_metadata
        WHERE token0_decimals IS NOT NULL AND token1_decimals IS NOT NULL
        """
    ).fetchall()
    existing_pairs = set(r[0] for r in existing_pairs)

    missing_pairs = [p for p in all_pairs if p not in existing_pairs]

    if not missing_pairs:
        logging.info("✓ All pairs already have metadata")
        return

    logging.info(
        f"Found {len(missing_pairs)} pairs missing metadata out of {len(all_pairs)} total"
    )

    successful = 0
    failed = 0
    total_batches = (len(missing_pairs) + batch_size - 1) // batch_size

    for i in range(0, len(missing_pairs), batch_size):
        batch = missing_pairs[i : i + batch_size]
        batch_num = i // batch_size + 1

        logging.info(
            f"Processing batch {batch_num}/{total_batches} ({len(batch)} pairs)"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_pair = {
                executor.submit(
                    fetch_and_store_uniswap_pair_metadata, pair, db_pool, None, provider
                ): pair
                for pair in batch
            }

            for future in as_completed(future_to_pair):
                pair = future_to_pair[future]
                try:
                    metadata = future.result()
                    if metadata:
                        successful += 1
                        if successful % 10 == 0:
                            logging.info(
                                f"Progress: {successful}/{len(missing_pairs)} pairs fetched"
                            )
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    logging.debug(f"Error for {pair[:10]}: {e}")

        logging.info(
            f"Batch {batch_num}/{total_batches} complete: {successful} total successful, {failed} total failed"
        )

    logging.info(
        f"✓ Metadata collection complete: {successful} successful, {failed} failed out of {len(missing_pairs)} pairs"
    )


def normalize_missing_pairs(db_pool, max_workers=8):
    conn = db_pool.get_connection()

    pairs_with_metadata = conn.execute(
        """
        SELECT pair_address, token0_decimals, token1_decimals
        FROM pair_metadata
        WHERE token0_decimals IS NOT NULL AND token1_decimals IS NOT NULL
        """
    ).fetchall()

    if not pairs_with_metadata:
        logging.warning(
            "No pairs with metadata found - run collect_missing_pair_metadata() first"
        )
        return

    pairs_to_normalize = []
    for pair_address, token0_dec, token1_dec in pairs_with_metadata:
        needs_norm = conn.execute(
            """
            SELECT COUNT(*) FROM transfer
            WHERE pair_address = ? AND value_normalized IS NULL
            LIMIT 1
            """,
            (pair_address,),
        ).fetchone()[0]

        if needs_norm > 0:
            pairs_to_normalize.append(pair_address)

    if not pairs_to_normalize:
        logging.info("✓ All pairs already normalized")
        return

    logging.info(f"Normalizing {len(pairs_to_normalize)} pairs...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pair = {
            executor.submit(normalize_values_for_pair, pair, db_pool): pair
            for pair in pairs_to_normalize
        }

        completed = 0
        for future in as_completed(future_to_pair):
            pair = future_to_pair[future]
            try:
                future.result()
                completed += 1
                if completed % 20 == 0 or completed == len(pairs_to_normalize):
                    logging.info(
                        f"Progress: {completed}/{len(pairs_to_normalize)} pairs normalized"
                    )
            except Exception as e:
                logging.warning(f"Failed to normalize {pair[:10]}: {e}")

    logging.info("✓ Normalization complete")


def populate_block_metadata_for_range(
    start_block, end_block, db_pool, batch_size=1000, provider=None, max_workers=8
):
    conn = db_pool.get_connection()

    existing_blocks = conn.execute(
        """
        SELECT block_number FROM block_metadata
        WHERE block_number BETWEEN ? AND ?
    """,
        (start_block, end_block),
    ).fetchall()
    existing_blocks = {b[0] for b in existing_blocks}

    all_blocks = set(range(start_block, end_block + 1))
    missing_blocks = sorted(all_blocks - existing_blocks)

    if not missing_blocks:
        logging.info("✓ All blocks already have metadata")
        return

    logging.info(f"Need to fetch {len(missing_blocks)} block timestamps")

    for i in range(0, len(missing_blocks), batch_size):
        batch = missing_blocks[i : i + batch_size]
        fetch_and_store_block_metadata(batch, db_pool, provider, max_workers)
        logging.info(
            f"Progress: {min(i + batch_size, len(missing_blocks))}/{len(missing_blocks)} blocks processed"
        )


# In[11]:


def get_all_unique_pairs_from_db(db_pool):
    conn = db_pool.get_connection()
    result = conn.execute(
        """
        SELECT DISTINCT pair_address 
        FROM transfer
        WHERE pair_address IS NOT NULL
        """
    ).fetchall()
    return [r[0] for r in result]


def fetch_logs_for_range(
    start_block, end_block, addresses, worker_id="main", retry_count=0, max_retries=5
):
    provider, provider_name = PROVIDER_POOL.get_provider()

    try:
        params = {
            "fromBlock": start_block,
            "toBlock": end_block,
            "address": addresses,
        }

        logs = provider.eth.get_logs(params)

        transactions = []
        for log in logs:
            transaction = {
                "transactionHash": provider.to_hex(log["transactionHash"]),
                "blockNumber": log["blockNumber"],
                "logIndex": log.get("logIndex", 0),
                "address": log["address"],
                "data": provider.to_hex(log["data"]),
            }

            topics = decode_topics(log)
            transaction.update(topics)

            transaction["eventSignature"] = ""
            if log.get("topics") and len(log["topics"]) > 0:
                transaction["eventSignature"] = provider.to_hex(log["topics"][0])

            transactions.append(transaction)

        logging.info(
            f"[{worker_id}] [{provider_name}] Fetched {len(transactions)} events from blocks [{start_block:,}, {end_block:,}]"
        )
        return transactions

    except HTTPError as e:
        if e.response.status_code == 413:
            # Response too large - need to split the range
            logging.warning(
                f"[{worker_id}] [{provider_name}] Response too large (413) for range [{start_block:,}, {end_block:,}] - will split"
            )
            raise Web3RPCError("Response payload too large - splitting range")

        elif e.response.status_code == 429:
            if retry_count < max_retries:
                wait_time = 2**retry_count
                logging.warning(
                    f"[{worker_id}] [{provider_name}] Rate limit hit, waiting {wait_time}s..."
                )
                time.sleep(wait_time)
                return fetch_logs_for_range(
                    start_block,
                    end_block,
                    addresses,
                    worker_id,
                    retry_count + 1,
                    max_retries,
                )
            else:
                logging.error(f"[{worker_id}] Max retries reached")
                raise

        elif e.response.status_code == 402:
            logging.critical(f"[{worker_id}] Payment required (402)")
            raise

        else:
            logging.error(f"[{worker_id}] HTTP error {e.response.status_code}: {e}")
            raise

    except Web3RPCError as e:
        if (
            "more than 10000 results" in str(e)
            or "-32005" in str(e)
            or "Response payload too large" in str(e)
        ):
            raise
        else:
            logging.error(f"[{worker_id}] Web3 RPC error: {e}")
            raise


def collect_block_metadata_for_range(start_block, end_block, worker_id="main"):
    conn = db_pool.get_connection()

    existing_blocks = conn.execute(
        """
        SELECT block_number FROM block_metadata 
        WHERE block_number BETWEEN ? AND ?
        """,
        (start_block, end_block),
    ).fetchall()
    existing_blocks = {b[0] for b in existing_blocks}

    missing_blocks = [
        b for b in range(start_block, end_block + 1) if b not in existing_blocks
    ]
    if not missing_blocks:
        return 0

    provider, provider_name = PROVIDER_POOL.get_provider()

    blocks_data = []
    for block_num in missing_blocks:
        try:
            block = provider.eth.get_block(block_num)
            blocks_data.append((block_num, block["timestamp"], block["hash"].hex()))
        except Exception as e:
            logging.warning(f"[{worker_id}] Failed to fetch block {block_num}: {e}")

    if blocks_data:
        batch_insert_block_metadata(blocks_data, db_pool)
        logging.debug(f"[{worker_id}] Stored metadata for {len(blocks_data)} blocks")

    return len(blocks_data)


def collect_pair_metadata_from_events(events, worker_id="main"):
    unique_pairs = set(e["address"] for e in events if "address" in e)

    for pair_address in unique_pairs:
        existing = get_pair_metadata(pair_address, db_pool)
        if existing is None:
            try:
                metadata = fetch_and_store_uniswap_pair_metadata(pair_address, db_pool)
                if metadata:
                    logging.debug(
                        f"[{worker_id}] Stored metadata for {metadata['token0_symbol']}/{metadata['token1_symbol']}"
                    )
            except Exception as e:
                logging.warning(
                    f"[{worker_id}] Failed to fetch metadata for {pair_address}: {e}"
                )


def process_block_range(
    start_block, end_block, addresses, worker_id="main", collect_metadata=False
):
    if (start_block, end_block) in get_completed_ranges(db_pool):
        logging.debug(
            f"[{worker_id}] Skipping already processed range [{start_block:,}, {end_block:,}]"
        )
        return 0

    mark_range_processing(start_block, end_block, db_pool, worker_id)

    try:
        events = fetch_logs_for_range(start_block, end_block, addresses, worker_id)

        batch_insert_events(events, db_pool, worker_id)

        if collect_metadata and events:
            try:
                collect_block_metadata_for_range(start_block, end_block, worker_id)
                collect_pair_metadata_from_events(events, worker_id)
            except Exception as e:
                logging.warning(
                    f"[{worker_id}] Metadata collection failed (non-fatal): {e}"
                )

        mark_range_completed(start_block, end_block, db_pool, worker_id)

        logging.debug(
            f"[{worker_id}] ✓ Processed [{start_block:,}, {end_block:,}] - {len(events)} events"
        )
        return len(events)

    except (Web3RPCError, HTTPError) as e:
        # Check if we need to split (too many results OR response too large)
        if (
            "more than 10000 results" in str(e)
            or "-32005" in str(e)
            or "Response payload too large" in str(e)
            or (hasattr(e, "response") and e.response.status_code == 413)
        ):

            mid = (start_block + end_block) // 2

            if mid == start_block:
                logging.error(
                    f"[{worker_id}] Cannot split range [{start_block:,}, {end_block:,}] further - skipping"
                )
                mark_range_completed(start_block, end_block, db_pool, worker_id)
                return 0

            logging.info(
                f"[{worker_id}] Splitting [{start_block:,}, {end_block:,}] at {mid:,} (reason: {type(e).__name__})"
            )

            count1 = process_block_range(
                start_block, mid, addresses, worker_id, collect_metadata
            )

            count2 = process_block_range(
                mid + 1, end_block, addresses, worker_id, collect_metadata
            )

            return count1 + count2
        else:
            logging.error(
                f"[{worker_id}] Failed to process [{start_block:,}, {end_block:,}]: {e}"
            )
            return 0

    except Exception as e:
        logging.error(f"[{worker_id}] Unexpected error: {e}")
        logging.error(traceback.format_exc())
        return 0


def generate_block_ranges(start_block, end_block, chunk_size):
    completed = get_completed_ranges(db_pool)

    ranges = []
    current = start_block

    while current <= end_block:
        end = min(current + chunk_size - 1, end_block)

        if (current, end) not in completed:
            ranges.append((current, end))

        current = end + 1

    return ranges


def scan_blockchain(
    addresses,
    start_block,
    end_block,
    chunk_size=10000,
    max_workers=3,
    collect_metadata=False,
):
    ranges = generate_block_ranges(start_block, end_block, chunk_size)

    if not ranges:
        logging.info("No ranges to process - all already completed!")
        return

    total_ranges = len(ranges)
    logging.info(f"Processing {total_ranges} block ranges with {max_workers} workers")

    total_events = 0
    completed_ranges = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_range = {
            executor.submit(
                process_block_range,
                start,
                end,
                addresses,
                f"worker-{i % max_workers}",
                collect_metadata,
            ): (start, end, i)
            for i, (start, end) in enumerate(ranges)
        }

        for future in as_completed(future_to_range):
            start, end, idx = future_to_range[future]

            try:
                event_count = future.result()
                total_events += event_count
                completed_ranges += 1

                progress = (completed_ranges / total_ranges) * 100
                elapsed = time.time() - start_time
                rate = completed_ranges / elapsed if elapsed > 0 else 0
                eta_seconds = (
                    (total_ranges - completed_ranges) / rate if rate > 0 else 0
                )
                eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"

                logging.info(
                    f"Progress: {completed_ranges}/{total_ranges} ({progress:.1f}%) | "
                    f"Events: {total_events:,} | "
                    f"Rate: {rate:.1f} ranges/s | "
                    f"ETA: {eta_str}"
                )

            except Exception as e:
                logging.error(f"Range [{start:,}, {end:,}] failed: {e}")

    elapsed_total = time.time() - start_time
    logging.info(f"\n{'='*60}")
    logging.info(f"Scan completed!")
    logging.info(f"Total events fetched: {total_events:,}")
    logging.info(f"Ranges processed: {completed_ranges}/{total_ranges}")
    logging.info(f"Total time: {int(elapsed_total // 60)}m {int(elapsed_total % 60)}s")
    logging.info(f"{'='*60}\n")


def post_process_normalization():
    logging.info("Starting post-processing normalization...")

    pairs = get_all_unique_pairs_from_db(db_pool)
    logging.info(f"Found {len(pairs)} unique pairs")

    for idx, pair_address in enumerate(pairs):
        logging.info(f"[{idx+1}/{len(pairs)}] Processing {pair_address}")

        metadata = get_pair_metadata(pair_address, db_pool)
        if metadata is None:
            logging.info(f"  Fetching metadata...")
            metadata = fetch_and_store_uniswap_pair_metadata(pair_address, db_pool)

        if metadata and metadata["token0_decimals"] is not None:
            logging.info(
                f"  Normalizing values for {metadata.get('token0_symbol', '?')}/{metadata.get('token1_symbol', '?')}..."
            )
            normalize_values_for_pair(pair_address, db_pool)
        else:
            logging.warning(f"  Cannot normalize - missing decimals")

    logging.info("✓ Post-processing complete")


print("✓ Enhanced scanning functions loaded")


# In[12]:


def scan_blockchain_to_duckdb(
    event_file=V3_EVENT_BY_CONTRACTS,
    db_pool=None,
    start_block=10000001,
    end_block=20000000,
    chunk_size=10000,
    max_workers=3,
    token_filter=None,
):
    logging.info("=" * 60)
    logging.info("BLOCKCHAIN SCANNER")
    logging.info("=" * 60)

    with open(event_file, "r") as f:
        all_pairs = json.load(f)

    all_addresses = [Web3.to_checksum_address(addr) for addr in all_pairs.keys()]

    if token_filter:
        filter_checksummed = [Web3.to_checksum_address(addr) for addr in token_filter]
        addresses = [addr for addr in all_addresses if addr in filter_checksummed]
        logging.info(f"Filtered: {len(addresses)}/{len(all_addresses)} addresses")
    else:
        addresses = all_addresses
        logging.info(f"Total addresses: {len(addresses)}")

    setup_database(db_pool)

    stats = get_database_stats(db_pool)
    logging.info(
        f"Blocks: {start_block:,} → {end_block:,} | Chunk: {chunk_size:,} | Workers: {max_workers}"
    )
    logging.info(
        f"DB: {stats['total_transfers']:,} transfers, {stats['total_swaps']:,} swaps, {stats['completed_ranges']} ranges done"
    )
    logging.info("=" * 60)

    try:
        scan_blockchain(
            addresses, start_block, end_block, chunk_size, max_workers, False
        )

        final_stats = get_database_stats(db_pool)
        logging.info("=" * 60)
        logging.info("SCAN COMPLETE")
        logging.info(
            f"Transfers: {final_stats['total_transfers']:,} | Swaps: {final_stats['total_swaps']:,}"
        )
        logging.info(
            f"Mints: {final_stats['total_mints']:,} | Burns: {final_stats['total_burns']:,}"
        )
        logging.info("=" * 60)

    except KeyboardInterrupt:
        logging.warning("\nInterrupted - progress saved to database")
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)


def scan_all_pairs_in_batches(
    event_file=V3_EVENT_BY_CONTRACTS,
    db_pool=None,
    start_block=10000001,
    end_block=20000000,
    chunk_size=10000,
    max_workers=3,
    batch_size=100,
):
    logging.info("=" * 60)
    logging.info("BATCH SCANNER")
    logging.info("=" * 60)

    with open(event_file, "r") as f:
        all_pairs = json.load(f)

    all_addresses = list(all_pairs.keys())
    total_pairs = len(all_addresses)
    total_batches = (total_pairs + batch_size - 1) // batch_size

    logging.info(
        f"Pairs: {total_pairs} | Batch size: {batch_size} | Batches: {total_batches}"
    )
    logging.info(f"Blocks: {start_block:,} → {end_block:,}")
    logging.info("=" * 60)

    for i in range(0, total_pairs, batch_size):
        batch = all_addresses[i : i + batch_size]
        batch_num = i // batch_size + 1

        logging.info(f"\nBatch {batch_num}/{total_batches} ({len(batch)} pairs)")

        try:
            scan_blockchain_to_duckdb(
                event_file=event_file,
                db_pool=db_pool,
                start_block=start_block,
                end_block=end_block,
                chunk_size=chunk_size,
                max_workers=max_workers,
                token_filter=batch,
            )
        except KeyboardInterrupt:
            logging.warning(f"Interrupted at batch {batch_num}/{total_batches}")
            raise
        except Exception as e:
            logging.error(f"Batch {batch_num} failed: {e}")
            continue

    final_stats = get_database_stats(db_pool)
    logging.info("=" * 60)
    logging.info("ALL BATCHES COMPLETE")
    logging.info(
        f"Transfers: {final_stats['total_transfers']:,} | Swaps: {final_stats['total_swaps']:,}"
    )
    logging.info(
        f"Mints: {final_stats['total_mints']:,} | Burns: {final_stats['total_burns']:,}"
    )
    logging.info("=" * 60)

    logging.info("Collecting metadata...")
    collect_missing_pair_metadata(db_pool)

    logging.info("Normalizing values...")
    normalize_missing_pairs(db_pool)

    logging.info("✓ Complete")


def query_database(db_pool):
    conn = duckdb.connect(db_pool, read_only=True)

    try:
        print("\n" + "=" * 60)
        print("DATABASE QUERIES")
        print("=" * 60)

        print("\nEvent counts:")
        print(
            f"  Transfers: {conn.execute('SELECT COUNT(*) FROM transfer').fetchone()[0]:,}"
        )
        print(f"  Swaps: {conn.execute('SELECT COUNT(*) FROM swap').fetchone()[0]:,}")
        print(f"  Mints: {conn.execute('SELECT COUNT(*) FROM mint').fetchone()[0]:,}")
        print(f"  Burns: {conn.execute('SELECT COUNT(*) FROM burn').fetchone()[0]:,}")
        print(
            f" Collects: {conn.execute('SELECT COUNT(*) FROM collect').fetchone()[0]:,}"
        )
        print(f" Flashes: {conn.execute('SELECT COUNT(*) FROM flash').fetchone()[0]:,}")
        print(
            f"  Approvals: {conn.execute('SELECT COUNT(*) FROM approval').fetchone()[0]:,}"
        )

        print("\nPair metadata:")
        result = conn.execute(
            """
            SELECT 
                COUNT(DISTINCT t.pair_address) as total_pairs,
                COUNT(DISTINCT pm.pair_address) as pairs_with_metadata,
                COUNT(DISTINCT CASE WHEN pm.token0_decimals IS NOT NULL THEN pm.pair_address END) as pairs_with_decimals
            FROM (SELECT DISTINCT pair_address FROM transfer) t
            LEFT JOIN pair_metadata pm ON t.pair_address = pm.pair_address
            """
        ).fetchone()
        print(f"  Total pairs: {result[0]:,}")
        print(f"  With metadata: {result[1]:,}")
        print(f"  With decimals: {result[2]:,}")

        print("\nMost active pairs (by swaps):")
        result = conn.execute(
            """
            SELECT 
                s.pair_address,
                COALESCE(pm.token0_symbol || '/' || pm.token1_symbol, 'Unknown') as pair_name,
                COUNT(*) as swap_count
            FROM swap s
            LEFT JOIN pair_metadata pm ON s.pair_address = pm.pair_address
            GROUP BY s.pair_address, pair_name
            ORDER BY swap_count DESC
            LIMIT 10
            """
        ).fetchdf()
        print(result)

        return result

    finally:
        conn.close()


def get_pair_info(pair_address, db_pool):
    pair_address = Web3.to_checksum_address(pair_address)
    conn = duckdb.connect(db_pool, read_only=True)

    try:
        print("\n" + "=" * 60)
        print(f"PAIR: {pair_address}")
        print("=" * 60)

        metadata = conn.execute(
            """
            SELECT token0_address, token1_address, token0_symbol, token1_symbol,
                   token0_decimals, token1_decimals, created_block
            FROM pair_metadata
            WHERE pair_address = ?
            """,
            (pair_address,),
        ).fetchone()

        if metadata:
            print(f"\n{metadata[2] or '?'}/{metadata[3] or '?'}")
            print(f"  Token0: {metadata[0]} ({metadata[4] or '?'} decimals)")
            print(f"  Token1: {metadata[1]} ({metadata[5] or '?'} decimals)")
            if metadata[6]:
                print(f"  Created: block {metadata[6]:,}")
        else:
            print("\n⚠️  No metadata found")

        transfers = conn.execute(
            "SELECT COUNT(*) FROM transfer WHERE pair_address = ?", (pair_address,)
        ).fetchone()[0]
        swaps = conn.execute(
            "SELECT COUNT(*) FROM swap WHERE pair_address = ?", (pair_address,)
        ).fetchone()[0]
        mints = conn.execute(
            "SELECT COUNT(*) FROM mint WHERE pair_address = ?", (pair_address,)
        ).fetchone()[0]
        burns = conn.execute(
            "SELECT COUNT(*) FROM burn WHERE pair_address = ?", (pair_address,)
        ).fetchone()[0]

        print(
            f"\nEvents: {transfers:,} transfers | {swaps:,} swaps | {mints:,} mints | {burns:,} burns"
        )

        latest_swap = conn.execute(
            """
            SELECT sqrt_price_x96, liquidity, tick, block_number
            FROM swap
            WHERE pair_address = ?
            ORDER BY block_number DESC
            LIMIT 1
            """,
            (pair_address,),
        ).fetchone()

        if latest_swap and latest_swap[0] is not None:
            print(f"\nLatest swap (block {latest_swap[3]:,}):")
            print(f" sqrtPriceX96: {latest_swap[0]:,}")
            print(f" Liquidity: {latest_swap[1]:,}")
            print(f" Tick: {latest_swap[2]:,}")

        print("=" * 60)

    finally:
        conn.close()


print("✓ Main functions loaded")


# In[ ]:


token_filter = [
    "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",
    "0x3139Ffc91B99aa94DA8A2dc13f1fC36F9BDc98eE",
    "0x12EDE161c702D1494612d19f05992f43aa6A26FB",
    "0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11",
    "0x07F068ca326a469Fc1d87d85d448990C8cBa7dF9",
    "0xAE461cA67B15dc8dc81CE7615e0320dA1A9aB8D5",
    "0xCe407CD7b95B39d3B4d53065E711e713dd5C5999",
    "0x33C2d48Bc95FB7D0199C5C693e7a9F527145a9Af",
]

START_BLOCK = 12369621
END_BLOCK = w3.eth.block_number
START_BLOCK = 10000000
END_BLOCK = 10500000
CHUNK_SIZE = 5000
MAX_WORKERS = 4

try:
    logging.info("=" * 80)
    logging.info("UNISWAP V3 PIPELINE")
    logging.info("=" * 80)
    logging.info(f"Blocks: {START_BLOCK:,} → {END_BLOCK:,} (current)")
    logging.info(f"Config: chunk={CHUNK_SIZE:,} | workers={MAX_WORKERS}")
    db_pool = setup_database(DB_PATH)
    stats = get_database_stats(db_pool)
    logging.info(
        f"DB: {stats['total_transfers']:,} transfers | {stats['total_swaps']:,} swaps | {stats['completed_ranges']} ranges done"
    )
    logging.info("=" * 80)

    if input("Start? (y/n): ").strip().lower() != "y":
        logging.info("Cancelled")
    else:
        scan_blockchain_to_duckdb(
            event_file=V3_EVENT_BY_CONTRACTS,
            db_pool=db_pool,
            start_block=START_BLOCK,
            end_block=END_BLOCK,
            chunk_size=CHUNK_SIZE,
            max_workers=MAX_WORKERS,
        )

        logging.info("\nPost-processing...")
        collect_missing_pair_metadata(db_pool, batch_size=50, max_workers=4)
        normalize_missing_pairs(db_pool, max_workers=4)

        final = get_database_stats(db_pool)
        logging.info("=" * 80)
        logging.info("COMPLETE")
        logging.info(
            f"Transfers: {final['total_transfers']:,} | Swaps: {final['total_swaps']:,}"
        )
        logging.info(
            f"Mints: {final['total_mints']:,} | Burns: {final['total_burns']:,}"
        )
        logging.info(
            f"Pairs: {final['total_pairs']:,} | Blocks: {final['total_blocks']:,}"
        )
        logging.info("=" * 80)

except KeyboardInterrupt:
    logging.warning("\n" + "=" * 80)
    logging.warning("INTERRUPTED - Progress saved")
    stats = get_database_stats(db_pool)
    logging.warning(
        f"State: {stats['total_transfers']:,} transfers | {stats['completed_ranges']} ranges"
    )
    logging.warning("Rerun to resume")
    logging.warning("=" * 80)

except Exception as e:
    logging.error("\n" + "=" * 80)
    logging.error(f"ERROR: {e}")
    logging.error(traceback.format_exc())
    logging.error("=" * 80)
    raise

finally:
    if hasattr(_thread_local, "conn") and _thread_local.conn:
        _thread_local.conn.close()

print("✓ Main ready")
