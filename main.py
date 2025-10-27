#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
import time
import traceback
import tempfile
import threading
import duckdb
import requests
import random

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from datetime import datetime
from plotly.subplots import make_subplots
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from requests.exceptions import HTTPError, ConnectionError
from web3 import Web3
from web3.exceptions import Web3RPCError
from web3.providers.rpc.utils import (
    ExceptionRetryConfiguration,
    REQUEST_RETRY_ALLOWLIST,
)

# Configuration
load_dotenv()
pd.options.display.float_format = "{:20,.4f}".format
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(
    # log_dir, f"uniswap_v3_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_dir,
    f"lol.txt",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(log_filename)],
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
FACTORY_DEPLOYMENT_BLOCK = 12369621
STATE_FILE = "out/V3/V3_final_scan_state.json"
TOKEN_NAME_FILE = "out/V3/V3_token_name.json"
V3_EVENT_BY_CONTRACTS = "out/V3/uniswap_v3_pairs_events.json"
DB_PATH = "out/V3/uniswap_v3.duckdb"
V3_POOL_LIST_FILE = "out/V3/uniswap_v3_pairs_events.json"
ABI_CACHE_FOLDER = "ABI"


class ProviderPool:
    def __init__(self, api_key_dict):
        self.providers = []
        self.provider_names = []

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
        idx = random.randint(0, len(self.providers) - 1)
        return self.providers[idx], self.provider_names[idx]


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
    def __init__(self, db_path):
        self.db_path = db_path
        self.connections = {}
        self.lock = threading.Lock()
        self.connection_timeout = 300
        self.last_used = {}

    def get_connection(self):
        thread_id = threading.get_ident()

        with self.lock:
            now = time.time()

            if thread_id in self.connections:
                self.last_used[thread_id] = now
                return self.connections[thread_id]

            self._cleanup_stale_connections(now)

            conn = duckdb.connect(self.db_path)
            self.connections[thread_id] = conn
            self.last_used[thread_id] = now
            logging.debug(f"Created DB connection for thread {thread_id}")
            return conn

    def _cleanup_stale_connections(self, now):
        stale_threads = [
            tid
            for tid, last_time in self.last_used.items()
            if now - last_time > self.connection_timeout
        ]

        for tid in stale_threads:
            if tid in self.connections:
                try:
                    self.connections[tid].close()
                    del self.connections[tid]
                    del self.last_used[tid]
                    logging.debug(f"Closed stale connection for thread {tid}")
                except Exception as e:
                    logging.warning(f"Error closing stale connection: {e}")

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
            self.last_used.clear()


class TokenCache:
    def __init__(self, cache_file=TOKEN_NAME_FILE):
        self.cache_file = cache_file
        self.cache = {}
        self.lock = threading.Lock()
        self.dirty = False
        self.last_save = time.time()
        self.save_interval = 60

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
            self.dirty = True

            now = time.time()
            if now - self.last_save >= self.save_interval:
                self._save_to_disk()

    def _save_to_disk(self):
        if not self.dirty:
            return

        try:
            dirn = os.path.dirname(self.cache_file) or "."
            os.makedirs(dirn, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=dirn, text=True)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.cache_file)
            self.dirty = False
            self.last_save = time.time()
        except Exception as e:
            logging.warning(f"Failed to save token cache: {e}")

    def flush(self):
        with self.lock:
            self._save_to_disk()


class RateLimiter:
    def __init__(self, max_calls_per_second):
        self.min_interval = 1.0 / max_calls_per_second
        self.last_call = 0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            time_since_last = now - self.last_call
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                time.sleep(sleep_time)
            self.last_call = time.time()


ETHERSCAN_RATE_LIMITER = RateLimiter(5)
TOKEN_CACHE = TokenCache()
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

    ETHERSCAN_RATE_LIMITER.wait()

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

MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "target", "type": "address"},
                    {"name": "callData", "type": "bytes"},
                ],
                "name": "calls",
                "type": "tuple[]",
            }
        ],
        "name": "aggregate",
        "outputs": [
            {"name": "blockNumber", "type": "uint256"},
            {"name": "returnData", "type": "bytes[]"},
        ],
        "stateMutability": "payable",
        "type": "function",
    }
]
EVENT_SIGNATURE_CACHE = {}
ABI_HASH_CACHE = {}
EVENT_CACHE_LOCK = threading.Lock()


def get_abi_hash(abi):
    return hash(json.dumps(abi, sort_keys=True))


def decode_logs_for_contract(contract_address, logs, provider):
    abi, contract = ABI_CACHE.get_contract(contract_address, provider)

    if not abi or not contract:
        return [create_transaction_dict(log, provider, {}) for log in logs]

    event_map = get_event_signature_map(contract_address, abi)
    transactions = []

    for log in logs:
        if log.get("topics") and len(log["topics"]) > 0:
            event_signature_hash = log["topics"][0].hex()

            if event_signature_hash in event_map:
                event_name = event_map[event_signature_hash]

                try:
                    decoded = contract.events[event_name]().process_log(log)
                    topics = {
                        "event": event_name,
                        "args": dict(decoded["args"]),
                    }
                except Exception:
                    topics = {}
            else:
                topics = {}
        else:
            topics = {}

        transactions.append(create_transaction_dict(log, provider, topics))

    return transactions


def get_event_signature_map(contract_address, abi):
    with EVENT_CACHE_LOCK:
        if contract_address in EVENT_SIGNATURE_CACHE:
            return EVENT_SIGNATURE_CACHE[contract_address]

        abi_hash = get_abi_hash(abi)

        if abi_hash in ABI_HASH_CACHE:
            event_map = ABI_HASH_CACHE[abi_hash]
            EVENT_SIGNATURE_CACHE[contract_address] = event_map
            return event_map

        event_map = build_event_signature_map(abi)
        EVENT_SIGNATURE_CACHE[contract_address] = event_map
        ABI_HASH_CACHE[abi_hash] = event_map
        return event_map


def fetch_metadata_multicall(pair_addresses, provider=None):
    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    multicall = provider.eth.contract(address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI)

    calls = []
    call_map = []

    for pair_address in pair_addresses:
        pair_address = provider.to_checksum_address(pair_address)

        pool_contract = provider.eth.contract(
            address=pair_address, abi=MINIMAL_UNISWAP_V3_POOL_ABI
        )

        calls.append(
            (
                pair_address,
                pool_contract.functions.token0().build_transaction(
                    {"to": pair_address}
                )["data"],
            )
        )
        call_map.append(("token0", pair_address))

        calls.append(
            (
                pair_address,
                pool_contract.functions.token1().build_transaction(
                    {"to": pair_address}
                )["data"],
            )
        )
        call_map.append(("token1", pair_address))

        calls.append(
            (
                pair_address,
                pool_contract.functions.fee().build_transaction({"to": pair_address})[
                    "data"
                ],
            )
        )
        call_map.append(("fee", pair_address))

    block_num, results = multicall.functions.aggregate(calls).call()

    metadata_dict = {}
    for idx, (call_type, pair_addr) in enumerate(call_map):
        if pair_addr not in metadata_dict:
            metadata_dict[pair_addr] = {}

        if call_type == "token0":
            metadata_dict[pair_addr]["token0_address"] = provider.to_checksum_address(
                "0x" + results[idx].hex()[-40:]
            )
        elif call_type == "token1":
            metadata_dict[pair_addr]["token1_address"] = provider.to_checksum_address(
                "0x" + results[idx].hex()[-40:]
            )
        elif call_type == "fee":
            metadata_dict[pair_addr]["fee_tier"] = int.from_bytes(
                results[idx], byteorder="big"
            )

    return metadata_dict


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


EVENT_SIGNATURE_CACHE = {}
EVENT_CACHE_LOCK = threading.Lock()


def get_event_signature_map(contract_address, abi):
    with EVENT_CACHE_LOCK:
        if contract_address in EVENT_SIGNATURE_CACHE:
            return EVENT_SIGNATURE_CACHE[contract_address]

        event_map = build_event_signature_map(abi)
        EVENT_SIGNATURE_CACHE[contract_address] = event_map
        return event_map


def setup_database(db_path=DB_PATH, schema_path="./out/V3/database/schema.sql"):
    conn = duckdb.connect(db_path)
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    conn.execute(schema_sql)
    conn.close()
    logging.info("✓ Database schema created successfully")
    return DuckDBConnectionPool(db_path)


DB_POOL = setup_database()


def batch_insert_events(events, worker_id="main"):
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
                    str(args.get("value", 0)),
                )
            )

    conn = DB_POOL.get_connection()

    try:
        conn.execute("BEGIN TRANSACTION")

        if transfers:
            conn.executemany(
                "INSERT INTO transfer (transaction_hash, log_index, block_number, pair_address, from_address, to_address, value) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (transaction_hash, log_index) DO NOTHING",
                transfers,
            )

        if swaps:
            conn.executemany(
                "INSERT INTO swap (transaction_hash, log_index, block_number, pair_address, sender, recipient, amount0, amount1, sqrt_price_x96, liquidity, tick) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (transaction_hash, log_index) DO NOTHING",
                swaps,
            )

        if mints:
            conn.executemany(
                "INSERT INTO mint (transaction_hash, log_index, block_number, pair_address, owner, tick_lower, tick_upper, sender, amount, amount0, amount1) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (transaction_hash, log_index) DO NOTHING",
                mints,
            )

        if burns:
            conn.executemany(
                "INSERT INTO burn (transaction_hash, log_index, block_number, pair_address, owner, tick_lower, tick_upper, amount, amount0, amount1) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (transaction_hash, log_index) DO NOTHING",
                burns,
            )

        if collects:
            conn.executemany(
                "INSERT INTO collect (transaction_hash, log_index, block_number, pair_address, owner, recipient, tick_lower, tick_upper, amount0, amount1) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (transaction_hash, log_index) DO NOTHING",
                collects,
            )

        if flashes:
            conn.executemany(
                "INSERT INTO flash (transaction_hash, log_index, block_number, pair_address, sender, recipient, amount0, amount1, paid0, paid1) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (transaction_hash, log_index) DO NOTHING",
                flashes,
            )

        if approvals:
            conn.executemany(
                "INSERT INTO approval (transaction_hash, log_index, block_number, pair_address, owner, spender, value) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (transaction_hash, log_index) DO NOTHING",
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
        logging.error(f"[{worker_id}] batch_insert_events failed: {e}")
        raise


def mark_range_completed(start_block, end_block, worker_id="main"):
    conn = DB_POOL.get_connection()
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


def mark_range_processing(start_block, end_block, worker_id="main"):
    conn = DB_POOL.get_connection()
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


def get_completed_ranges():
    conn = DB_POOL.get_connection()
    result = conn.execute(
        """
        SELECT start_block, end_block 
        FROM processing_state 
        WHERE status = 'completed'
    """
    ).fetchall()
    return set((r[0], r[1]) for r in result)


def get_database_stats():
    conn = DB_POOL.get_connection()
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
    token0_symbol=None,
    token1_symbol=None,
    token0_decimals=None,
    token1_decimals=None,
    fee_tier=None,
    tick_spacing=None,
    created_block=None,
):
    conn = DB_POOL.get_connection()
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


def get_pair_metadata(pair_address):
    conn = DB_POOL.get_connection()
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


def batch_insert_block_metadata(blocks_data):
    if not blocks_data:
        return 0

    conn = DB_POOL.get_connection()
    conn.executemany(
        """
        INSERT INTO block_metadata (block_number, block_timestamp, block_hash)
        VALUES (?, ?, ?)
        ON CONFLICT (block_number) DO NOTHING
    """,
        blocks_data,
    )
    return len(blocks_data)


def normalize_values_for_pair(pair_address):
    metadata = get_pair_metadata(pair_address)

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

    conn = DB_POOL.get_connection()

    conn.execute("BEGIN TRANSACTION")

    try:
        conn.execute(
            "UPDATE transfer SET value_normalized = CAST(value AS DOUBLE) / POWER(10, ?) WHERE pair_address = ? AND value_normalized IS NULL",
            (lp_decimals, pair_address),
        )

        conn.execute(
            "UPDATE mint SET amount0_normalized = CAST(amount0 AS DOUBLE) / POWER(10, ?), amount1_normalized = CAST(amount1 AS DOUBLE) / POWER(10, ?) WHERE pair_address = ? AND amount0_normalized IS NULL",
            (token0_decimals, token1_decimals, pair_address),
        )

        conn.execute(
            "UPDATE burn SET amount0_normalized = CAST(amount0 AS DOUBLE) / POWER(10, ?), amount1_normalized = CAST(amount1 AS DOUBLE) / POWER(10, ?) WHERE pair_address = ? AND amount0_normalized IS NULL",
            (token0_decimals, token1_decimals, pair_address),
        )

        conn.execute(
            "UPDATE swap SET amount0_normalized = CAST(amount0 AS DOUBLE) / POWER(10, ?), amount1_normalized = CAST(amount1 AS DOUBLE) / POWER(10, ?) WHERE pair_address = ? AND amount0_normalized IS NULL",
            (token0_decimals, token1_decimals, pair_address),
        )

        conn.execute(
            "UPDATE collect SET amount0_normalized = CAST(amount0 AS DOUBLE) / POWER(10, ?), amount1_normalized = CAST(amount1 AS DOUBLE) / POWER(10, ?) WHERE pair_address = ? AND amount0_normalized IS NULL",
            (token0_decimals, token1_decimals, pair_address),
        )

        conn.execute("COMMIT")

    except Exception as e:
        conn.execute("ROLLBACK")
        logging.error(f"Failed to normalize {pair_address}: {e}")
        raise


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

    pair_contract = get_contract_with_fallback(
        pair_address, provider, contract_type="uniswap_v3_pool"
    )

    def safe_call(func, default=None):
        try:
            return func()
        except Exception:
            return default

    token0_address = safe_call(lambda: pair_contract.functions.token0().call())
    token1_address = safe_call(lambda: pair_contract.functions.token1().call())

    if not token0_address or not token1_address:
        return None

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
        "fee_tier": safe_call(lambda: pair_contract.functions.fee().call()),
        "tick_spacing": safe_call(lambda: pair_contract.functions.tickSpacing().call()),
        "token0_symbol": safe_call(lambda: token0_contract.functions.symbol().call()),
        "token0_decimals": safe_call(
            lambda: token0_contract.functions.decimals().call()
        ),
        "token1_symbol": safe_call(lambda: token1_contract.functions.symbol().call()),
        "token1_decimals": safe_call(
            lambda: token1_contract.functions.decimals().call()
        ),
    }

    return metadata


def fetch_block_metadata_batch(block_numbers, provider=None, batch_size=100):
    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    blocks_data = []

    for i in range(0, len(block_numbers), batch_size):
        batch = block_numbers[i : i + batch_size]

        try:
            batch_results = []
            for block_num in batch:
                try:
                    block = provider.eth.get_block(block_num)
                    batch_results.append(
                        (
                            block_num,
                            datetime.fromtimestamp(block["timestamp"]),
                            block["hash"].hex(),
                        )
                    )
                except Exception as e:
                    logging.warning(f"Failed to fetch block {block_num}: {e}")

            blocks_data.extend(batch_results)

            if len(blocks_data) % 1000 == 0:
                logging.info(f"Fetched {len(blocks_data)}/{len(block_numbers)} blocks")

        except Exception as e:
            logging.error(f"Batch failed: {e}")

    return blocks_data


def get_blocks_in_range_efficiently(start_block, end_block, provider=None):
    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    conn = DB_POOL.get_connection()

    existing = conn.execute(
        "SELECT block_number FROM block_metadata WHERE block_number BETWEEN ? AND ?",
        (start_block, end_block),
    ).fetchall()
    existing_set = {b[0] for b in existing}

    missing = [b for b in range(start_block, end_block + 1) if b not in existing_set]

    if not missing:
        return []

    blocks_data = []

    for block_num in missing:
        try:
            block = provider.eth.get_block(block_num)
            blocks_data.append(
                (
                    block_num,
                    datetime.fromtimestamp(block["timestamp"]),
                    block["hash"].hex(),
                )
            )
        except Exception as e:
            logging.warning(f"Failed block {block_num}: {e}")

    if blocks_data:
        conn.executemany(
            "INSERT INTO block_metadata (block_number, block_timestamp, block_hash) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
            blocks_data,
        )

    return blocks_data


def fetch_block_metadata(block_number, provider=None, retry_count=0, max_retries=3):
    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    try:
        block = provider.eth.get_block(block_number)
        return (
            block_number,
            datetime.fromtimestamp(block["timestamp"]),
            block["hash"].hex(),
        )

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


def parallel_fetch_with_backoff(items, fetch_func, max_workers=4, desc="Processing"):
    results = [None] * len(items)
    results_lock = threading.Lock()

    def worker(idx, item):
        provider, provider_name = PROVIDER_POOL.get_provider()
        try:
            result = fetch_func(item, provider)
            with results_lock:
                results[idx] = result
            return result
        except Exception as e:
            logging.warning(f"{desc} failed for item {idx}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, i, item): i for i, item in enumerate(items)}

        completed = 0
        total = len(items)

        for future in as_completed(futures):
            completed += 1
            if completed % 10 == 0 or completed == total:
                logging.info(
                    f"{desc}: {completed}/{total} ({100*completed/total:.1f}%)"
                )

    return [r for r in results if r is not None]


def estimate_chunk_size(start_block, end_block, addresses):
    conn = DB_POOL.get_connection()

    sample_size = min(100, end_block - start_block)
    sample_start = start_block
    sample_end = start_block + sample_size

    provider, _ = PROVIDER_POOL.get_provider()

    try:
        logs = provider.eth.get_logs(
            {
                "fromBlock": sample_start,
                "toBlock": sample_end,
                "address": addresses[:100],
            }
        )

        events_per_block = len(logs) / sample_size if sample_size > 0 else 0

        if events_per_block > 100:
            return 1000
        elif events_per_block > 10:
            return 5000
        else:
            return 10000

    except:
        return 10000


def compute_pool_hourly_stats(pool_address, start_block, end_block):
    conn = DB_POOL.get_connection()

    metadata = get_pair_metadata(pool_address)
    if not metadata:
        return

    fee_tier = metadata.get("fee_tier", 3000)
    fee_percentage = fee_tier / 1000000

    conn.execute(
        """
        INSERT INTO pool_hourly_stats (
            pool_address,
            hour_timestamp,
            swap_count,
            volume_token0,
            volume_token1,
            fees_token0,
            fees_token1,
            liquidity_avg,
            price_avg
        )
        SELECT 
            s.pair_address,
            DATE_TRUNC('hour', b.block_timestamp) as hour,
            COUNT(*) as swap_count,
            SUM(ABS(s.amount0_normalized)) as volume_token0,
            SUM(ABS(s.amount1_normalized)) as volume_token1,
            SUM(ABS(s.amount0_normalized)) * ? as fees_token0,
            SUM(ABS(s.amount1_normalized)) * ? as fees_token1,
            AVG(s.liquidity) as liquidity_avg,
            AVG(POWER(s.sqrt_price_x96 / POWER(2.0, 96), 2)) as price_avg
        FROM swap s
        JOIN block_metadata b ON s.block_number = b.block_number
        WHERE s.pair_address = ?
            AND s.block_number BETWEEN ? AND ?
        GROUP BY s.pair_address, hour
        ON CONFLICT (pool_address, hour_timestamp) DO UPDATE SET
            swap_count = EXCLUDED.swap_count,
            volume_token0 = EXCLUDED.volume_token0,
            volume_token1 = EXCLUDED.volume_token1,
            fees_token0 = EXCLUDED.fees_token0,
            fees_token1 = EXCLUDED.fees_token1,
            liquidity_avg = EXCLUDED.liquidity_avg,
            price_avg = EXCLUDED.price_avg
    """,
        (fee_percentage, fee_percentage, pool_address, start_block, end_block),
    )

    logging.info(f"✓ Computed hourly stats for {pool_address[:10]}")


def update_pool_current_state(pool_address):
    conn = DB_POOL.get_connection()

    latest_swap = conn.execute(
        """
        SELECT sqrt_price_x96, tick, liquidity, block_number
        FROM swap
        WHERE pair_address = ?
        ORDER BY block_number DESC, log_index DESC
        LIMIT 1
    """,
        (pool_address,),
    ).fetchone()

    if not latest_swap:
        return

    total_stats = conn.execute(
        """
        SELECT 
            COUNT(*) as total_swaps,
            SUM(ABS(amount0_normalized)) as total_volume0,
            SUM(ABS(amount1_normalized)) as total_volume1
        FROM swap
        WHERE pair_address = ?
    """,
        (pool_address,),
    ).fetchone()

    conn.execute(
        """
        INSERT INTO pool_current_state (
            pool_address,
            current_sqrt_price_x96,
            current_tick,
            current_liquidity,
            last_swap_block,
            total_swaps,
            total_volume_token0,
            total_volume_token1,
            updated_at
        ) VALUES (?, CAST(? AS HUGEINT), CAST(? AS HUGEINT), ?, ?, ?, ?, ?, NOW())
        ON CONFLICT (pool_address) DO UPDATE SET
            current_sqrt_price_x96 = EXCLUDED.current_sqrt_price_x96,
            current_tick = EXCLUDED.current_tick,
            current_liquidity = EXCLUDED.current_liquidity,
            last_swap_block = EXCLUDED.last_swap_block,
            total_swaps = EXCLUDED.total_swaps,
            total_volume_token0 = EXCLUDED.total_volume_token0,
            total_volume_token1 = EXCLUDED.total_volume_token1,
            updated_at = NOW()
    """,
        (
            pool_address,
            latest_swap[0],
            latest_swap[1],
            latest_swap[2],
            latest_swap[3],
            total_stats[0],
            total_stats[1],
            total_stats[2],
        ),
    )


def build_position_state(pool_address):
    conn = DB_POOL.get_connection()

    positions = {}

    mints = conn.execute(
        """
        SELECT owner, tick_lower, tick_upper, amount0_normalized, amount1_normalized, block_number
        FROM mint
        WHERE pair_address = ?
        ORDER BY block_number ASC, log_index ASC
    """,
        (pool_address,),
    ).fetchall()

    burns = conn.execute(
        """
        SELECT owner, tick_lower, tick_upper, amount0_normalized, amount1_normalized, block_number
        FROM burn
        WHERE pair_address = ?
        ORDER BY block_number ASC, log_index ASC
    """,
        (pool_address,),
    ).fetchall()

    for owner, tick_lower, tick_upper, amount0, amount1, block_number in mints:
        key = (owner, tick_lower, tick_upper)
        if key not in positions:
            positions[key] = {"amount0": 0, "amount1": 0, "last_block": block_number}
        positions[key]["amount0"] += amount0 or 0
        positions[key]["amount1"] += amount1 or 0
        positions[key]["last_block"] = block_number

    for owner, tick_lower, tick_upper, amount0, amount1, block_number in burns:
        key = (owner, tick_lower, tick_upper)
        if key not in positions:
            positions[key] = {"amount0": 0, "amount1": 0, "last_block": block_number}
        positions[key]["amount0"] -= amount0 or 0
        positions[key]["amount1"] -= amount1 or 0
        positions[key]["last_block"] = block_number

    active_positions = []
    for (owner, tick_lower, tick_upper), data in positions.items():
        if abs(data["amount0"]) > 1e-10 or abs(data["amount1"]) > 1e-10:
            active_positions.append(
                {
                    "owner": owner,
                    "tick_lower": tick_lower,
                    "tick_upper": tick_upper,
                    "amount0": data["amount0"],
                    "amount1": data["amount1"],
                    "last_block": data["last_block"],
                }
            )

    return active_positions


def get_liquidity_distribution(pool_address, tick_spacing=60):
    conn = DB_POOL.get_connection()

    positions = build_position_state(pool_address)

    tick_liquidity = {}

    for pos in positions:
        tick_lower = pos["tick_lower"]
        tick_upper = pos["tick_upper"]
        amount0 = pos["amount0"]
        amount1 = pos["amount1"]

        for tick in range(tick_lower, tick_upper + 1, tick_spacing):
            if tick not in tick_liquidity:
                tick_liquidity[tick] = {"token0": 0, "token1": 0}
            tick_liquidity[tick]["token0"] += amount0
            tick_liquidity[tick]["token1"] += amount1

    distribution = []
    for tick, liquidity in sorted(tick_liquidity.items()):
        distribution.append(
            {
                "tick": tick,
                "token0_liquidity": liquidity["token0"],
                "token1_liquidity": liquidity["token1"],
            }
        )

    return distribution


def denormalize_timestamps():
    conn = DB_POOL.get_connection()

    logging.info("Denormalizing timestamps...")

    conn.execute(
        """
        UPDATE swap s
        SET block_timestamp = b.block_timestamp
        FROM block_metadata b
        WHERE s.block_number = b.block_number
        AND s.block_timestamp IS NULL
    """
    )

    conn.execute(
        """
        UPDATE mint m
        SET block_timestamp = b.block_timestamp
        FROM block_metadata b
        WHERE m.block_number = b.block_number
        AND m.block_timestamp IS NULL
    """
    )

    conn.execute(
        """
        UPDATE burn bn
        SET block_timestamp = b.block_timestamp
        FROM block_metadata b
        WHERE bn.block_number = b.block_number
        AND bn.block_timestamp IS NULL
    """
    )

    logging.info("✓ Timestamps denormalized")


def denormalize_pair_symbols():
    conn = DB_POOL.get_connection()

    logging.info("Denormalizing pair symbols...")

    conn.execute(
        """
        UPDATE swap s
        SET token0_symbol = pm.token0_symbol,
            token1_symbol = pm.token1_symbol
        FROM pair_metadata pm
        WHERE s.pair_address = pm.pair_address
        AND s.token0_symbol IS NULL
    """
    )

    logging.info("✓ Pair symbols denormalized")


def refresh_pool_summary():
    conn = DB_POOL.get_connection()

    logging.info("Refreshing pool summary...")

    conn.execute("DROP TABLE IF EXISTS pool_summary")

    conn.execute(
        """
        CREATE TABLE pool_summary AS
        SELECT 
            pm.pair_address,
            pm.token0_symbol,
            pm.token1_symbol,
            pm.token0_decimals,
            pm.token1_decimals,
            pm.fee_tier,
            COUNT(DISTINCT s.transaction_hash) as total_swaps,
            COUNT(DISTINCT m.transaction_hash) as total_mints,
            COUNT(DISTINCT b.transaction_hash) as total_burns,
            SUM(ABS(s.amount0_normalized)) as total_volume_token0,
            SUM(ABS(s.amount1_normalized)) as total_volume_token1,
            MIN(s.block_number) as first_swap_block,
            MAX(s.block_number) as last_swap_block
        FROM pair_metadata pm
        LEFT JOIN swap s ON pm.pair_address = s.pair_address
        LEFT JOIN mint m ON pm.pair_address = m.pair_address
        LEFT JOIN burn b ON pm.pair_address = b.pair_address
        GROUP BY pm.pair_address, pm.token0_symbol, pm.token1_symbol, 
                 pm.token0_decimals, pm.token1_decimals, pm.fee_tier
    """
    )

    conn.execute(
        "CREATE INDEX idx_pool_summary_volume ON pool_summary(total_volume_token0)"
    )
    conn.execute("CREATE INDEX idx_pool_summary_swaps ON pool_summary(total_swaps)")

    logging.info("✓ Pool summary refreshed")


def aggregate_all_pools(max_workers=8):
    conn = DB_POOL.get_connection()

    pools = conn.execute("SELECT DISTINCT pair_address FROM swap").fetchall()
    pools = [p[0] for p in pools]

    logging.info(f"Aggregating stats for {len(pools)} pools...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for pool in pools:
            futures.append(executor.submit(update_pool_current_state, pool))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Failed to aggregate pool: {e}")


def sqrt_price_x96_to_price(sqrt_price_x96, token0_decimals, token1_decimals):
    price = (sqrt_price_x96 / (2**96)) ** 2
    decimal_adjustment = (10**token0_decimals) / (10**token1_decimals)
    return price * decimal_adjustment


def tick_to_price(tick, token0_decimals, token1_decimals):
    price = 1.0001**tick
    decimal_adjustment = (10**token0_decimals) / (10**token1_decimals)
    return price * decimal_adjustment


def get_pool_price_history(pool_address, start_block=None, end_block=None, limit=1000):
    conn = DB_POOL.get_connection()

    metadata = get_pair_metadata(pool_address)
    if (
        not metadata
        or not metadata["token0_decimals"]
        or not metadata["token1_decimals"]
    ):
        logging.error(f"Cannot compute price: missing decimals for {pool_address}")
        return None

    token0_decimals = metadata["token0_decimals"]
    token1_decimals = metadata["token1_decimals"]

    query = """
        SELECT 
            s.block_number,
            b.block_timestamp,
            s.sqrt_price_x96,
            s.tick,
            s.liquidity
        FROM swap s
        JOIN block_metadata b ON s.block_number = b.block_number
        WHERE s.pair_address = ?
    """

    params = [pool_address]

    if start_block:
        query += " AND s.block_number >= ?"
        params.append(start_block)

    if end_block:
        query += " AND s.block_number <= ?"
        params.append(end_block)

    query += " ORDER BY s.block_number DESC, s.log_index DESC LIMIT ?"
    params.append(limit)

    results = conn.execute(query, params).fetchall()

    price_history = []
    for row in results:
        block_number, timestamp, sqrt_price_x96, tick, liquidity = row

        if sqrt_price_x96:
            price = sqrt_price_x96_to_price(
                sqrt_price_x96, token0_decimals, token1_decimals
            )
            price_history.append(
                {
                    "block_number": block_number,
                    "timestamp": timestamp,
                    "price": price,
                    "tick": tick,
                    "liquidity": liquidity,
                }
            )

    return price_history


def calculate_tvl_for_pool(pool_address, block_number=None):
    conn = DB_POOL.get_connection()

    metadata = get_pair_metadata(pool_address)
    if not metadata:
        return None

    if block_number:
        query = """
            SELECT 
                SUM(CASE WHEN amount0_normalized > 0 THEN amount0_normalized ELSE 0 END) -
                SUM(CASE WHEN amount0_normalized < 0 THEN ABS(amount0_normalized) ELSE 0 END) as net_token0,
                SUM(CASE WHEN amount1_normalized > 0 THEN amount1_normalized ELSE 0 END) -
                SUM(CASE WHEN amount1_normalized < 0 THEN ABS(amount1_normalized) ELSE 0 END) as net_token1
            FROM (
                SELECT amount0_normalized, amount1_normalized FROM mint WHERE pair_address = ? AND block_number <= ?
                UNION ALL
                SELECT -amount0_normalized, -amount1_normalized FROM burn WHERE pair_address = ? AND block_number <= ?
            )
        """
        result = conn.execute(
            query, (pool_address, block_number, pool_address, block_number)
        ).fetchone()
    else:
        query = """
            SELECT 
                SUM(CASE WHEN amount0_normalized > 0 THEN amount0_normalized ELSE 0 END) -
                SUM(CASE WHEN amount0_normalized < 0 THEN ABS(amount0_normalized) ELSE 0 END) as net_token0,
                SUM(CASE WHEN amount1_normalized > 0 THEN amount1_normalized ELSE 0 END) -
                SUM(CASE WHEN amount1_normalized < 0 THEN ABS(amount1_normalized) ELSE 0 END) as net_token1
            FROM (
                SELECT amount0_normalized, amount1_normalized FROM mint WHERE pair_address = ?
                UNION ALL
                SELECT -amount0_normalized, -amount1_normalized FROM burn WHERE pair_address = ?
            )
        """
        result = conn.execute(query, (pool_address, pool_address)).fetchone()

    return {
        "token0_tvl": result[0] or 0,
        "token1_tvl": result[1] or 0,
        "token0_symbol": metadata.get("token0_symbol"),
        "token1_symbol": metadata.get("token1_symbol"),
    }


def generate_block_ranges_adaptive(start_block, end_block, addresses):
    completed = get_completed_ranges()
    ranges = []
    current = start_block

    while current <= end_block:
        chunk_size = estimate_chunk_size(current, end_block, addresses)
        end = min(current + chunk_size - 1, end_block)

        if (current, end) not in completed:
            ranges.append((current, end))

        current = end + 1

    return ranges


def generate_v3_pool_list(
    output_file=V3_POOL_LIST_FILE, start_block=FACTORY_DEPLOYMENT_BLOCK, max_workers=4
):
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            pools_dict = json.load(f)
        max_block = max(
            [p.get("created_block", 0) for p in pools_dict.values()] or [start_block]
        )
        logging.info(
            f"Loaded {len(pools_dict)} existing pools, last block: {max_block:,}"
        )
        provider, _ = PROVIDER_POOL.get_provider()
        current_block = provider.eth.block_number
        if max_block >= current_block - 10:
            logging.info(f"Pool list is up to date")
            return list(pools_dict.keys())
        logging.info(
            f"Updating pool list from block {max_block + 1:,} to {current_block:,}"
        )
        start_block = max_block + 1
    else:
        pools_dict = {}
        provider, _ = PROVIDER_POOL.get_provider()
        current_block = provider.eth.block_number

    logging.info("Generating V3 pool list from PoolCreated events...")
    factory_abi = get_abi(UNISWAP_V3_FACTORY)
    chunk_size = 10000
    ranges = [
        (fb, min(fb + chunk_size - 1, current_block))
        for fb in range(start_block, current_block + 1, chunk_size)
    ]

    def fetch_pool_range(range_tuple, provider):
        from_block, to_block = range_tuple
        for retry in range(5):
            try:
                factory_contract = provider.eth.contract(
                    address=UNISWAP_V3_FACTORY, abi=factory_abi
                )
                logs = factory_contract.events.PoolCreated.get_logs(
                    from_block=from_block, to_block=to_block
                )
                pools = {}
                for log in logs:
                    pools[log.args.pool] = {
                        "token0": log.args.token0,
                        "token1": log.args.token1,
                        "fee": log.args.fee,
                        "tickSpacing": log.args.tickSpacing,
                        "created_block": log.blockNumber,
                    }
                if pools:
                    logging.info(
                        f"[{from_block:,} - {to_block:,}] Found {len(pools)} pools"
                    )
                return pools
            except Exception as e:
                if "429" in str(e) or "Too Many Requests" in str(e):
                    wait = 3 * (retry + 1)
                    logging.warning(
                        f"Rate limit [{from_block:,}-{to_block:,}], retry in {wait}s"
                    )
                    time.sleep(wait)
                    provider, _ = PROVIDER_POOL.get_provider()
                    continue
                else:
                    logging.error(f"Error [{from_block:,}-{to_block:,}]: {e}")
                    return {}
        logging.error(f"Failed [{from_block:,}-{to_block:,}] after 5 retries")
        return {}

    all_pools_list = parallel_fetch_with_backoff(
        ranges, fetch_pool_range, max_workers, "Fetching pools"
    )

    for pool_batch in all_pools_list:
        pools_dict.update(pool_batch)

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(pools_dict, f, indent=2)

    logging.info(f"✓ Saved {len(pools_dict)} V3 pools to {output_file}")
    return list(pools_dict.keys())


def fetch_and_store_uniswap_pair_metadata(
    pair_address, created_block=None, provider=None, max_retries=3
):
    existing = get_pair_metadata(pair_address)
    if existing and existing["token0_decimals"] is not None:
        logging.debug(f"Using cached metadata for {pair_address[:10]}")
        return existing

    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    # Retry logic
    for attempt in range(max_retries):
        try:
            metadata = fetch_uniswap_pair_metadata(pair_address, provider)

            if metadata:
                insert_pair_metadata(
                    pair_address=metadata["pair_address"],
                    token0_address=metadata["token0_address"],
                    token1_address=metadata["token1_address"],
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


def parallel_fetch_with_backoff(
    items, fetch_func, max_workers=4, desc="Processing", max_retries=3
):
    results = [None] * len(items)
    results_lock = threading.Lock()

    def worker(idx, item, retry_count=0):
        provider, provider_name = PROVIDER_POOL.get_provider()
        try:
            result = fetch_func(item, provider)
            with results_lock:
                results[idx] = result
            return result
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                if retry_count < max_retries:
                    wait_time = (2**retry_count) + random.uniform(0, 1)
                    logging.warning(
                        f"{desc} failed for item {idx} (429 rate limit), retrying in {wait_time:.1f}s... (attempt {retry_count + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    return worker(idx, item, retry_count + 1)
                else:
                    logging.error(
                        f"{desc} failed for item {idx} after {max_retries} retries: {e}"
                    )
                    return None
            else:
                logging.warning(f"{desc} failed for item {idx}: {e}")
                return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, i, item): i for i, item in enumerate(items)}
        completed = 0
        total = len(items)
        for future in as_completed(futures):
            completed += 1
            if completed % 10 == 0 or completed == total:
                logging.info(
                    f"{desc}: {completed}/{total} ({100*completed/total:.1f}%)"
                )

    return [r for r in results if r is not None]


def fetch_and_store_block_metadata(block_numbers, max_workers=4):
    if not block_numbers:
        return 0

    def fetch_single_block(block_num, provider):
        try:
            block = provider.eth.get_block(block_num)
            return (
                block_num,
                datetime.fromtimestamp(block["timestamp"]),
                block["hash"].hex(),
            )
        except Exception as e:
            logging.warning(f"Failed to fetch block {block_num}: {e}")
            return None

    blocks_data = parallel_fetch_with_backoff(
        list(block_numbers),
        fetch_single_block,
        max_workers=max_workers,
        desc="Fetching block metadata",
    )

    blocks_data = [b for b in blocks_data if b is not None]

    if blocks_data:
        conn = DB_POOL.get_connection()
        conn.executemany(
            "INSERT INTO block_metadata (block_number, block_timestamp, block_hash) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
            blocks_data,
        )
        logging.info(f"✓ Stored metadata for {len(blocks_data)} blocks")

    return len(blocks_data)


def collect_missing_pair_metadata(batch_size=100, provider=None, max_workers=12):
    conn = DB_POOL.get_connection()

    all_pairs = conn.execute(
        """
        SELECT DISTINCT pair_address FROM (
            SELECT DISTINCT pair_address FROM transfer
            UNION
            SELECT DISTINCT pair_address FROM swap
            UNION
            SELECT DISTINCT pair_address FROM mint
            UNION
            SELECT DISTINCT pair_address FROM burn
            UNION
            SELECT DISTINCT pair_address FROM collect
            UNION
            SELECT DISTINCT pair_address FROM flash
        )
        WHERE pair_address IS NOT NULL
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
                    fetch_and_store_uniswap_pair_metadata, pair, None, provider
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


def normalize_missing_pairs(max_workers=8):
    conn = DB_POOL.get_connection()

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
            executor.submit(normalize_values_for_pair, pair): pair
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
    start_block, end_block, batch_size=1000, provider=None, max_workers=8
):
    conn = DB_POOL.get_connection()

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
        fetch_and_store_block_metadata(batch, provider, max_workers)
        logging.info(
            f"Progress: {min(i + batch_size, len(missing_blocks))}/{len(missing_blocks)} blocks processed"
        )


# In[11]:
def build_event_signature_map(abi):
    event_map = {}
    for item in abi:
        if item.get("type") == "event":
            event_signature = (
                f'{item["name"]}({",".join(i["type"] for i in item["inputs"])})'
            )
            event_hash = w3.keccak(text=event_signature).hex()
            event_map[event_hash] = item["name"]
    return event_map


def create_transaction_dict(log, provider, topics):
    transaction = {
        "transactionHash": provider.to_hex(log["transactionHash"]),
        "blockNumber": log["blockNumber"],
        "logIndex": log.get("logIndex", 0),
        "address": log["address"],
        "data": provider.to_hex(log["data"]),
    }

    transaction.update(topics)

    return transaction


def decode_logs_for_contract(contract_address, logs, provider):
    abi, contract = ABI_CACHE.get_contract(contract_address, provider)

    if not abi or not contract:
        return [create_transaction_dict(log, provider, {}) for log in logs]

    event_map = get_event_signature_map(contract_address, abi)

    transactions = []
    for log in logs:
        if log.get("topics") and len(log["topics"]) > 0:
            event_signature_hash = log["topics"][0].hex()

            if event_signature_hash in event_map:
                event_name = event_map[event_signature_hash]
                try:
                    decoded = contract.events[event_name]().process_log(log)
                    topics = {
                        "event": event_name,
                        "args": dict(decoded["args"]),
                    }
                except Exception as e:
                    logging.debug(f"Failed to decode event {event_name}: {e}")
                    topics = {}
            else:
                topics = {}
        else:
            topics = {}

        transactions.append(create_transaction_dict(log, provider, topics))

    return transactions


def get_all_unique_pairs_from_db():
    conn = DB_POOL.get_connection()
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

        # Group logs by contract address
        logs_by_address = {}
        for log in logs:
            addr = log["address"]
            if addr not in logs_by_address:
                logs_by_address[addr] = []
            logs_by_address[addr].append(log)

        # Decode logs grouped by address
        transactions = []
        for contract_address, contract_logs in logs_by_address.items():
            decoded_logs = decode_logs_for_contract(
                contract_address, contract_logs, provider
            )
            transactions.extend(decoded_logs)

        logging.info(
            f"[{worker_id}] [{provider_name}] Fetched {len(transactions)} events from blocks [{start_block:,} - {end_block:,}]"
        )

        return transactions

    except HTTPError as e:
        if e.response.status_code == 413:
            # Response too large - need to split the range
            logging.warning(
                f"[{worker_id}] [{provider_name}] Response too large (413) for range [{start_block:,} - {end_block:,}] - will split"
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
    conn = DB_POOL.get_connection()

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
            blocks_data.append(
                (
                    block_num,
                    datetime.fromtimestamp(block["timestamp"]),
                    block["hash"].hex(),
                )
            )
        except Exception as e:
            logging.warning(f"[{worker_id}] Failed to fetch block {block_num}: {e}")

    if blocks_data:
        batch_insert_block_metadata(blocks_data)
        logging.debug(f"[{worker_id}] Stored metadata for {len(blocks_data)} blocks")

    return len(blocks_data)


def process_block_range(start_block, end_block, addresses, worker_id="main"):
    if (start_block, end_block) in get_completed_ranges():
        logging.debug(
            f"[{worker_id}] Skipping already processed range [{start_block:,}, {end_block:,}]"
        )
        return 0

    mark_range_processing(start_block, end_block, worker_id)

    try:
        events = fetch_logs_for_range(start_block, end_block, addresses, worker_id)
        batch_insert_events(events, worker_id)
        mark_range_completed(start_block, end_block, worker_id)
        logging.debug(
            f"[{worker_id}] ✓ Processed [{start_block:,}, {end_block:,}] - {len(events)} events"
        )
        return len(events)

    except (Web3RPCError, HTTPError) as e:
        if isinstance(e, Web3RPCError):
            error_msg = (
                e.args[0].get("message", str(e))
                if e.args and isinstance(e.args[0], dict)
                else str(e)
            )
        else:
            error_msg = str(e)

        if (
            "more than 10000 results" in error_msg
            or "-32005" in error_msg
            or "Response payload too large" in error_msg
            or (hasattr(e, "response") and e.response.status_code == 413)
        ):

            mid = (start_block + end_block) // 2
            if mid == start_block:
                logging.error(
                    f"[{worker_id}] Cannot split range [{start_block:,}, {end_block:,}] further - skipping"
                )
                mark_range_completed(start_block, end_block, worker_id)
                return 0

            logging.info(
                f"[{worker_id}] Splitting [{start_block:,}, {end_block:,}] at {mid:,} (reason: {error_msg})"
            )
            count1 = process_block_range(start_block, mid, addresses, worker_id)
            count2 = process_block_range(mid + 1, end_block, addresses, worker_id)
            return count1 + count2
        else:
            logging.error(
                f"[{worker_id}] Failed to process [{start_block:,}, {end_block:,}]: {error_msg}"
            )
            return 0

    except Exception as e:
        logging.error(
            f"[{worker_id}] Unexpected error [{start_block:,}, {end_block:,}]: {e}"
        )
        return 0


def generate_block_ranges(start_block, end_block, chunk_size):
    completed = get_completed_ranges()

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
):

    all_addresses = generate_v3_pool_list()
    if token_filter:
        filter_checksummed = [Web3.to_checksum_address(addr) for addr in token_filter]
        addresses = [addr for addr in all_addresses if addr in filter_checksummed]
        logging.info(f"Filtered: {len(addresses)}/{len(all_addresses)} addresses")
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


def scan_blockchain_to_duckdb(
    start_block=FACTORY_DEPLOYMENT_BLOCK,
    end_block=20000000,
    chunk_size=10000,
    max_workers=3,
    token_filter=None,
):
    logging.info("=" * 60)
    logging.info("BLOCKCHAIN SCANNER")
    logging.info("=" * 60)

    # Generate/load V3 pool list instead of reading JSON
    all_addresses = generate_v3_pool_list()

    if token_filter:
        filter_checksummed = [Web3.to_checksum_address(addr) for addr in token_filter]
        addresses = [addr for addr in all_addresses if addr in filter_checksummed]
        logging.info(f"Filtered: {len(addresses)}/{len(all_addresses)} addresses")
    else:
        addresses = all_addresses

    logging.info(f"Total addresses: {len(addresses)}")
    stats = get_database_stats()
    logging.info(
        f"Blocks: {start_block:,} → {end_block:,} | Chunk: {chunk_size:,} | Workers: {max_workers}"
    )
    logging.info(
        f"DB: {stats['total_transfers']:,} transfers, {stats['total_swaps']:,} swaps, {stats['completed_ranges']} ranges done"
    )
    logging.info("=" * 60)

    try:
        scan_blockchain(addresses, start_block, end_block, chunk_size, max_workers)
        final_stats = get_database_stats()
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


def collect_metadata_for_scanned_events(max_workers=4):
    conn = DB_POOL.get_connection()

    unique_blocks = conn.execute(
        """
        SELECT DISTINCT block_number FROM (
            SELECT DISTINCT block_number FROM transfer UNION
            SELECT DISTINCT block_number FROM swap UNION
            SELECT DISTINCT block_number FROM mint UNION
            SELECT DISTINCT block_number FROM burn UNION
            SELECT DISTINCT block_number FROM collect UNION
            SELECT DISTINCT block_number FROM flash
        ) ORDER BY block_number
    """
    ).fetchall()
    unique_blocks = [b[0] for b in unique_blocks]

    existing_blocks = conn.execute("SELECT block_number FROM block_metadata").fetchall()
    existing_blocks = {b[0] for b in existing_blocks}

    missing_blocks = [b for b in unique_blocks if b not in existing_blocks]

    if not missing_blocks:
        logging.info("✓ All event blocks already have metadata")
        return

    logging.info(f"Fetching metadata for {len(missing_blocks):,} blocks with events...")
    fetch_and_store_block_metadata(missing_blocks, max_workers=max_workers)
    logging.info("✓ Block metadata collection complete")


def scan_all_pairs_in_batches(
    event_file=V3_EVENT_BY_CONTRACTS,
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

    final_stats = get_database_stats()
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
    collect_missing_pair_metadata()

    logging.info("Normalizing values...")
    normalize_missing_pairs()

    logging.info("✓ Complete")


def query_database():
    conn = DB_POOL.get_connection()

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


def get_pair_info(pair_address):
    pair_address = Web3.to_checksum_address(pair_address)
    conn = DB_POOL.get_connection()

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


# Find the token_filter around line 1040, change to just a few pools:
token_filter = [
    "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",  # USDC/WETH
    "0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11",  # DAI/WETH
]

START_BLOCK = 12369621  # V3 Factory deployment
END_BLOCK = 12370000  # Start with just 130k blocks to test
CHUNK_SIZE = 100
MAX_WORKERS = 2

try:
    logging.info("=" * 80)
    logging.info("UNISWAP V3 PIPELINE")
    logging.info("=" * 80)
    logging.info(f"Blocks: {START_BLOCK:,} → {END_BLOCK:,} (current)")
    logging.info(f"Config: chunk={CHUNK_SIZE:,} | workers={MAX_WORKERS}")
    stats = get_database_stats()
    logging.info(
        f"DB: {stats['total_transfers']:,} transfers | {stats['total_swaps']:,} swaps | {stats['completed_ranges']} ranges done"
    )
    logging.info("=" * 80)

    scan_blockchain_to_duckdb(
        start_block=START_BLOCK,
        end_block=END_BLOCK,
        chunk_size=CHUNK_SIZE,
        max_workers=MAX_WORKERS,
        token_filter=token_filter,
    )

    logging.info("\nPost-processing...")
    collect_metadata_for_scanned_events(max_workers=8)
    collect_missing_pair_metadata(batch_size=50, max_workers=4)
    normalize_missing_pairs(max_workers=4)

    final = get_database_stats()
    logging.info("=" * 80)
    logging.info("COMPLETE")
    logging.info(
        f"Transfers: {final['total_transfers']:,} | Swaps: {final['total_swaps']:,}"
    )
    logging.info(f"Mints: {final['total_mints']:,} | Burns: {final['total_burns']:,}")
    logging.info(f"Pairs: {final['total_pairs']:,} | Blocks: {final['total_blocks']:,}")
    logging.info("=" * 80)

except KeyboardInterrupt:
    logging.warning("\n" + "=" * 80)
    logging.warning("INTERRUPTED - Progress saved")
    stats = get_database_stats()
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
    if DB_POOL:
        DB_POOL.close_all()
print("✓ Main ready")
# =============================================================================
# MISSING HELPER FUNCTIONS - ADD THESE
# =============================================================================


def print_database_summary():
    stats = get_database_stats()

    print("\n" + "=" * 60)
    print("DATABASE SUMMARY")
    print("=" * 60)
    print(f"Events:")
    print(f"  Transfers:  {stats['total_transfers']:>12,}")
    print(f"  Swaps:      {stats['total_swaps']:>12,}")
    print(f"  Mints:      {stats['total_mints']:>12,}")
    print(f"  Burns:      {stats['total_burns']:>12,}")
    print(f"  Collects:   {stats['total_collects']:>12,}")
    print(f"  Flashes:    {stats['total_flashes']:>12,}")
    print(f"  Approvals:  {stats['total_approvals']:>12,}")
    total_events = (
        stats["total_transfers"]
        + stats["total_swaps"]
        + stats["total_mints"]
        + stats["total_burns"]
        + stats["total_collects"]
        + stats["total_flashes"]
        + stats["total_approvals"]
    )
    print(f"  TOTAL:      {total_events:>12,}")
    print(f"\nMetadata:")
    print(f"  Pairs:      {stats['total_pairs']:>12,}")
    print(f"  Blocks:     {stats['total_blocks']:>12,}")
    print(f"  Ranges:     {stats['completed_ranges']:>12,}")
    print("=" * 60 + "\n")


def collect_metadata_for_scanned_events(max_workers=8):
    conn = DB_POOL.get_connection()

    unique_blocks = conn.execute(
        """
        SELECT DISTINCT block_number FROM (
            SELECT DISTINCT block_number FROM transfer
            UNION SELECT DISTINCT block_number FROM swap
            UNION SELECT DISTINCT block_number FROM mint
            UNION SELECT DISTINCT block_number FROM burn
            UNION SELECT DISTINCT block_number FROM collect
            UNION SELECT DISTINCT block_number FROM flash
        ) ORDER BY block_number
    """
    ).fetchall()
    unique_blocks = [b[0] for b in unique_blocks]

    existing_blocks = conn.execute("SELECT block_number FROM block_metadata").fetchall()
    existing_blocks = {b[0] for b in existing_blocks}

    missing_blocks = [b for b in unique_blocks if b not in existing_blocks]

    if not missing_blocks:
        logging.info("✓ All event blocks already have metadata")
        return

    logging.info(f"Fetching metadata for {len(missing_blocks):,} blocks with events...")
    fetch_and_store_block_metadata(missing_blocks, max_workers=max_workers)
    logging.info("✓ Block metadata collection complete")


def run_full_pipeline(
    start_block=FACTORY_DEPLOYMENT_BLOCK,
    end_block=None,
    chunk_size=10000,
    max_workers=3,
    token_filter=None,
):
    """
    Complete Uniswap V3 data pipeline
    """

    try:
        logging.info("=" * 80)
        logging.info("UNISWAP V3 DATA PIPELINE")
        logging.info("=" * 80)

        provider, _ = PROVIDER_POOL.get_provider()
        if end_block is None:
            end_block = provider.eth.block_number
            logging.info(f"Using current block as end: {end_block:,}")

        logging.info(f"Block range: {start_block:,} → {end_block:,}")
        logging.info(f"Configuration: chunk={chunk_size:,}, workers={max_workers}")

        logging.info("\n" + "=" * 80)
        logging.info("STAGE 1: BLOCKCHAIN SCANNING")
        logging.info("=" * 80)

        scan_blockchain_to_duckdb(
            start_block=start_block,
            end_block=end_block,
            chunk_size=chunk_size,
            max_workers=max_workers,
            token_filter=token_filter,
        )

        print_database_summary()

        logging.info("\n" + "=" * 80)
        logging.info("STAGE 2: METADATA COLLECTION")
        logging.info("=" * 80)

        logging.info("\n📦 Collecting block metadata...")
        collect_metadata_for_scanned_events(max_workers=8)

        logging.info("\n📦 Collecting pair metadata...")
        collect_missing_pair_metadata(batch_size=50, max_workers=4)

        print_database_summary()

        logging.info("\n" + "=" * 80)
        logging.info("STAGE 3: VALUE NORMALIZATION")
        logging.info("=" * 80)

        normalize_missing_pairs(max_workers=8)

        print_database_summary()

        logging.info("\n" + "=" * 80)
        logging.info("STAGE 4: AGGREGATION & OPTIMIZATION")
        logging.info("=" * 80)

        logging.info("\n📊 Denormalizing timestamps...")
        denormalize_timestamps()

        logging.info("\n📊 Denormalizing pair symbols...")
        denormalize_pair_symbols()

        logging.info("\n📊 Refreshing pool summary...")
        refresh_pool_summary()

        logging.info("\n📊 Updating pool current state...")
        aggregate_all_pools(max_workers=8)

        logging.info("\n" + "=" * 80)
        logging.info("✅ PIPELINE COMPLETE")
        logging.info("=" * 80)

        print_database_summary()

    except KeyboardInterrupt:
        logging.warning("\n⚠️  INTERRUPTED - Progress saved")
        print_database_summary()

    except Exception as e:
        logging.error(f"\n❌ ERROR: {e}")
        logging.error(traceback.format_exc())
        raise

    finally:
        if TOKEN_CACHE:
            TOKEN_CACHE.flush()
        if DB_POOL:
            DB_POOL.close_all()


# =============================================================================
# MAIN EXECUTION - THIS IS WHAT RUNS WHEN YOU START THE SCRIPT
# =============================================================================

if __name__ == "__main__":

    # CONFIGURATION - EDIT THESE VALUES
    START_BLOCK = 12369621  # Uniswap V3 Factory deployment
    END_BLOCK = 12370000  # Small range for testing (change to None for latest)
    CHUNK_SIZE = 100  # Blocks per request
    MAX_WORKERS = 2  # Parallel workers

    # Optional: filter specific pools (None = scan all pools)
    TOKEN_FILTER = None
    # TOKEN_FILTER = [
    #     "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",  # USDC/WETH
    #     "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # USDC/WETH (another one)
    # ]

    # RUN THE PIPELINE
    run_full_pipeline(
        start_block=START_BLOCK,
        end_block=END_BLOCK,
        chunk_size=CHUNK_SIZE,
        max_workers=MAX_WORKERS,
        token_filter=TOKEN_FILTER,
    )

    logging.info("\n✅ All done! You can now query the database.")
