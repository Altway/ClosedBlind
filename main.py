#!/usr/bin/env python
# coding: utf-8

# In[15]:


# =============================================================================
# STANDARD LIBRARY IMPORTS
# =============================================================================
import json
import logging
import os
import tempfile
import requests_cache
import threading
import time
import traceback
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================================================================
# THIRD-PARTY IMPORTS
# =============================================================================
# Data manipulation and analysis
import pandas as pd

# Database
import duckdb

# Web requests and API calls
import requests
from requests.exceptions import HTTPError, ConnectionError

# Blockchain and Web3
from web3 import Web3
from web3.exceptions import Web3RPCError
from web3.providers.rpc.utils import (
    ExceptionRetryConfiguration,
    REQUEST_RETRY_ALLOWLIST,
)

# Visualization
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Environment and configuration
from dotenv import load_dotenv

# Utilities
import random

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================
# Load environment variables from .env file
load_dotenv()

# Configure pandas display options
pd.options.display.float_format = "{:20,.4f}".format

# =============================================================================
# CORE CONSTANTS AND CONFIGURATION
# =============================================================================

# Blockchain Constants
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
FACTORY_DEPLOYMENT_BLOCK = 12369600
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

# File Paths and Directories
BASE_OUTPUT_DIR = "out/V3"
LOG_DIR = "logs"
ABI_CACHE_FOLDER = "ABI"

# Data Files
STATE_FILE = f"{BASE_OUTPUT_DIR}/V3_final_scan_state.json"
TOKEN_NAME_FILE = f"{BASE_OUTPUT_DIR}/V3_token_name.json"
V3_EVENT_BY_CONTRACTS = f"{BASE_OUTPUT_DIR}/uniswap_v3_pairs_events.json"
DB_PATH = f"{BASE_OUTPUT_DIR}/uniswap_v3.duckdb"
V3_POOL_LIST_FILE = f"{BASE_OUTPUT_DIR}/uniswap_v3_pairs_events.json"


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
def setup_logging():
    """Configure logging for the notebook with both console and file output."""
    try:
        # Ensure log directory exists
        os.makedirs(LOG_DIR, exist_ok=True)

        # Generate log filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = os.path.join(LOG_DIR, f"uniswap_v3_pipeline_{timestamp}.log")

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[
                logging.StreamHandler(),  # Console output
                logging.FileHandler(log_filename),  # File output
            ],
            force=True,  # Override any existing configuration
        )

        logging.info(f"Logging initialized. Log file: {log_filename}")
        return log_filename

    except Exception as e:
        print(f"Warning: Could not setup file logging: {e}")
        # Fallback to console-only logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            force=True,
        )
        return None


# Initialize logging
LOG_FILENAME = setup_logging()


# =============================================================================
# ENVIRONMENT VARIABLES AND API CONFIGURATION
# =============================================================================
def validate_environment():
    """Validate that required environment variables are present."""
    required_vars = [
        "INFURA_URL_HEARTHQUAKE",
        "INFURA_URL_OPENSEE",
        "INFURA_URL_ECO",
        "ETHERSCAN_API_KEY",
    ]

    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}\n"
            f"Please check your .env file and ensure all required variables are set."
        )

    logging.info("✓ Environment variables validated")


# Validate environment before proceeding
validate_environment()

# API Configuration Dictionary
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

# Primary API key for Etherscan
ETHERSCAN_API_KEY = ETHERSCAN_API_KEY_DICT["hearthquake"]["ETHERSCAN_API_KEY"]


# =============================================================================
# DIRECTORY INITIALIZATION
# =============================================================================
def create_required_directories():
    """Create all required directories for the pipeline."""
    directories = [BASE_OUTPUT_DIR, LOG_DIR, ABI_CACHE_FOLDER]

    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            logging.debug(f"✓ Directory ensured: {directory}")
        except Exception as e:
            logging.error(f"Failed to create directory {directory}: {e}")
            raise


# Create required directories
create_required_directories()

logging.info("✓ Initialization cell completed successfully")
logging.info(f"✓ Base output directory: {BASE_OUTPUT_DIR}")
logging.info(f"✓ Database path: {DB_PATH}")
logging.info(f"✓ Log file: {LOG_FILENAME or 'Console only'}")


# In[16]:


# =============================================================================
# CELL 2 - UPDATED CORE INFRASTRUCTURE
# Fixed DuckDB connection pool without SQLAlchemy dependency issues
# =============================================================================


class ProviderPool:
    def __init__(self, api_key_dict):
        self.providers = []
        self.provider_names = []
        self.current_index = 0

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

    def get_provider(self):
        if not self.providers:
            raise Exception("No providers available")

        provider = self.providers[self.current_index]
        name = self.provider_names[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.providers)
        return provider, name


class DuckDBConnectionPool:
    """
    Simple DuckDB connection pool without SQLAlchemy dependencies
    Eliminates transaction rollback warnings and connection issues
    """

    def __init__(self, db_path, pool_size=5, max_overflow=10):
        self.db_path = db_path
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.connections = []
        self.in_use = set()
        self.lock = threading.RLock()
        self.created_connections = 0

        # Pre-create initial pool connections
        for _ in range(pool_size):
            self._create_connection()

        logging.debug(f"DuckDB connection pool initialized: {db_path}")

    def _create_connection(self):
        """Create a new DuckDB connection"""
        with self.lock:
            if self.created_connections < self.pool_size + self.max_overflow:
                conn = duckdb.connect(self.db_path)
                self.connections.append(conn)
                self.created_connections += 1
                return conn
            else:
                raise Exception("Connection pool exhausted")

    def get_connection(self):
        """Get a connection from the pool"""
        with self.lock:
            # Try to get an existing available connection
            for conn in self.connections:
                if conn not in self.in_use:
                    self.in_use.add(conn)
                    return conn

            # If no available connection and we can create more
            if self.created_connections < self.pool_size + self.max_overflow:
                conn = self._create_connection()
                self.in_use.add(conn)
                return conn

            # Wait for a connection to become available or raise exception
            raise Exception("No connections available in pool")

    def return_connection(self, conn):
        """Return a connection to the pool"""
        with self.lock:
            if conn in self.in_use:
                self.in_use.remove(conn)

    @contextmanager
    def connection(self):
        """Context manager for getting and returning connections"""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)

    def close_all(self):
        """Close all connections in the pool"""
        with self.lock:
            for conn in self.connections:
                try:
                    conn.close()
                except:
                    pass
            self.connections.clear()
            self.in_use.clear()
            self.created_connections = 0

        logging.debug("DuckDB connection pool closed")

    def size(self):
        """Get the current pool size"""
        with self.lock:
            return len(self.connections)


class ABINotVerified(Exception):
    pass


class ABIRateLimited(Exception):
    pass


class ABINetworkError(Exception):
    pass


class ABIFetchError(Exception):
    pass


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

    def set(self, token_address, token_info):
        with self.lock:
            self.cache[token_address] = token_info
            self.dirty = True
            if time.time() - self.last_save > self.save_interval:
                self._save()

    def _save(self):
        if self.dirty:
            try:
                os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
                with open(self.cache_file, "w") as f:
                    json.dump(self.cache, f, indent=2)
                self.dirty = False
                self.last_save = time.time()
            except Exception as e:
                logging.warning(f"Failed to save token cache: {e}")

    def flush(self):
        self._save()


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
        except (ABINotVerified, ABIRateLimited, ABINetworkError, ABIFetchError):
            return (None, None)


def setup_request_deduplication():
    requests_cache.install_cache(
        cache_name="temp_session_cache",
        backend="memory",
        expire_after=3600,
        allowable_codes=[200, 404],
        ignored_parameters=["apikey"],
    )
    logging.info("✓ HTTP request deduplication enabled")


class ABIIndexOptimizer:
    def __init__(self, db_pool, abi_folder="ABI"):
        self.db_pool = db_pool
        self.abi_folder = abi_folder
        self.stats = {"hits": 0, "misses": 0}
        self._setup_and_build_index()

    def _setup_and_build_index(self):
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS abi_file_index (
                    contract_address VARCHAR PRIMARY KEY,
                    file_path VARCHAR,
                    is_verified BOOLEAN,
                    file_size INTEGER,
                    last_indexed TIMESTAMP DEFAULT NOW()
                )
            """
            )
        self._rebuild_index()
        logging.info("✓ ABI index optimization ready")

    def _rebuild_index(self):
        if not os.path.exists(self.abi_folder):
            return

        indexed_files = []

        for filename in os.listdir(self.abi_folder):
            if filename.endswith(".json"):
                contract_address = filename[:-5]
                file_path = os.path.join(self.abi_folder, filename)

                try:
                    file_size = os.path.getsize(file_path)
                    with open(file_path, "r") as f:
                        content = json.load(f)
                        is_verified = content is not None and len(content) > 0
                    indexed_files.append(
                        (contract_address, file_path, is_verified, file_size)
                    )
                except:
                    indexed_files.append((contract_address, file_path, False, 0))

        with self.db_pool.connection() as conn:
            conn.execute("DELETE FROM abi_file_index")
            if indexed_files:
                conn.executemany(
                    """
                    INSERT INTO abi_file_index (contract_address, file_path, is_verified, file_size)
                    VALUES (?, ?, ?, ?)
                """,
                    indexed_files,
                )
        logging.info(f"✓ Indexed {len(indexed_files)} ABI files")

    def has_abi(self, contract_address):
        with self.db_pool.connection() as conn:
            result = conn.execute(
                """
                SELECT is_verified FROM abi_file_index WHERE contract_address = ?
                """,
                (contract_address,),
            ).fetchone()

        if result:
            self.stats["hits"] += 1
            return result[0]
        else:
            self.stats["misses"] += 1
            return None

    def add_to_index(self, contract_address, file_path, is_verified):
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO abi_file_index (contract_address, file_path, is_verified, file_size)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (contract_address) DO UPDATE SET
                    file_path = EXCLUDED.file_path,
                    is_verified = EXCLUDED.is_verified,
                    file_size = EXCLUDED.file_size,
                    last_indexed = NOW()
            """,
                (contract_address, file_path, is_verified, file_size),
            )


def setup_token_metadata_storage(db_pool):
    with db_pool.connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS token_metadata (
                token_address VARCHAR PRIMARY KEY,
                symbol VARCHAR,
                name VARCHAR,
                decimals INTEGER,
                is_valid BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT NOW(),
                last_updated TIMESTAMP DEFAULT NOW()
            )
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_token_metadata_symbol
            ON token_metadata(symbol)
        """
        )


def setup_block_range_tracking(db_pool):
    """
    Create table to track which block ranges have been analyzed for which addresses
    """
    with db_pool.connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS block_range_address_tracking (
                pair_address VARCHAR NOT NULL,
                start_block INTEGER NOT NULL,
                end_block INTEGER NOT NULL,
                status VARCHAR DEFAULT 'completed',
                events_found INTEGER DEFAULT 0,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                worker_id VARCHAR DEFAULT 'main',
                PRIMARY KEY(pair_address, start_block, end_block)
            )
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_block_range_tracking_address
            ON block_range_address_tracking(pair_address)
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_block_range_tracking_blocks
            ON block_range_address_tracking(start_block, end_block)
        """
        )


def setup_pools_storage(db_pool):
    """
    Create table to store uniswap_v3_pools.json data in database
    """
    with db_pool.connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS uniswap_v3_pools (
                pool_address VARCHAR PRIMARY KEY,
                token0 VARCHAR NOT NULL,
                token1 VARCHAR NOT NULL,
                token0_name VARCHAR,
                token1_name VARCHAR,
                token0_symbol VARCHAR,
                token1_symbol VARCHAR,
                fee INTEGER NOT NULL,
                tick_spacing INTEGER,
                created_block INTEGER NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pools_token0 ON uniswap_v3_pools(token0)
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pools_token1 ON uniswap_v3_pools(token1)
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pools_created_block ON uniswap_v3_pools(created_block)
        """
        )


def setup_abi_storage(db_pool):
    """
    Create table to store ABI data in database for faster access
    """
    with db_pool.connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contract_abis (
                contract_address VARCHAR PRIMARY KEY,
                abi_json TEXT NOT NULL,
                is_verified BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_abi_verified ON contract_abis(is_verified)
        """
        )


def store_abi_in_db(contract_address, abi_data, db_pool=None):
    """
    Store ABI in database for fast retrieval
    """
    if db_pool is None:
        db_pool = DB_POOL

    conn = db_pool.get_connection()
    try:
        import json
        import datetime

        current_time = datetime.datetime.now()

        abi_json = (
            json.dumps(abi_data)
            if isinstance(abi_data, (list, dict))
            else str(abi_data)
        )

        conn.execute(
            """INSERT INTO contract_abis
            (contract_address, abi_json, is_verified, created_at, last_updated)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (contract_address) DO UPDATE SET
                abi_json = EXCLUDED.abi_json,
                is_verified = EXCLUDED.is_verified,
                last_updated = EXCLUDED.last_updated""",
            (contract_address, abi_json, True, current_time, current_time),
        )
        return True
    except Exception as e:
        logging.warning(f"Failed to store ABI for {contract_address}: {e}")
        return False
    finally:
        db_pool.return_connection(conn)


def get_abi_from_db(contract_address, db_pool=None):
    """
    Retrieve ABI from database
    """
    if db_pool is None:
        db_pool = DB_POOL

    conn = db_pool.get_connection()
    try:
        result = conn.execute(
            """SELECT abi_json FROM contract_abis WHERE contract_address = ?""",
            (contract_address,),
        ).fetchone()

        if result:
            import json

            return json.loads(result[0])
        return None
    except Exception as e:
        logging.warning(f"Failed to get ABI for {contract_address}: {e}")
        return None
    finally:
        db_pool.return_connection(conn)


def migrate_existing_abis_to_db(abi_folder=ABI_CACHE_FOLDER, db_pool=None):
    """
    Migrate existing ABI files to database for faster access
    """
    if db_pool is None:
        db_pool = DB_POOL

    if not os.path.exists(abi_folder):
        logging.info("No existing ABI folder found - skipping migration")
        return

    abi_files = [f for f in os.listdir(abi_folder) if f.endswith(".json")]

    if not abi_files:
        logging.info("No ABI files found - skipping migration")
        return

    logging.info(f"🚀 Migrating {len(abi_files)} existing ABI files to database...")

    migrated = 0
    skipped = 0

    for filename in abi_files:
        contract_address = filename.replace(".json", "")

        # Check if already in database
        if get_abi_from_db(contract_address):
            skipped += 1
            continue

        # Load from file and store in database
        try:
            filepath = os.path.join(abi_folder, filename)
            with open(filepath, "r") as f:
                abi_data = json.load(f)

            if abi_data and abi_data != []:
                if store_abi_in_db(contract_address, abi_data):
                    migrated += 1
                else:
                    skipped += 1
            else:
                skipped += 1

        except Exception as e:
            logging.warning(f"Failed to migrate ABI for {contract_address}: {e}")
            skipped += 1

    logging.info(f"✓ ABI migration complete: {migrated} migrated, {skipped} skipped")


def store_pools_in_db(pools_dict, db_pool=None, batch_mode=False):
    """
    Store pools dictionary in database
    batch_mode: If True, reduces logging for incremental writes
    """
    if db_pool is None:
        db_pool = DB_POOL

    if not batch_mode:
        logging.info(f"🗃️ Starting to store {len(pools_dict)} pools in database...")
    else:
        logging.debug(f"🗃️ Batch storing {len(pools_dict)} pools in database...")

    conn = db_pool.get_connection()
    try:
        pools_data = []
        for pool_addr, pool_info in pools_dict.items():
            pools_data.append(
                (
                    pool_addr,
                    pool_info.get("token0"),
                    pool_info.get("token1"),
                    pool_info.get("token0_name"),
                    pool_info.get("token1_name"),
                    pool_info.get("token0_symbol"),
                    pool_info.get("token1_symbol"),
                    pool_info.get("fee"),
                    pool_info.get("tickSpacing"),
                    pool_info.get("created_block"),
                )
            )

        import datetime

        current_time = datetime.datetime.now()

        # Process in batches to avoid memory issues with large datasets
        batch_size = 1000
        total_processed = 0

        for i in range(0, len(pools_data), batch_size):
            batch = pools_data[i : i + batch_size]
            logging.info(
                f"📦 Processing batch {i//batch_size + 1}/{(len(pools_data) + batch_size - 1)//batch_size} ({len(batch)} pools)"
            )

            pools_data_with_time = []
            for pool_data in batch:
                pools_data_with_time.append(pool_data + (current_time,))

            conn.executemany(
                """INSERT INTO uniswap_v3_pools
                (pool_address, token0, token1, token0_name, token1_name, token0_symbol, token1_symbol, fee, tick_spacing, created_block, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (pool_address) DO UPDATE SET
                    token0 = EXCLUDED.token0,
                    token1 = EXCLUDED.token1,
                    token0_name = EXCLUDED.token0_name,
                    token1_name = EXCLUDED.token1_name,
                    token0_symbol = EXCLUDED.token0_symbol,
                    token1_symbol = EXCLUDED.token1_symbol,
                    fee = EXCLUDED.fee,
                    tick_spacing = EXCLUDED.tick_spacing,
                    created_block = EXCLUDED.created_block,
                    last_updated = EXCLUDED.last_updated""",
                pools_data_with_time,
            )
            total_processed += len(batch)

        # Explicit commit to ensure data is actually written
        conn.commit()
        logging.info(f"✓ Stored {total_processed} pools in database (committed)")
        return total_processed

    finally:
        db_pool.return_connection(conn)


def load_pools_from_db(db_pool=None):
    """
    Load pools from database back to dictionary format
    """
    if db_pool is None:
        db_pool = DB_POOL

    conn = db_pool.get_connection()
    try:
        rows = conn.execute(
            """SELECT pool_address, token0, token1, fee, tick_spacing, created_block,
                      token0_symbol, token1_symbol
               FROM uniswap_v3_pools
               ORDER BY created_block"""
        ).fetchall()

        pools_dict = {}
        for row in rows:
            (
                pool_addr,
                token0,
                token1,
                fee,
                tick_spacing,
                created_block,
                token0_symbol,
                token1_symbol,
            ) = row
            pools_dict[pool_addr] = {
                "token0": token0,
                "token1": token1,
                "fee": fee,
                "tickSpacing": tick_spacing,
                "created_block": created_block,
                "token0_symbol": token0_symbol,
                "token1_symbol": token1_symbol,
            }

        logging.info(f"✓ Loaded {len(pools_dict)} pools from database")
        return pools_dict

    finally:
        db_pool.return_connection(conn)


def get_token_metadata_optimized(token_address, provider=None, db_pool=None):
    if db_pool is None:
        db_pool = globals().get("DB_POOL")
    if db_pool is None:
        raise ValueError("db_pool parameter required or DB_POOL must be initialized")

    token_address = Web3.to_checksum_address(token_address)

    with db_pool.connection() as conn:
        result = conn.execute(
            """
            SELECT symbol, name, decimals FROM token_metadata
            WHERE token_address = ? AND is_valid = true AND decimals IS NOT NULL
        """,
            (token_address,),
        ).fetchone()

        if result:
            return {
                "address": token_address,
                "symbol": result[0],
                "name": result[1],
                "decimals": result[2],
                "cached": True,
            }

    if not provider:
        provider, _ = PROVIDER_POOL.get_provider()

    try:
        contract = provider.eth.contract(address=token_address, abi=MINIMAL_ERC20_ABI)
        symbol = contract.functions.symbol().call()
        name = contract.functions.name().call()
        decimals = contract.functions.decimals().call()

        with db_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO token_metadata (token_address, symbol, name, decimals, is_valid)
                VALUES (?, ?, ?, ?, true)
                ON CONFLICT (token_address) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    decimals = EXCLUDED.decimals,
                    is_valid = true,
                    last_updated = NOW()
            """,
                (token_address, symbol, name, decimals),
            )

        return {
            "address": token_address,
            "symbol": symbol,
            "name": name,
            "decimals": decimals,
            "cached": False,
        }

    except Exception as e:
        with db_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO token_metadata (token_address, is_valid)
                VALUES (?, false)
                ON CONFLICT (token_address) DO UPDATE SET
                    is_valid = false, last_updated = NOW()
            """,
                (token_address,),
            )
        return None


def test_optimizations():
    print("=" * 60)
    print("TESTING CACHE OPTIMIZATIONS")
    print("=" * 60)

    if ABI_OPTIMIZER:
        print("✓ ABI_OPTIMIZER initialized")
        test_contracts = [
            "0xA0b86a33E6441b42B38ac693D6af30A5A4beE9b7",
            "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        ]

        for contract in test_contracts:
            cached_status = ABI_OPTIMIZER.has_abi(contract)
            if cached_status is not None:
                print(
                    f"  ✓ {contract[:10]} cached: {'verified' if cached_status else 'unverified'}"
                )
            else:
                print(f"  - {contract[:10]} not cached")

        print(
            f"  ABI index stats: {ABI_OPTIMIZER.stats['hits']} hits, {ABI_OPTIMIZER.stats['misses']} misses"
        )
    else:
        print("❌ ABI_OPTIMIZER not initialized")

    try:
        cache = requests_cache.get_cache()
        if cache:
            print("✓ HTTP request deduplication active")
        else:
            print("❌ HTTP request deduplication not active")
    except:
        print("? HTTP request deduplication status unknown")

    try:
        db_pool = globals().get("DB_POOL")
        if db_pool:
            with db_pool.connection() as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM token_metadata_permanent"
                ).fetchone()[0]
                print(f"✓ Token metadata storage: {count:,} tokens cached")
        else:
            print("- Token metadata storage: DB_POOL not initialized yet")
    except Exception as e:
        print(f"❌ Token metadata storage error: {e}")

    print("=" * 60)


def print_optimization_stats():
    try:
        db_pool = globals().get("DB_POOL")
        if not db_pool:
            print("DB_POOL not initialized yet - cannot show optimization stats")
            return

        with db_pool.connection() as conn:
            abi_cached = conn.execute(
                "SELECT COUNT(*) FROM abi_file_index WHERE is_verified = true"
            ).fetchone()[0]
            abi_unverified = conn.execute(
                "SELECT COUNT(*) FROM abi_file_index WHERE is_verified = false"
            ).fetchone()[0]

            tokens_cached = conn.execute(
                "SELECT COUNT(*) FROM token_metadata WHERE is_valid = true"
            ).fetchone()[0]
            tokens_failed = conn.execute(
                "SELECT COUNT(*) FROM token_metadata WHERE is_valid = false"
            ).fetchone()[0]

        try:
            cache = requests_cache.get_cache()
            http_cached = len(list(cache.urls)) if hasattr(cache, "urls") else 0
        except:
            http_cached = 0

        print("=" * 60)
        print("OPTIMIZATION STATISTICS")
        print("=" * 60)
        print(f"ABI Index:")
        print(f"  Verified ABIs cached: {abi_cached:,}")
        print(f"  Unverified contracts: {abi_unverified:,}")
        if hasattr(globals().get("ABI_OPTIMIZER"), "stats"):
            print(f"  Index hits: {ABI_OPTIMIZER.stats['hits']:,}")
            print(f"  Index misses: {ABI_OPTIMIZER.stats['misses']:,}")
            if ABI_OPTIMIZER.stats["hits"] + ABI_OPTIMIZER.stats["misses"] > 0:
                hit_rate = (
                    ABI_OPTIMIZER.stats["hits"]
                    / (ABI_OPTIMIZER.stats["hits"] + ABI_OPTIMIZER.stats["misses"])
                    * 100
                )
                print(f"  Hit rate: {hit_rate:.1f}%")

        print(f"\nToken Metadata:")
        print(f"  Tokens cached: {tokens_cached:,}")
        print(f"  Failed tokens: {tokens_failed:,}")

        print(f"\nHTTP Deduplication:")
        print(f"  Requests cached: {http_cached:,}")
        print("=" * 60)
    except Exception as e:
        print(f"Error showing optimization stats: {e}")


# Initialize all components
setup_request_deduplication()

TOKEN_CACHE = TokenCache()
ABI_CACHE = ABICache()
PROVIDER_POOL = ProviderPool(ETHERSCAN_API_KEY_DICT)
ABI_OPTIMIZER = None

w3, _ = PROVIDER_POOL.get_provider()
assert w3.is_connected(), "Web3 provider connection failed"
print(f"✓ Connected to Ethereum. Latest block: {w3.eth.block_number:,}")


# In[17]:


def get_abi(contract_address, api_key=ETHERSCAN_API_KEY, abi_folder=ABI_CACHE_FOLDER):
    # Try database first for speed
    abi_data = get_abi_from_db(contract_address)
    if abi_data:
        return abi_data

    os.makedirs(abi_folder, exist_ok=True)
    filename = os.path.join(abi_folder, f"{contract_address}.json")

    if "ABI_OPTIMIZER" in globals() and ABI_OPTIMIZER:
        cached_status = ABI_OPTIMIZER.has_abi(contract_address)
        if cached_status is True:
            with open(filename, "r") as f:
                abi_data = json.load(f)
                # Store in database for future fast access
                store_abi_in_db(contract_address, abi_data)
                return abi_data
        elif cached_status is False:
            raise ABINotVerified(f"Contract {contract_address} not verified (cached)")

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

            if "ABI_OPTIMIZER" in globals() and ABI_OPTIMIZER:
                ABI_OPTIMIZER.add_to_index(contract_address, filename, True)

            # Store in database for future fast access
            store_abi_in_db(contract_address, abi)

            return abi
        else:
            error_msg = data.get("result", "Unknown error")
            if "not verified" in error_msg.lower():
                with open(filename, "w") as f:
                    json.dump(None, f)

                if "ABI_OPTIMIZER" in globals() and ABI_OPTIMIZER:
                    ABI_OPTIMIZER.add_to_index(contract_address, filename, False)

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
ABI_HASH_CACHE = {}


def get_abi_hash(abi):
    return hash(json.dumps(abi, sort_keys=True))


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
                except Exception:
                    topics = {}
            else:
                topics = {}
        else:
            topics = {}

        transactions.append(create_transaction_dict(log, provider, topics))

    return transactions


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

        abi_hash = get_abi_hash(abi)

        if abi_hash in ABI_HASH_CACHE:
            event_map = ABI_HASH_CACHE[abi_hash]
            EVENT_SIGNATURE_CACHE[contract_address] = event_map
            return event_map

        event_map = build_event_signature_map(abi)
        EVENT_SIGNATURE_CACHE[contract_address] = event_map
        ABI_HASH_CACHE[abi_hash] = event_map
        return event_map


# In[18]:


# =============================================================================
# CELL 4 - FIXED DATABASE OPERATIONS AND SETUP
# Fixed to work with updated DuckDBConnectionPool
# =============================================================================


def setup_database(db_path=DB_PATH, schema_path="./out/V3/database/schema_optimized.sql"):
    conn = duckdb.connect(db_path)

    try:
        result = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'swap'").fetchone()
        if result[0] > 0:
            logging.info("✓ Database tables already exist, skipping schema creation")
        else:
            with open(schema_path, "r") as f:
                schema_sql = f.read()
            conn.execute(schema_sql)
            logging.info("✓ Database schema created successfully")
    except Exception as e:
        with open(schema_path, "r") as f:
            schema_sql = f.read()
        conn.execute(schema_sql)
        logging.info("✓ Database schema loaded successfully")

    conn.close()
    return DuckDBConnectionPool(db_path)


DB_POOL = setup_database()


def is_range_processed(start_block, end_block):
    conn = DB_POOL.get_connection()
    try:
        result = conn.execute(
            "SELECT status FROM processing_state WHERE start_block = ? AND end_block = ?",
            (start_block, end_block),
        ).fetchone()
        return result and result[0] == "completed"
    finally:
        DB_POOL.return_connection(conn)


def get_completed_ranges():
    conn = DB_POOL.get_connection()
    try:
        result = conn.execute(
            "SELECT start_block, end_block FROM processing_state WHERE status = 'completed'"
        ).fetchall()
        return set((r[0], r[1]) for r in result)
    finally:
        DB_POOL.return_connection(conn)


def mark_range_processing(start_block, end_block, worker_id="main"):
    conn = DB_POOL.get_connection()
    try:
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
    finally:
        DB_POOL.return_connection(conn)


def mark_range_completed(start_block, end_block, worker_id="main"):
    conn = DB_POOL.get_connection()
    try:
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
    finally:
        DB_POOL.return_connection(conn)


def get_pair_metadata_cached(pair_address):
    conn = DB_POOL.get_connection()
    try:
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
    finally:
        DB_POOL.return_connection(conn)


def is_block_metadata_cached(block_number):
    conn = DB_POOL.get_connection()
    try:
        result = conn.execute(
            "SELECT 1 FROM block_metadata WHERE block_number = ?", (block_number,)
        ).fetchone()
        return result is not None
    finally:
        DB_POOL.return_connection(conn)


def is_block_range_analyzed_for_addresses(start_block, end_block, addresses):
    """
    OPTIMIZED: Check if this exact block range has been analyzed for these specific addresses using bulk query
    Returns dict with addresses that have been analyzed
    """
    conn = DB_POOL.get_connection()
    try:
        if not addresses:
            return {}

        # Create temp table with addresses (FAST bulk operation)
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_check_addresses (address VARCHAR)")
        conn.execute("DELETE FROM temp_check_addresses")
        conn.executemany("INSERT INTO temp_check_addresses VALUES (?)", [(addr,) for addr in addresses])

        # Single bulk query instead of N queries
        result = conn.execute(
            """SELECT pair_address, events_found, processed_at
            FROM block_range_address_tracking
            WHERE start_block = ? AND end_block = ? AND status = 'completed'
            AND pair_address IN (SELECT address FROM temp_check_addresses)""",
            (start_block, end_block),
        ).fetchall()

        # Build result dict
        analyzed_addresses = {
            row[0]: {
                "events_found": row[1],
                "processed_at": row[2],
            }
            for row in result
        }

        return analyzed_addresses
    finally:
        DB_POOL.return_connection(conn)


def mark_block_range_analyzed(
    start_block, end_block, address, events_found, worker_id="main"
):
    """
    Mark that this block range has been analyzed for this specific address
    """
    conn = DB_POOL.get_connection()
    try:
        import datetime

        current_time = datetime.datetime.now()

        conn.execute(
            """INSERT INTO block_range_address_tracking
            (pair_address, start_block, end_block, status, events_found, worker_id, processed_at)
            VALUES (?, ?, ?, 'completed', ?, ?, ?)
            ON CONFLICT (pair_address, start_block, end_block) DO UPDATE SET
                events_found = EXCLUDED.events_found,
                processed_at = EXCLUDED.processed_at,
                worker_id = EXCLUDED.worker_id,
                status = 'completed'""",
            (address, start_block, end_block, events_found, worker_id, current_time),
        )
    finally:
        DB_POOL.return_connection(conn)


def get_missing_block_ranges_for_addresses(
    start_block, end_block, chunk_size, addresses
):
    """
    OPTIMIZED: Find which block ranges are missing for which addresses using bulk query
    Returns list of (start_block, end_block, [missing_addresses]) tuples
    """
    conn = DB_POOL.get_connection()
    try:
        # Generate all expected ranges
        ranges = []
        for range_start in range(start_block, end_block + 1, chunk_size):
            range_end = min(range_start + chunk_size - 1, end_block)
            ranges.append((range_start, range_end))

        # Create temp table with all addresses (FAST bulk operation)
        conn.execute("CREATE TEMP TABLE temp_addresses (address VARCHAR)")
        conn.executemany("INSERT INTO temp_addresses VALUES (?)", [(addr,) for addr in addresses])

        missing_ranges = []

        # For each range, get completed addresses in ONE query
        for range_start, range_end in ranges:
            completed_addresses = set()

            try:
                result = conn.execute(
                    """SELECT pair_address FROM block_range_address_tracking
                    WHERE start_block = ? AND end_block = ? AND status = 'completed'
                    AND pair_address IN (SELECT address FROM temp_addresses)""",
                    (range_start, range_end),
                ).fetchall()

                completed_addresses = {row[0] for row in result}
            except:
                pass

            # Find missing addresses (set difference)
            all_addresses_set = set(addresses)
            missing_addresses = list(all_addresses_set - completed_addresses)

            if missing_addresses:
                missing_ranges.append((range_start, range_end, missing_addresses))

        # Cleanup
        conn.execute("DROP TABLE temp_addresses")

        return missing_ranges
    finally:
        DB_POOL.return_connection(conn)


def check_existing_events_for_range(start_block, end_block, addresses):
    """
    DEPRECATED: Use is_block_range_analyzed_for_addresses instead
    This function is kept for backward compatibility
    """
    return is_block_range_analyzed_for_addresses(start_block, end_block, addresses)


def calculate_price_from_sqrt(sqrt_price_x96):
    if sqrt_price_x96 is None or sqrt_price_x96 == 0:
        return None
    try:
        return (float(sqrt_price_x96) / (2 ** 96)) ** 2
    except:
        return None

def safe_normalize(raw_value, decimals):
    if raw_value is None or decimals is None:
        return 0.0
    try:
        return float(raw_value) / (10 ** decimals)
    except:
        return 0.0

def safe_str(value):
    return str(value) if value is not None else None

def _insert_one_by_one(conn, records, table_name, worker_id="main"):
    sql_templates = {
        "transfer": """INSERT INTO transfer
            (transaction_hash, log_index, block_number, pair_address, from_address, to_address, value_raw, value, abs_value, block_timestamp, token0_symbol, token1_symbol)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
        "swap": """INSERT INTO swap
            (transaction_hash, log_index, block_number, pair_address, sender, recipient, amount0_raw, amount1_raw, amount0, amount1, abs_amount0, abs_amount1, sqrt_price_x96, liquidity, tick, price_token0_token1, block_timestamp, token0_symbol, token1_symbol)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
        "mint": """INSERT INTO mint
            (transaction_hash, log_index, block_number, pair_address, owner, sender, tick_lower, tick_upper, amount_raw, amount0_raw, amount1_raw, amount, amount0, amount1, abs_amount0, abs_amount1, block_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
        "burn": """INSERT INTO burn
            (transaction_hash, log_index, block_number, pair_address, owner, tick_lower, tick_upper, amount_raw, amount0_raw, amount1_raw, amount, amount0, amount1, abs_amount0, abs_amount1, block_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
        "collect": """INSERT INTO collect
            (transaction_hash, log_index, block_number, pair_address, owner, recipient, tick_lower, tick_upper, amount0_raw, amount1_raw, amount0, amount1, abs_amount0, abs_amount1, block_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
        "flash": """INSERT INTO flash
            (transaction_hash, log_index, block_number, pair_address, sender, recipient, amount0_raw, amount1_raw, paid0_raw, paid1_raw, amount0, amount1, paid0, paid1, block_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
        "approval": """INSERT INTO approval
            (transaction_hash, log_index, block_number, pair_address, owner, spender, value, block_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
    }

    sql = sql_templates.get(table_name)
    if not sql:
        logging.error(f"[{worker_id}] Unknown table name: {table_name}")
        return 0

    success_count = 0
    failed_count = 0

    for record in records:
        try:
            conn.execute(sql, record)
            success_count += 1
        except Exception as e:
            if "Conversion Error" in str(e) or "out of range" in str(e):
                failed_count += 1
            else:
                logging.error(f"[{worker_id}] Unexpected error inserting {table_name} record: {e}")
                failed_count += 1

    if failed_count > 0:
        logging.warning(f"[{worker_id}] Skipped {failed_count} {table_name} records with overflow values, inserted {success_count}")

    return success_count


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

        base_data = (
            e["transactionHash"],
            e["logIndex"],
            e["blockNumber"],
            e["address"],
        )

        if event_type == "Transfer":
            value_raw = args.get("value")
            value_norm = safe_normalize(value_raw, 18)
            transfers.append(
                base_data
                + (
                    args.get("from"),
                    args.get("to"),
                    safe_str(value_raw),
                    value_norm,
                    abs(value_norm),
                    None,
                    None,
                    None,
                )
            )
        elif event_type == "Swap":
            amount0_raw = args.get("amount0")
            amount1_raw = args.get("amount1")
            sqrt_price = args.get("sqrtPriceX96")

            amount0_norm = safe_normalize(amount0_raw, 18)
            amount1_norm = safe_normalize(amount1_raw, 18)

            swaps.append(
                base_data
                + (
                    args.get("sender"),
                    args.get("recipient"),
                    safe_str(amount0_raw),
                    safe_str(amount1_raw),
                    amount0_norm,
                    amount1_norm,
                    abs(amount0_norm),
                    abs(amount1_norm),
                    safe_str(sqrt_price),
                    safe_str(args.get("liquidity")),
                    args.get("tick"),
                    calculate_price_from_sqrt(sqrt_price),
                    None,
                    None,
                    None,
                )
            )
        elif event_type == "Mint":
            amount_raw = args.get("amount")
            amount0_raw = args.get("amount0")
            amount1_raw = args.get("amount1")

            amount0_norm = safe_normalize(amount0_raw, 18)
            amount1_norm = safe_normalize(amount1_raw, 18)

            mints.append(
                base_data
                + (
                    args.get("owner"),
                    args.get("sender"),
                    args.get("tickLower"),
                    args.get("tickUpper"),
                    safe_str(amount_raw),
                    safe_str(amount0_raw),
                    safe_str(amount1_raw),
                    safe_normalize(amount_raw, 18),
                    amount0_norm,
                    amount1_norm,
                    abs(amount0_norm),
                    abs(amount1_norm),
                    None,
                )
            )
        elif event_type == "Burn":
            amount_raw = args.get("amount")
            amount0_raw = args.get("amount0")
            amount1_raw = args.get("amount1")

            amount0_norm = safe_normalize(amount0_raw, 18)
            amount1_norm = safe_normalize(amount1_raw, 18)

            burns.append(
                base_data
                + (
                    args.get("owner"),
                    args.get("tickLower"),
                    args.get("tickUpper"),
                    safe_str(amount_raw),
                    safe_str(amount0_raw),
                    safe_str(amount1_raw),
                    safe_normalize(amount_raw, 18),
                    amount0_norm,
                    amount1_norm,
                    abs(amount0_norm),
                    abs(amount1_norm),
                    None,
                )
            )
        elif event_type == "Collect":
            amount0_raw = args.get("amount0")
            amount1_raw = args.get("amount1")

            amount0_norm = safe_normalize(amount0_raw, 18)
            amount1_norm = safe_normalize(amount1_raw, 18)

            collects.append(
                base_data
                + (
                    args.get("owner"),
                    args.get("recipient"),
                    args.get("tickLower"),
                    args.get("tickUpper"),
                    safe_str(amount0_raw),
                    safe_str(amount1_raw),
                    amount0_norm,
                    amount1_norm,
                    abs(amount0_norm),
                    abs(amount1_norm),
                    None,
                )
            )
        elif event_type == "Flash":
            amount0_raw = args.get("amount0")
            amount1_raw = args.get("amount1")
            paid0_raw = args.get("paid0")
            paid1_raw = args.get("paid1")

            flashes.append(
                base_data
                + (
                    args.get("sender"),
                    args.get("recipient"),
                    safe_str(amount0_raw),
                    safe_str(amount1_raw),
                    safe_str(paid0_raw),
                    safe_str(paid1_raw),
                    safe_normalize(amount0_raw, 18),
                    safe_normalize(amount1_raw, 18),
                    safe_normalize(paid0_raw, 18),
                    safe_normalize(paid1_raw, 18),
                    None,
                )
            )
        elif event_type == "Approval":
            approvals.append(
                base_data
                + (args.get("owner"), args.get("spender"), args.get("value"), None)
            )

    conn = DB_POOL.get_connection()
    try:
        total_attempted = (
            len(transfers)
            + len(swaps)
            + len(mints)
            + len(burns)
            + len(collects)
            + len(flashes)
            + len(approvals)
        )

        successfully_inserted = 0

        if transfers:
            try:
                conn.executemany(
                    """INSERT INTO transfer
                    (transaction_hash, log_index, block_number, pair_address, from_address, to_address, value_raw, value, abs_value, block_timestamp, token0_symbol, token1_symbol)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
                    transfers,
                )
                successfully_inserted += len(transfers)
            except Exception as e:
                if "Conversion Error" in str(e) or "out of range" in str(e):
                    logging.warning(f"[{worker_id}] Batch transfer insert failed with conversion error, trying one-by-one")
                    successfully_inserted += _insert_one_by_one(conn, transfers, "transfer", worker_id)
                else:
                    raise

        if swaps:
            try:
                conn.executemany(
                    """INSERT INTO swap
                    (transaction_hash, log_index, block_number, pair_address, sender, recipient, amount0_raw, amount1_raw, amount0, amount1, abs_amount0, abs_amount1, sqrt_price_x96, liquidity, tick, price_token0_token1, block_timestamp, token0_symbol, token1_symbol)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
                    swaps,
                )
                successfully_inserted += len(swaps)
            except Exception as e:
                if "Conversion Error" in str(e) or "out of range" in str(e):
                    logging.warning(f"[{worker_id}] Batch swap insert failed with conversion error, trying one-by-one")
                    successfully_inserted += _insert_one_by_one(conn, swaps, "swap", worker_id)
                else:
                    raise

        if mints:
            try:
                conn.executemany(
                    """INSERT INTO mint
                    (transaction_hash, log_index, block_number, pair_address, owner, sender, tick_lower, tick_upper, amount_raw, amount0_raw, amount1_raw, amount, amount0, amount1, abs_amount0, abs_amount1, block_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
                    mints,
                )
                successfully_inserted += len(mints)
            except Exception as e:
                if "Conversion Error" in str(e) or "out of range" in str(e):
                    logging.warning(f"[{worker_id}] Batch mint insert failed with conversion error, trying one-by-one")
                    successfully_inserted += _insert_one_by_one(conn, mints, "mint", worker_id)
                else:
                    raise

        if burns:
            try:
                conn.executemany(
                    """INSERT INTO burn
                    (transaction_hash, log_index, block_number, pair_address, owner, tick_lower, tick_upper, amount_raw, amount0_raw, amount1_raw, amount, amount0, amount1, abs_amount0, abs_amount1, block_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
                    burns,
                )
                successfully_inserted += len(burns)
            except Exception as e:
                if "Conversion Error" in str(e) or "out of range" in str(e):
                    logging.warning(f"[{worker_id}] Batch burn insert failed with conversion error, trying one-by-one")
                    successfully_inserted += _insert_one_by_one(conn, burns, "burn", worker_id)
                else:
                    raise

        if collects:
            try:
                conn.executemany(
                    """INSERT INTO collect
                    (transaction_hash, log_index, block_number, pair_address, owner, recipient, tick_lower, tick_upper, amount0_raw, amount1_raw, amount0, amount1, abs_amount0, abs_amount1, block_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
                    collects,
                )
                successfully_inserted += len(collects)
            except Exception as e:
                if "Conversion Error" in str(e) or "out of range" in str(e):
                    logging.warning(f"[{worker_id}] Batch collect insert failed with conversion error, trying one-by-one")
                    successfully_inserted += _insert_one_by_one(conn, collects, "collect", worker_id)
                else:
                    raise

        if flashes:
            try:
                conn.executemany(
                    """INSERT INTO flash
                    (transaction_hash, log_index, block_number, pair_address, sender, recipient, amount0_raw, amount1_raw, paid0_raw, paid1_raw, amount0, amount1, paid0, paid1, block_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
                    flashes,
                )
                successfully_inserted += len(flashes)
            except Exception as e:
                if "Conversion Error" in str(e) or "out of range" in str(e):
                    logging.warning(f"[{worker_id}] Batch flash insert failed with conversion error, trying one-by-one")
                    successfully_inserted += _insert_one_by_one(conn, flashes, "flash", worker_id)
                else:
                    raise

        if approvals:
            try:
                conn.executemany(
                    """INSERT INTO approval
                    (transaction_hash, log_index, block_number, pair_address, owner, spender, value, block_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (transaction_hash, log_index) DO NOTHING""",
                    approvals,
                )
                successfully_inserted += len(approvals)
            except Exception as e:
                if "Conversion Error" in str(e) or "out of range" in str(e):
                    logging.warning(f"[{worker_id}] Batch approval insert failed with conversion error, trying one-by-one")
                    successfully_inserted += _insert_one_by_one(conn, approvals, "approval", worker_id)
                else:
                    raise

        conn.commit()

        if successfully_inserted > 0:
            try:
                total_swaps = conn.execute("SELECT COUNT(*) FROM swap").fetchone()[0]
                logging.debug(f"[{worker_id}] Database now has {total_swaps:,} total swaps")
            except:
                pass

        return successfully_inserted
    finally:
        DB_POOL.return_connection(conn)


def batch_insert_pair_metadata(pairs_data):
    if not pairs_data:
        return 0

    conn = DB_POOL.get_connection()
    try:
        conn.executemany(
            """INSERT INTO pair_metadata
            (pair_address, token0_address, token1_address, token0_symbol, token1_symbol, token0_decimals, token1_decimals, fee_tier, tick_spacing, created_block, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            ON CONFLICT (pair_address) DO UPDATE SET
                token0_address = EXCLUDED.token0_address,
                token1_address = EXCLUDED.token1_address,
                token0_symbol = EXCLUDED.token0_symbol,
                token1_symbol = EXCLUDED.token1_symbol,
                token0_decimals = EXCLUDED.token0_decimals,
                token1_decimals = EXCLUDED.token1_decimals,
                fee_tier = EXCLUDED.fee_tier,
                tick_spacing = EXCLUDED.tick_spacing,
                created_block = EXCLUDED.created_block,
                last_updated = NOW()""",
            pairs_data,
        )
        conn.commit()  # Ensure pair metadata is committed to database
        return len(pairs_data)
    finally:
        DB_POOL.return_connection(conn)


def batch_insert_block_metadata(blocks_data):
    if not blocks_data:
        return 0

    conn = DB_POOL.get_connection()
    try:
        conn.executemany(
            """INSERT INTO block_metadata (block_number, block_timestamp, block_hash)
            VALUES (?, ?, ?)
            ON CONFLICT (block_number) DO NOTHING""",
            blocks_data,
        )
        conn.commit()  # Ensure block metadata is committed to database
        return len(blocks_data)
    finally:
        DB_POOL.return_connection(conn)


def get_missing_block_metadata(block_numbers):
    if not block_numbers:
        return []

    conn = DB_POOL.get_connection()
    try:
        placeholders = ",".join(["?" for _ in block_numbers])
        existing = conn.execute(
            f"SELECT block_number FROM block_metadata WHERE block_number IN ({placeholders})",
            block_numbers,
        ).fetchall()
        existing_set = {b[0] for b in existing}
        return [b for b in block_numbers if b not in existing_set]
    finally:
        DB_POOL.return_connection(conn)


def get_pairs_missing_metadata():
    conn = DB_POOL.get_connection()
    try:
        all_pairs = conn.execute(
            """SELECT DISTINCT pair_address FROM (
                SELECT pair_address FROM transfer UNION
                SELECT pair_address FROM swap UNION
                SELECT pair_address FROM mint UNION
                SELECT pair_address FROM burn UNION
                SELECT pair_address FROM collect UNION
                SELECT pair_address FROM flash UNION
                SELECT pair_address FROM approval
            ) WHERE pair_address IS NOT NULL"""
        ).fetchall()
        all_pairs = {r[0] for r in all_pairs}

        cached_pairs = conn.execute(
            "SELECT pair_address FROM pair_metadata WHERE token0_decimals IS NOT NULL AND token1_decimals IS NOT NULL"
        ).fetchall()
        cached_pairs = {r[0] for r in cached_pairs}

        return list(all_pairs - cached_pairs)
    finally:
        DB_POOL.return_connection(conn)


def normalize_event_values(pair_address):
    metadata = get_pair_metadata_cached(pair_address)
    if (
        not metadata
        or metadata["token0_decimals"] is None
        or metadata["token1_decimals"] is None
    ):
        return False

    token0_decimals = metadata["token0_decimals"]
    token1_decimals = metadata["token1_decimals"]

    conn = DB_POOL.get_connection()
    try:
        conn.execute(
            """UPDATE transfer
            SET value_normalized = CAST(value AS DOUBLE) / POW(10, 18)
            WHERE pair_address = ? AND value_normalized IS NULL""",
            (pair_address,),
        )

        conn.execute(
            """UPDATE swap
            SET amount0_normalized = CAST(amount0 AS DOUBLE) / POW(10, ?),
                amount1_normalized = CAST(amount1 AS DOUBLE) / POW(10, ?)
            WHERE pair_address = ? AND amount0_normalized IS NULL""",
            (token0_decimals, token1_decimals, pair_address),
        )

        conn.execute(
            """UPDATE mint
            SET amount0_normalized = CAST(amount0 AS DOUBLE) / POW(10, ?),
                amount1_normalized = CAST(amount1 AS DOUBLE) / POW(10, ?)
            WHERE pair_address = ? AND amount0_normalized IS NULL""",
            (token0_decimals, token1_decimals, pair_address),
        )

        conn.execute(
            """UPDATE burn
            SET amount0_normalized = CAST(amount0 AS DOUBLE) / POW(10, ?),
                amount1_normalized = CAST(amount1 AS DOUBLE) / POW(10, ?)
            WHERE pair_address = ? AND amount0_normalized IS NULL""",
            (token0_decimals, token1_decimals, pair_address),
        )

        conn.execute(
            """UPDATE collect
            SET amount0_normalized = CAST(amount0 AS DOUBLE) / POW(10, ?),
                amount1_normalized = CAST(amount1 AS DOUBLE) / POW(10, ?)
            WHERE pair_address = ? AND amount0_normalized IS NULL""",
            (token0_decimals, token1_decimals, pair_address),
        )

        conn.commit()
        return True
    finally:
        DB_POOL.return_connection(conn)


def get_database_stats():
    conn = DB_POOL.get_connection()
    try:
        result = conn.execute(
            """SELECT
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
    finally:
        DB_POOL.return_connection(conn)


# Initialize optimizations with fixed connection pool
ABI_OPTIMIZER = ABIIndexOptimizer(DB_POOL, ABI_CACHE_FOLDER)
setup_token_metadata_storage(DB_POOL)
setup_block_range_tracking(DB_POOL)
setup_pools_storage(DB_POOL)
setup_abi_storage(DB_POOL)

# Migrate existing ABI files to database for faster access
migrate_existing_abis_to_db()

print("✓ Database setup and core operations loaded")
print(f"✓ Database initialized at: {DB_PATH}")
print(
    f"✓ Connection pool ready with {DB_POOL.size()} connections"
)  # FIXED: Use .size() instead of .pool.size()
print("✓ No more SQLAlchemy warnings!")

# Initialize pool data at startup
print("\n🦄 Initializing Uniswap V3 pool data...")
try:
    pool_addresses = generate_v3_pool_list(V3_EVENT_BY_CONTRACTS)
    print(f"✅ Initialized with {len(pool_addresses)} V3 pools (basic data)")
except Exception as e:
    print(f"❌ Pool initialization failed: {e}")
    pool_addresses = []


# In[19]:


# =============================================================================
# CELL 5 - MULTICALL OPTIMIZED VERSION
# Maximum performance with batched contract calls
# =============================================================================


def generate_block_ranges_for_addresses(start_block, end_block, chunk_size, addresses):
    """
    Generate block ranges that need to be processed for specific addresses
    Only returns ranges that have missing data for at least one address
    """
    missing_ranges = get_missing_block_ranges_for_addresses(
        start_block, end_block, chunk_size, addresses
    )
    logging.info(
        f"Found {len(missing_ranges)} ranges needing analysis for these addresses"
    )

    # Log summary
    total_missing_work = sum(
        len(missing_addrs) for _, _, missing_addrs in missing_ranges
    )
    if total_missing_work > 0:
        logging.info(
            f"Total address-range combinations to process: {total_missing_work}"
        )

    return missing_ranges


def generate_block_ranges(start_block, end_block, chunk_size):
    """
    Legacy function - generates ranges based on old completed_ranges system
    """
    completed_ranges = get_completed_ranges()
    ranges = []
    current = start_block
    while current <= end_block:
        range_end = min(current + chunk_size - 1, end_block)
        if (current, range_end) not in completed_ranges:
            ranges.append((current, range_end))
        current += chunk_size
    return ranges


def multicall_batch_contract_calls(calls, provider=None, max_retries=3):
    """
    Execute multiple contract calls in a single RPC request using Multicall3

    calls: List of (target_address, call_data) tuples
    Returns: List of (success, return_data) tuples
    """
    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    if not calls:
        return []

    # Get or create multicall contract
    multicall_contract = provider.eth.contract(
        address=MULTICALL3_ADDRESS, abi=MULTICALL3_ABI
    )

    for retry in range(max_retries):
        try:
            # Format calls for multicall
            multicall_calls = [
                {"target": target, "callData": call_data} for target, call_data in calls
            ]

            # Execute multicall
            block_number, return_data = multicall_contract.functions.aggregate(
                multicall_calls
            ).call()

            # Parse results
            results = []
            for i, data in enumerate(return_data):
                try:
                    results.append((True, data))
                except Exception as e:
                    logging.warning(f"Failed to decode multicall result {i}: {e}")
                    results.append((False, None))

            return results

        except Exception as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                if retry < max_retries - 1:
                    wait_time = (2**retry) + random.uniform(0.5, 1.5)
                    logging.warning(
                        f"Multicall rate limited, waiting {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    provider, _ = PROVIDER_POOL.get_provider()
                    continue

            logging.error(f"Multicall failed after {retry + 1} attempts: {e}")
            # Return failed results for all calls
            return [(False, None)] * len(calls)

    return [(False, None)] * len(calls)


def fetch_uniswap_pair_metadata_multicall(pair_addresses, provider=None):
    """
    Fetch metadata for multiple pairs using multicall for maximum efficiency
    """
    if not pair_addresses:
        return []

    # Check cache first and filter out already cached pairs
    cached_results = {}
    uncached_addresses = []

    for addr in pair_addresses:
        cached = get_pair_metadata_cached(addr)
        if cached and cached.get("token0_decimals") is not None:
            cached_results[addr] = cached
        else:
            uncached_addresses.append(addr)

    if not uncached_addresses:
        logging.info(f"✓ All {len(pair_addresses)} pairs found in cache")
        return [cached_results.get(addr) for addr in pair_addresses]

    logging.info(
        f"Fetching {len(uncached_addresses)} pairs via Multicall ({len(cached_results)} cached)"
    )

    if provider is None:
        provider, _ = PROVIDER_POOL.get_provider()

    results = {}
    results.update(cached_results)

    # Process uncached pairs in batches to avoid huge multicalls
    batch_size = 50  # Reasonable batch size for multicall

    for i in range(0, len(uncached_addresses), batch_size):
        batch_addresses = uncached_addresses[i : i + batch_size]
        batch_results = _fetch_pair_batch_multicall(batch_addresses, provider)
        results.update(batch_results)

    # Return results in original order
    return [results.get(addr) for addr in pair_addresses]


def _fetch_pair_batch_multicall(pair_addresses, provider):
    """
    Fetch a batch of pair metadata using multicall
    """
    # Prepare multicall data
    calls = []
    call_map = {}  # Maps call index to (pair_address, call_type)

    for pair_addr in pair_addresses:
        pair_addr = provider.to_checksum_address(pair_addr)

        # Create contract instances for encoding
        pair_contract = provider.eth.contract(
            address=pair_addr, abi=MINIMAL_UNISWAP_V3_POOL_ABI
        )

        # Add calls for each pair
        base_idx = len(calls)

        # Pool contract calls
        calls.append(
            (pair_addr, pair_contract.functions.token0().build_transaction()["data"])
        )
        call_map[base_idx] = (pair_addr, "token0")

        calls.append(
            (pair_addr, pair_contract.functions.token1().build_transaction()["data"])
        )
        call_map[base_idx + 1] = (pair_addr, "token1")

        calls.append(
            (pair_addr, pair_contract.functions.fee().build_transaction()["data"])
        )
        call_map[base_idx + 2] = (pair_addr, "fee")

        calls.append(
            (
                pair_addr,
                pair_contract.functions.tickSpacing().build_transaction()["data"],
            )
        )
        call_map[base_idx + 3] = (pair_addr, "tickSpacing")

    # Execute multicall
    multicall_results = multicall_batch_contract_calls(calls, provider)

    # Parse results into pair metadata
    pair_data = {}
    token_addresses = set()

    for i, (success, data) in enumerate(multicall_results):
        if not success or not data:
            continue

        pair_addr, call_type = call_map[i]

        if pair_addr not in pair_data:
            pair_data[pair_addr] = {"pair_address": pair_addr}

        try:
            if call_type == "token0":
                decoded = provider.eth.codec.decode(["address"], data)[0]
                pair_data[pair_addr]["token0_address"] = decoded
                token_addresses.add(decoded)
            elif call_type == "token1":
                decoded = provider.eth.codec.decode(["address"], data)[0]
                pair_data[pair_addr]["token1_address"] = decoded
                token_addresses.add(decoded)
            elif call_type == "fee":
                decoded = provider.eth.codec.decode(["uint24"], data)[0]
                pair_data[pair_addr]["fee_tier"] = decoded
            elif call_type == "tickSpacing":
                decoded = provider.eth.codec.decode(["int24"], data)[0]
                pair_data[pair_addr]["tick_spacing"] = decoded
        except Exception as e:
            logging.warning(f"Failed to decode {call_type} for {pair_addr[:10]}: {e}")

    # Now fetch token metadata for all unique tokens using multicall
    if token_addresses:
        token_metadata = _fetch_token_metadata_multicall(
            list(token_addresses), provider
        )
    else:
        token_metadata = {}

    # Combine pair and token data
    final_results = {}
    for pair_addr, data in pair_data.items():
        token0_addr = data.get("token0_address")
        token1_addr = data.get("token1_address")

        if token0_addr and token1_addr:
            token0_meta = token_metadata.get(token0_addr, {})
            token1_meta = token_metadata.get(token1_addr, {})

            final_results[pair_addr] = {
                **data,
                "token0_symbol": token0_meta.get("symbol"),
                "token0_decimals": token0_meta.get("decimals"),
                "token1_symbol": token1_meta.get("symbol"),
                "token1_decimals": token1_meta.get("decimals"),
            }

    return final_results


def _fetch_token_metadata_multicall(token_addresses, provider):
    """
    Fetch token metadata for multiple tokens using multicall
    """
    # Check cache first
    cached_results = {}
    uncached_addresses = []

    for addr in token_addresses:
        cached = get_token_metadata_optimized(addr, provider, DB_POOL)
        if cached:
            cached_results[addr] = cached
        else:
            uncached_addresses.append(addr)

    if not uncached_addresses:
        return cached_results

    # Prepare multicall for uncached tokens
    calls = []
    call_map = {}

    for token_addr in uncached_addresses:
        token_addr = provider.to_checksum_address(token_addr)
        token_contract = provider.eth.contract(
            address=token_addr, abi=MINIMAL_ERC20_ABI
        )

        base_idx = len(calls)

        calls.append(
            (token_addr, token_contract.functions.symbol().build_transaction()["data"])
        )
        call_map[base_idx] = (token_addr, "symbol")

        calls.append(
            (
                token_addr,
                token_contract.functions.decimals().build_transaction()["data"],
            )
        )
        call_map[base_idx + 1] = (token_addr, "decimals")

        calls.append(
            (token_addr, token_contract.functions.name().build_transaction()["data"])
        )
        call_map[base_idx + 2] = (token_addr, "name")

    # Execute multicall
    multicall_results = multicall_batch_contract_calls(calls, provider)

    # Parse token results
    token_data = {}
    for i, (success, data) in enumerate(multicall_results):
        if not success or not data:
            continue

        token_addr, call_type = call_map[i]

        if token_addr not in token_data:
            token_data[token_addr] = {"address": token_addr}

        try:
            if call_type == "symbol":
                decoded = provider.eth.codec.decode(["string"], data)[0]
                token_data[token_addr]["symbol"] = decoded
            elif call_type == "decimals":
                decoded = provider.eth.codec.decode(["uint8"], data)[0]
                token_data[token_addr]["decimals"] = decoded
            elif call_type == "name":
                decoded = provider.eth.codec.decode(["string"], data)[0]
                token_data[token_addr]["name"] = decoded
        except Exception as e:
            logging.warning(f"Failed to decode {call_type} for {token_addr[:10]}: {e}")

    # Store successful results in cache
    conn = DB_POOL.get_connection()
    try:
        for token_addr, metadata in token_data.items():
            if metadata.get("decimals") is not None:
                conn.execute(
                    """
                    INSERT INTO token_metadata (token_address, symbol, name, decimals, is_valid)
                    VALUES (?, ?, ?, ?, true)
                    ON CONFLICT (token_address) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        name = EXCLUDED.name,
                        decimals = EXCLUDED.decimals,
                        is_valid = true,
                        last_updated = NOW()
                """,
                    (
                        token_addr,
                        metadata.get("symbol"),
                        metadata.get("name"),
                        metadata.get("decimals"),
                    ),
                )
        conn.commit()
    finally:
        DB_POOL.return_connection(conn)

    # Combine cached and new results
    results = {}
    results.update(cached_results)
    results.update(token_data)

    return results


def fetch_uniswap_pair_metadata(pair_address, provider=None):
    """
    Single pair metadata fetch - optimized version that uses multicall for batches
    For single calls, falls back to individual calls with cache check
    """
    # Check cache first
    cached_metadata = get_pair_metadata_cached(pair_address)
    if cached_metadata and cached_metadata.get("token0_decimals") is not None:
        return cached_metadata

    # For single pair, use multicall batch of 1
    results = fetch_uniswap_pair_metadata_multicall([pair_address], provider)
    return results[0] if results else None


def fetch_block_metadata(block_number, provider=None, retry_count=0, max_retries=3):
    # Check cache first
    if is_block_metadata_cached(block_number):
        return None  # Already cached

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
                wait_time = (2**retry_count) + random.uniform(0.5, 1.5)
                logging.warning(
                    f"Rate limit (429) for block {block_number}, waiting {wait_time:.1f}s..."
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
            wait_time = (2**retry_count) + random.uniform(0.5, 1.5)
            logging.warning(
                f"Timeout for block {block_number}, retrying in {wait_time:.1f}s..."
            )
            time.sleep(wait_time)
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
            wait_time = (2**retry_count) + random.uniform(0.5, 1.5)
            logging.warning(
                f"Connection error for block {block_number}, retrying in {wait_time:.1f}s..."
            )
            time.sleep(wait_time)
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


def parallel_fetch_with_backoff(
    items, fetch_func, max_workers=4, desc="Processing", max_retries=3
):
    results = [None] * len(items)
    results_lock = threading.Lock()
    rate_limit_pause = threading.Event()

    def worker(idx, item, retry_count=0):
        rate_limit_pause.wait()

        provider, provider_name = PROVIDER_POOL.get_provider()
        try:
            result = fetch_func(item, provider)
            with results_lock:
                results[idx] = result
            return result
        except Exception as e:
            error_str = str(e).lower()
            if (
                "429" in error_str
                or "too many requests" in error_str
                or "rate limit" in error_str
            ):
                if retry_count < max_retries:
                    rate_limit_pause.clear()
                    wait_time = (2**retry_count) + random.uniform(1, 3)
                    logging.warning(
                        f"{desc} rate limited for item {idx}, pausing all workers for {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    rate_limit_pause.set()
                    return worker(idx, item, retry_count + 1)
                else:
                    logging.error(
                        f"{desc} failed for item {idx} after {max_retries} retries: {e}"
                    )
                    return None
            else:
                logging.warning(f"{desc} failed for item {idx}: {e}")
                return None

    rate_limit_pause.set()

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


def update_pool_current_state(pool_address):
    conn = DB_POOL.get_connection()
    try:
        latest_swap = conn.execute(
            """SELECT sqrt_price_x96, tick, liquidity, block_number
            FROM swap WHERE pair_address = ?
            ORDER BY block_number DESC, log_index DESC LIMIT 1""",
            (pool_address,),
        ).fetchone()

        if not latest_swap:
            return

        total_stats = conn.execute(
            """SELECT COUNT(*) as total_swaps,
            SUM(ABS(amount0_normalized)) as total_volume0,
            SUM(ABS(amount1_normalized)) as total_volume1
            FROM swap WHERE pair_address = ?""",
            (pool_address,),
        ).fetchone()

        conn.execute(
            """INSERT INTO pool_current_state (
                pool_address, current_sqrt_price_x96, current_tick, current_liquidity,
                last_swap_block, total_swaps, total_volume_token0, total_volume_token1, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())
            ON CONFLICT (pool_address) DO UPDATE SET
                current_sqrt_price_x96 = EXCLUDED.current_sqrt_price_x96,
                current_tick = EXCLUDED.current_tick,
                current_liquidity = EXCLUDED.current_liquidity,
                last_swap_block = EXCLUDED.last_swap_block,
                total_swaps = EXCLUDED.total_swaps,
                total_volume_token0 = EXCLUDED.total_volume_token0,
                total_volume_token1 = EXCLUDED.total_volume_token1,
                updated_at = NOW()""",
            (
                pool_address,
                str(latest_swap[0]) if latest_swap[0] is not None else None,
                latest_swap[1],
                str(latest_swap[2]) if latest_swap[2] is not None else None,
                latest_swap[3],
                total_stats[0],
                total_stats[1],
                total_stats[2],
            ),
        )
        conn.commit()
    finally:
        DB_POOL.return_connection(conn)


def denormalize_timestamps():
    conn = DB_POOL.get_connection()
    try:
        logging.info("Denormalizing timestamps...")

        conn.execute(
            """UPDATE swap s SET block_timestamp = b.block_timestamp
            FROM block_metadata b WHERE s.block_number = b.block_number AND s.block_timestamp IS NULL"""
        )

        conn.execute(
            """UPDATE mint m SET block_timestamp = b.block_timestamp
            FROM block_metadata b WHERE m.block_number = b.block_number AND m.block_timestamp IS NULL"""
        )

        conn.execute(
            """UPDATE burn bn SET block_timestamp = b.block_timestamp
            FROM block_metadata b WHERE bn.block_number = b.block_number AND bn.block_timestamp IS NULL"""
        )

        conn.commit()
        logging.info("✓ Timestamps denormalized")
    finally:
        DB_POOL.return_connection(conn)


def denormalize_pair_symbols():
    conn = DB_POOL.get_connection()
    try:
        logging.info("Denormalizing pair symbols...")

        conn.execute(
            """UPDATE swap s SET token0_symbol = pm.token0_symbol, token1_symbol = pm.token1_symbol
            FROM pair_metadata pm WHERE s.pair_address = pm.pair_address AND s.token0_symbol IS NULL"""
        )

        conn.commit()
        logging.info("✓ Pair symbols denormalized")
    finally:
        DB_POOL.return_connection(conn)


def refresh_pool_summary():
    conn = DB_POOL.get_connection()
    try:
        logging.info("Refreshing pool summary...")

        conn.execute("DROP TABLE IF EXISTS pool_summary")
        conn.execute(
            """CREATE TABLE pool_summary AS
            SELECT pm.pair_address, pm.token0_symbol, pm.token1_symbol, pm.token0_decimals, pm.token1_decimals, pm.fee_tier,
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
            GROUP BY pm.pair_address, pm.token0_symbol, pm.token1_symbol, pm.token0_decimals, pm.token1_decimals, pm.fee_tier"""
        )

        conn.execute(
            "CREATE INDEX idx_pool_summary_volume ON pool_summary(total_volume_token0)"
        )
        conn.execute("CREATE INDEX idx_pool_summary_swaps ON pool_summary(total_swaps)")
        conn.commit()
        logging.info("✓ Pool summary refreshed")
    finally:
        DB_POOL.return_connection(conn)


def aggregate_all_pools(max_workers=8):
    conn = DB_POOL.get_connection()
    try:
        pools = conn.execute("SELECT DISTINCT pair_address FROM swap").fetchall()
        pools = [p[0] for p in pools]
        logging.info(f"Aggregating stats for {len(pools)} pools...")
    finally:
        DB_POOL.return_connection(conn)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(update_pool_current_state, pool) for pool in pools]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Failed to aggregate pool: {e}")


def generate_v3_pool_list_with_symbols(
    output_file,
    start_block=FACTORY_DEPLOYMENT_BLOCK,
    max_workers=4,
    include_symbols=True,
):
    """
    Enhanced version that can optionally include token symbols in the pool data
    """
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
            if include_symbols:
                return _add_symbols_to_existing_pools_smart(pools_dict, output_file)
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
        if provider is None:
            provider, _ = PROVIDER_POOL.get_provider()

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
                    # Get token metadata for names and symbols
                    token0_meta = get_token_metadata_optimized(
                        log.args.token0, provider=None, db_pool=DB_POOL
                    )
                    token1_meta = get_token_metadata_optimized(
                        log.args.token1, provider=None, db_pool=DB_POOL
                    )

                    pool_data = {
                        "token0": log.args.token0,
                        "token1": log.args.token1,
                        "token0_name": (
                            token0_meta.get("name", "Unknown")
                            if token0_meta
                            else "Unknown"
                        ),
                        "token1_name": (
                            token1_meta.get("name", "Unknown")
                            if token1_meta
                            else "Unknown"
                        ),
                        "token0_symbol": (
                            token0_meta.get("symbol", "UNKNOWN")
                            if token0_meta
                            else "UNKNOWN"
                        ),
                        "token1_symbol": (
                            token1_meta.get("symbol", "UNKNOWN")
                            if token1_meta
                            else "UNKNOWN"
                        ),
                        "fee": log.args.fee,
                        "tickSpacing": log.args.tickSpacing,
                        "created_block": log.blockNumber,
                    }
                    pools[log.args.pool] = pool_data

                if pools:
                    logging.info(
                        f"[{from_block:,} - {to_block:,}] Found {len(pools)} pools with token names"
                    )
                return pools
            except Exception as e:
                error_str = str(e).lower()
                if "429" in error_str or "too many requests" in error_str:
                    wait = min(10 * (retry + 1), 60)
                    logging.warning(
                        f"Rate limit [{from_block:,}-{to_block:,}], retry in {wait}s"
                    )
                    time.sleep(wait)
                    provider, _ = PROVIDER_POOL.get_provider()
                    continue
                elif "402" in error_str or "payment required" in error_str:
                    logging.critical(f"💸 INFURA CREDITS EXHAUSTED - STOPPING IMMEDIATELY")
                    logging.critical(f"❌ All API credits have been consumed. Come back tomorrow.")
                    logging.critical(f"🛑 Range [{from_block:,}-{to_block:,}] failed due to payment required")
                    raise Exception("INFURA_CREDITS_EXHAUSTED")
                else:
                    logging.error(f"Error [{from_block:,}-{to_block:,}]: {e}")
                    return {}
        logging.error(f"Failed [{from_block:,}-{to_block:,}] after 5 retries")
        return {}

    # REAL-TIME INCREMENTAL WRITING: Write as each batch completes
    batch_count = 0
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    total_ranges = len(ranges)
    completed_ranges = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_range = {
            executor.submit(fetch_pool_range, range_tuple, None): range_tuple
            for range_tuple in ranges
        }

        for future in as_completed(future_to_range):
            range_tuple = future_to_range[future]
            try:
                batch_result = future.result()
                if batch_result:  # Only process non-empty batches
                    batch_count += 1
                    pools_dict.update(batch_result)

                    # Write to database immediately (every batch)
                    try:
                        store_pools_in_db(batch_result, batch_mode=True)
                        logging.info(f"📦 Batch {batch_count}: Added {len(batch_result)} pools to database")
                    except Exception as db_error:
                        logging.warning(f"⚠️ Database write failed for batch {batch_count}: {db_error}")
                        # Continue processing even if database write fails

                    # Write to JSON file every 5 batches
                    if batch_count % 5 == 0:
                        logging.info(f"💾 Incremental save: {len(pools_dict)} pools to {output_file}")
                        with open(output_file, "w") as f:
                            json.dump(pools_dict, f, indent=2)

                completed_ranges += 1
                if completed_ranges % 20 == 0:
                    progress = (completed_ranges / total_ranges) * 100
                    logging.info(f"Progress: {completed_ranges}/{total_ranges} ranges ({progress:.1f}%)")

            except Exception as e:
                if "INFURA_CREDITS_EXHAUSTED" in str(e):
                    logging.critical(f"🚨 STOPPING ALL PROCESSING - INFURA CREDITS EXHAUSTED 🚨")
                    logging.critical(f"💸 API credits consumed. Resume tomorrow with fresh credits.")
                    logging.critical(f"📊 Progress saved: {len(pools_dict)} pools processed so far")
                    break  # Stop processing immediately
                else:
                    logging.error(f"Range {range_tuple} failed: {e}")

    # Write current state before adding symbols
    if pools_dict:
        logging.info(f"💾 Pre-symbols save: {len(pools_dict)} pools to {output_file}")
        with open(output_file, "w") as f:
            json.dump(pools_dict, f, indent=2)

    if include_symbols:
        logging.info("🔍 Fetching token symbols for pools...")
        pools_dict = _add_symbols_to_pools_dict(pools_dict, skip_existing=True)

        # Final save with symbols
        with open(output_file, "w") as f:
            json.dump(pools_dict, f, indent=2)

    symbol_info = " with symbols" if include_symbols else ""
    logging.info(
        f"✓ Saved {len(pools_dict)} V3 pools{symbol_info} to {output_file} and database"
    )
    logging.info(f"🎯 Returning pool addresses list with {len(pools_dict)} entries")
    return list(pools_dict.keys())


def _add_symbols_to_pools_dict(pools_dict, skip_existing=True):
    """
    Add token0_symbol and token1_symbol to pools dictionary using multicall optimization
    """
    logging.info(f"📊 Adding token symbols to {len(pools_dict)} pools...")

    # Check which pools already have symbols if skip_existing is True
    pools_needing_symbols = {}
    pools_with_symbols = {}

    if skip_existing:
        for pool_addr, pool_data in pools_dict.items():
            if pool_data.get("token0_symbol") and pool_data.get("token1_symbol"):
                pools_with_symbols[pool_addr] = pool_data
            else:
                pools_needing_symbols[pool_addr] = pool_data

        if pools_with_symbols:
            logging.info(
                f"✓ Found {len(pools_with_symbols)} pools already with symbols"
            )

        if not pools_needing_symbols:
            logging.info("✓ All pools already have symbols - no API calls needed!")
            return pools_dict

        logging.info(f"🔍 Need to fetch symbols for {len(pools_needing_symbols)} pools")
        target_pools = pools_needing_symbols
    else:
        target_pools = pools_dict

    # Collect all unique token addresses that need symbols
    token_addresses = set()
    for pool_data in target_pools.values():
        token_addresses.add(pool_data["token0"])
        token_addresses.add(pool_data["token1"])

    logging.info(f"🔍 Fetching symbols for {len(token_addresses)} unique tokens...")

    # Fetch token metadata in batches using existing optimized function
    token_metadata = {}
    token_list = list(token_addresses)
    batch_size = 50

    for i in range(0, len(token_list), batch_size):
        batch = token_list[i : i + batch_size]
        logging.info(
            f"Processing token batch {i//batch_size + 1}/{(len(token_list) + batch_size - 1)//batch_size}"
        )

        for token_addr in batch:
            try:
                metadata = get_token_metadata_optimized(
                    token_addr, provider=None, db_pool=DB_POOL
                )
                if metadata:
                    token_metadata[token_addr] = metadata
            except Exception as e:
                logging.warning(f"Failed to get metadata for token {token_addr}: {e}")
                continue

    # Add symbols to pools dictionary
    enhanced_pools = {}

    # First add pools that already had symbols
    if skip_existing:
        enhanced_pools.update(pools_with_symbols)

    # Then add/update pools that needed symbols
    for pool_addr, pool_data in target_pools.items():
        enhanced_data = pool_data.copy()

        token0_meta = token_metadata.get(pool_data["token0"], {})
        token1_meta = token_metadata.get(pool_data["token1"], {})

        enhanced_data["token0_symbol"] = token0_meta.get("symbol") or pool_data.get(
            "token0_symbol"
        )
        enhanced_data["token1_symbol"] = token1_meta.get("symbol") or pool_data.get(
            "token1_symbol"
        )

        enhanced_pools[pool_addr] = enhanced_data

    logging.info(f"✓ Added token symbols to pools dictionary")
    return enhanced_pools


def _add_symbols_to_existing_pools_smart(pools_dict, output_file):
    """
    Smart version that only fetches missing symbols from existing pools dictionary
    """
    logging.info("🔄 Checking existing pool data for missing symbols...")
    enhanced_pools = _add_symbols_to_pools_dict(pools_dict, skip_existing=True)

    with open(output_file, "w") as f:
        json.dump(enhanced_pools, f, indent=2)

    logging.info(f"✓ Updated {output_file} with token symbols")
    return list(enhanced_pools.keys())


def _add_symbols_to_existing_pools(pools_dict, output_file):
    """
    Add symbols to existing pools dictionary and save it (legacy - forces refetch)
    """
    logging.info("🔄 Adding symbols to existing pool data...")
    enhanced_pools = _add_symbols_to_pools_dict(pools_dict, skip_existing=False)

    with open(output_file, "w") as f:
        json.dump(enhanced_pools, f, indent=2)

    logging.info(f"✓ Updated {output_file} with token symbols")
    return list(enhanced_pools.keys())


def generate_v3_pool_list(
    output_file, start_block=FACTORY_DEPLOYMENT_BLOCK, max_workers=4
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
        if provider is None:
            provider, _ = PROVIDER_POOL.get_provider()

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
                    # Get token metadata for names and symbols
                    token0_meta = get_token_metadata_optimized(
                        log.args.token0, provider=None, db_pool=DB_POOL
                    )
                    token1_meta = get_token_metadata_optimized(
                        log.args.token1, provider=None, db_pool=DB_POOL
                    )

                    pools[log.args.pool] = {
                        "token0": log.args.token0,
                        "token1": log.args.token1,
                        "token0_name": (
                            token0_meta.get("name", "Unknown")
                            if token0_meta
                            else "Unknown"
                        ),
                        "token1_name": (
                            token1_meta.get("name", "Unknown")
                            if token1_meta
                            else "Unknown"
                        ),
                        "token0_symbol": (
                            token0_meta.get("symbol", "UNKNOWN")
                            if token0_meta
                            else "UNKNOWN"
                        ),
                        "token1_symbol": (
                            token1_meta.get("symbol", "UNKNOWN")
                            if token1_meta
                            else "UNKNOWN"
                        ),
                        "fee": log.args.fee,
                        "tickSpacing": log.args.tickSpacing,
                        "created_block": log.blockNumber,
                    }
                if pools:
                    logging.info(
                        f"[{from_block:,} - {to_block:,}] Found {len(pools)} pools with token names"
                    )
                return pools
            except Exception as e:
                error_str = str(e).lower()
                if "429" in error_str or "too many requests" in error_str:
                    wait = min(10 * (retry + 1), 60)
                    logging.warning(
                        f"Rate limit [{from_block:,}-{to_block:,}], retry in {wait}s"
                    )
                    time.sleep(wait)
                    provider, _ = PROVIDER_POOL.get_provider()
                    continue
                elif "402" in error_str or "payment required" in error_str:
                    logging.critical(f"💸 INFURA CREDITS EXHAUSTED - STOPPING IMMEDIATELY")
                    logging.critical(f"❌ All API credits have been consumed. Come back tomorrow.")
                    logging.critical(f"🛑 Range [{from_block:,}-{to_block:,}] failed due to payment required")
                    raise Exception("INFURA_CREDITS_EXHAUSTED")
                else:
                    logging.error(f"Error [{from_block:,}-{to_block:,}]: {e}")
                    return {}
        logging.error(f"Failed [{from_block:,}-{to_block:,}] after 5 retries")
        return {}

    # REAL-TIME INCREMENTAL WRITING: Write as each batch completes
    batch_count = 0
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    total_ranges = len(ranges)
    completed_ranges = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_range = {
            executor.submit(fetch_pool_range, range_tuple, None): range_tuple
            for range_tuple in ranges
        }

        for future in as_completed(future_to_range):
            range_tuple = future_to_range[future]
            try:
                batch_result = future.result()
                if batch_result:  # Only process non-empty batches
                    batch_count += 1
                    pools_dict.update(batch_result)

                    # Write to database immediately (every batch)
                    try:
                        store_pools_in_db(batch_result, batch_mode=True)
                        logging.info(f"📦 Batch {batch_count}: Added {len(batch_result)} pools to database")
                    except Exception as db_error:
                        logging.warning(f"⚠️ Database write failed for batch {batch_count}: {db_error}")
                        # Continue processing even if database write fails

                    # Write to JSON file every 5 batches
                    if batch_count % 5 == 0:
                        logging.info(f"💾 Incremental save: {len(pools_dict)} pools to {output_file}")
                        with open(output_file, "w") as f:
                            json.dump(pools_dict, f, indent=2)

                completed_ranges += 1
                if completed_ranges % 20 == 0:
                    progress = (completed_ranges / total_ranges) * 100
                    logging.info(f"Progress: {completed_ranges}/{total_ranges} ranges ({progress:.1f}%)")

            except Exception as e:
                if "INFURA_CREDITS_EXHAUSTED" in str(e):
                    logging.critical(f"🚨 STOPPING ALL PROCESSING - INFURA CREDITS EXHAUSTED 🚨")
                    logging.critical(f"💸 API credits consumed. Resume tomorrow with fresh credits.")
                    logging.critical(f"📊 Progress saved: {len(pools_dict)} pools processed so far")
                    break  # Stop processing immediately
                else:
                    logging.error(f"Range {range_tuple} failed: {e}")

    # Final save
    if pools_dict:
        logging.info(f"💾 Final save: {len(pools_dict)} pools to {output_file}")
        with open(output_file, "w") as f:
            json.dump(pools_dict, f, indent=2)

    logging.info(f"✓ Saved {len(pools_dict)} V3 pools to {output_file} and database")
    return list(pools_dict.keys())


def fetch_and_store_block_metadata(block_numbers, max_workers=4):
    if not block_numbers:
        return 0

    missing_blocks = [b for b in block_numbers if not is_block_metadata_cached(b)]
    if not missing_blocks:
        logging.info("✓ All blocks already cached")
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
        missing_blocks,
        fetch_single_block,
        max_workers=max_workers,
        desc="Fetching block metadata",
    )
    blocks_data = [b for b in blocks_data if b is not None]

    if blocks_data:
        batch_insert_block_metadata(blocks_data)
        logging.info(f"✓ Stored metadata for {len(blocks_data)} blocks")

    return len(blocks_data)


def collect_missing_pair_metadata(batch_size=100, provider=None, max_workers=12):
    """
    MULTICALL OPTIMIZED: Collects pair metadata using batched multicalls
    Reduces API calls by ~75% compared to individual calls
    """
    conn = DB_POOL.get_connection()

    all_pairs = conn.execute(
        """
        SELECT DISTINCT pair_address FROM (
            SELECT DISTINCT pair_address FROM transfer UNION
            SELECT DISTINCT pair_address FROM swap UNION
            SELECT DISTINCT pair_address FROM mint UNION
            SELECT DISTINCT pair_address FROM burn UNION
            SELECT DISTINCT pair_address FROM collect UNION
            SELECT DISTINCT pair_address FROM flash
        ) WHERE pair_address IS NOT NULL"""
    ).fetchall()
    all_pairs = [r[0] for r in all_pairs]

    existing_pairs = conn.execute(
        """
        SELECT pair_address FROM pair_metadata
        WHERE token0_decimals IS NOT NULL AND token1_decimals IS NOT NULL"""
    ).fetchall()
    existing_pairs = set(r[0] for r in existing_pairs)

    missing_pairs = [p for p in all_pairs if p not in existing_pairs]

    if not missing_pairs:
        logging.info("✓ All pairs already have metadata")
        return

    logging.info(
        f"Found {len(missing_pairs)} pairs missing metadata out of {len(all_pairs)} total"
    )
    logging.info("🚀 Using MULTICALL optimization for maximum speed...")

    successful = 0
    total_batches = (len(missing_pairs) + batch_size - 1) // batch_size

    for i in range(0, len(missing_pairs), batch_size):
        batch = missing_pairs[i : i + batch_size]
        batch_num = i // batch_size + 1
        logging.info(
            f"Processing batch {batch_num}/{total_batches} ({len(batch)} pairs) via Multicall"
        )

        # Use multicall batch fetching
        try:
            batch_results = fetch_uniswap_pair_metadata_multicall(batch, provider)

            # Store successful results
            pairs_data = []
            for pair_addr, metadata in zip(batch, batch_results):
                if metadata and metadata.get("token0_decimals") is not None:
                    pairs_data.append(
                        (
                            metadata["pair_address"],
                            metadata["token0_address"],
                            metadata["token1_address"],
                            metadata.get("token0_symbol"),
                            metadata.get("token1_symbol"),
                            metadata.get("token0_decimals"),
                            metadata.get("token1_decimals"),
                            metadata.get("fee_tier"),
                            metadata.get("tick_spacing"),
                            None,
                        )
                    )
                    successful += 1

            if pairs_data:
                batch_insert_pair_metadata(pairs_data)

        except Exception as e:
            logging.error(f"Batch {batch_num} failed: {e}")

    logging.info(
        f"✓ Multicall metadata collection complete: {successful} successful out of {len(missing_pairs)} pairs"
    )
    logging.info(
        f"🎯 Estimated API call reduction: ~75% (Multicall vs individual calls)"
    )


def normalize_missing_pairs(max_workers=8):
    conn = DB_POOL.get_connection()
    pairs_with_metadata = conn.execute(
        """
        SELECT pair_address FROM pair_metadata
        WHERE token0_decimals IS NOT NULL AND token1_decimals IS NOT NULL"""
    ).fetchall()

    if not pairs_with_metadata:
        logging.warning(
            "No pairs with metadata found - run collect_missing_pair_metadata() first"
        )
        return

    pairs_to_normalize = []
    for (pair_address,) in pairs_with_metadata:
        needs_norm = conn.execute(
            """
            SELECT COUNT(*) FROM transfer
            WHERE pair_address = ? AND value_normalized IS NULL LIMIT 1""",
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
            executor.submit(normalize_event_values, pair): pair
            for pair in pairs_to_normalize
        }
        completed = 0
        for future in as_completed(future_to_pair):
            try:
                future.result()
                completed += 1
                if completed % 20 == 0 or completed == len(pairs_to_normalize):
                    logging.info(
                        f"Progress: {completed}/{len(pairs_to_normalize)} pairs normalized"
                    )
            except Exception as e:
                logging.warning(f"Failed to normalize pair: {e}")

    logging.info("✓ Normalization complete")


def fetch_logs_for_range(
    start_block, end_block, addresses, worker_id="main", retry_count=0, max_retries=5
):
    # Check if this exact range has been analyzed for these specific addresses
    analyzed_addresses = is_block_range_analyzed_for_addresses(
        start_block, end_block, addresses
    )

    if analyzed_addresses:
        # Some or all addresses already analyzed
        analyzed_list = list(analyzed_addresses.keys())
        total_events = sum(
            addr_info["events_found"] for addr_info in analyzed_addresses.values()
        )

        if len(analyzed_list) == len(addresses):
            # ALL addresses already analyzed for this range
            logging.info(
                f"[{worker_id}] ✅ Range [{start_block:,} - {end_block:,}] already analyzed for ALL {len(addresses)} addresses "
                f"({total_events:,} events found previously) - skipping Infura call"
            )
            return []
        else:
            # Some addresses analyzed, some not
            remaining_addresses = [
                addr for addr in addresses if addr not in analyzed_list
            ]
            logging.info(
                f"[{worker_id}] ⚡ Range [{start_block:,} - {end_block:,}] partially analyzed: "
                f"{len(analyzed_list)} done, {len(remaining_addresses)} remaining"
            )
            # Continue with remaining addresses only
            addresses = remaining_addresses

    provider, provider_name = PROVIDER_POOL.get_provider()

    try:
        logging.info(
            f"[{worker_id}] 🔗 Making Infura call for blocks [{start_block:,} - {end_block:,}] | {len(addresses)} addresses"
        )
        params = {"fromBlock": start_block, "toBlock": end_block, "address": addresses}
        logs = provider.eth.get_logs(params)

        logs_by_address = {}
        for log in logs:
            addr = log["address"]
            if addr not in logs_by_address:
                logs_by_address[addr] = []
            logs_by_address[addr].append(log)

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
            logging.warning(
                f"[{worker_id}] [{provider_name}] Response too large (413) for range [{start_block:,} - {end_block:,}] - will split"
            )
            raise Web3RPCError("Response payload too large - splitting range")
        elif e.response.status_code == 429:
            if retry_count < max_retries:
                wait_time = min((2**retry_count) + random.uniform(1, 3), 30)
                logging.warning(
                    f"[{worker_id}] [{provider_name}] Rate limit hit, waiting {wait_time:.1f}s..."
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
        WHERE block_number BETWEEN ? AND ?""",
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
    # Check if ALL addresses have been analyzed for this exact range
    analyzed_addresses = is_block_range_analyzed_for_addresses(
        start_block, end_block, addresses
    )

    if analyzed_addresses and len(analyzed_addresses) == len(addresses):
        # All addresses already analyzed
        total_events = sum(
            addr_info["events_found"] for addr_info in analyzed_addresses.values()
        )
        logging.info(
            f"[{worker_id}] ✅ Range [{start_block:,} - {end_block:,}] already analyzed for ALL {len(addresses)} addresses "
            f"({total_events:,} events) - skipping"
        )
        return 0

    mark_range_processing(start_block, end_block, worker_id)

    try:
        # fetch_logs_for_range will handle partially analyzed ranges
        events = fetch_logs_for_range(start_block, end_block, addresses, worker_id)

        # Group events by address to track per-address counts
        events_by_address = {}
        for event in events:
            addr = event.get("address")
            if addr:
                events_by_address[addr] = events_by_address.get(addr, 0) + 1

        if events:
            # CRITICAL: Only proceed if database insertion is successful
            inserted_count = batch_insert_events(events, worker_id)

            # Verify insertion actually worked
            if inserted_count == 0:
                raise Exception("Database insertion failed - no events were stored")

            if inserted_count < len(events):
                duplicates_skipped = len(events) - inserted_count
                logging.info(
                    f"[{worker_id}] ✓ Processed [{start_block:,} - {end_block:,}] - {inserted_count} new events, {duplicates_skipped} duplicates skipped"
                )
            else:
                logging.info(
                    f"[{worker_id}] ✓ Processed [{start_block:,} - {end_block:,}] - {inserted_count} events"
                )

            # CRITICAL: Only mark as analyzed AFTER successful database insertion
            # This ensures we never claim completion if data wasn't actually stored
            for address in addresses:
                events_found = events_by_address.get(address, 0)
                mark_block_range_analyzed(
                    start_block, end_block, address, events_found, worker_id
                )

        else:
            # No events found - only mark as analyzed after confirming the range was fully processed
            logging.info(
                f"[{worker_id}] ✓ Processed [{start_block:,} - {end_block:,}] - 0 events found"
            )

            # CRITICAL: Only mark as analyzed AFTER confirming the range was fully scanned
            for address in addresses:
                mark_block_range_analyzed(start_block, end_block, address, 0, worker_id)

        # Mark in old tracking system only after new tracking is complete
        mark_range_completed(start_block, end_block, worker_id)
        return len(events) if events else 0

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
            # CRITICAL: Do NOT mark as analyzed if there was an error
            return 0

    except Exception as e:
        logging.error(
            f"[{worker_id}] Unexpected error [{start_block:,}, {end_block:,}]: {e}"
        )
        # CRITICAL: Do NOT mark as analyzed if there was an unexpected error
        return 0


def scan_blockchain(addresses, start_block, end_block, chunk_size=10000, max_workers=3):
    # Use new address-aware range generation
    missing_ranges = generate_block_ranges_for_addresses(
        start_block, end_block, chunk_size, addresses
    )

    if not missing_ranges:
        logging.info(
            "🎯 All block ranges already analyzed for these addresses - nothing to process!"
        )
        return

    total_ranges = len(missing_ranges)
    logging.info(f"Processing {total_ranges} block ranges with {max_workers} workers")

    total_events = 0
    completed_ranges = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_range = {}
        for i, (start, end, missing_addresses) in enumerate(missing_ranges):
            future = executor.submit(
                process_block_range,
                start,
                end,
                missing_addresses,
                f"worker-{i % max_workers}",
            )
            future_to_range[future] = (start, end, missing_addresses, i)

        for future in as_completed(future_to_range):
            start, end, missing_addresses, idx = future_to_range[future]
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
                    f"Events: {total_events:,} | Rate: {rate:.1f} ranges/s | ETA: {eta_str}"
                )
            except Exception as e:
                logging.error(
                    f"Range [{start:,}, {end:,}] with {len(missing_addresses)} addresses failed: {e}"
                )

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
    logging.info("BLOCKCHAIN SCANNER - MULTICALL OPTIMIZED")
    logging.info("=" * 60)

    pool_list_file = f"{BASE_OUTPUT_DIR}/uniswap_v3_pools.json"
    all_addresses = generate_v3_pool_list(pool_list_file)

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
    logging.info("🚀 MULTICALL optimization active for metadata collection")
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


def run_full_pipeline(
    start_block=FACTORY_DEPLOYMENT_BLOCK,
    end_block=None,
    chunk_size=10000,
    max_workers=3,
    token_filter=None,
):
    try:
        logging.info("=" * 80)
        logging.info("UNISWAP V3 DATA PIPELINE - MULTICALL OPTIMIZED")
        logging.info("=" * 80)

        provider, _ = PROVIDER_POOL.get_provider()
        if end_block is None:
            end_block = provider.eth.block_number
            logging.info(f"Using current block as end: {end_block:,}")

        logging.info(f"Block range: {start_block:,} → {end_block:,}")
        logging.info(f"Configuration: chunk={chunk_size:,}, workers={max_workers}")
        logging.info("🚀 Multicall optimizations: ACTIVE")

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
        logging.info("STAGE 2: METADATA COLLECTION (MULTICALL OPTIMIZED)")
        logging.info("=" * 80)
        logging.info("\n📦 Collecting pair metadata via Multicall...")
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
        logging.info("✅ PIPELINE COMPLETE - MULTICALL OPTIMIZED")
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


print("🚀 Multicall-optimized functions loaded - Maximum Infura efficiency!")


# In[20]:


# =============================================================================
# CELL 6 - FINAL COMPREHENSIVE ORCHESTRATOR
# Integrates all previous cells into a complete pipeline management system
# Run this cell after executing cells 1-5 in sequence
# =============================================================================


def print_pipeline_banner():
    print("=" * 80)
    print("🦄 UNISWAP V3 DATA PIPELINE - MULTICALL OPTIMIZED")
    print("=" * 80)
    print("📊 Comprehensive ETL Pipeline for Uniswap V3 Pool Data")
    print("🚀 Featuring Multicall optimization for maximum efficiency")
    print("💾 DuckDB analytics database with full normalization")
    print("⚡ Multi-threaded processing with intelligent rate limiting")
    print("=" * 80)


def get_pipeline_status():
    stats = get_database_stats()
    current_block = w3.eth.block_number

    print("\n📋 CURRENT PIPELINE STATUS")
    print("=" * 50)
    print(f"🔗 Latest Ethereum Block: {current_block:,}")
    print(f"📦 Database Events:")
    print(f"   • Transfers:  {stats['total_transfers']:>12,}")
    print(f"   • Swaps:      {stats['total_swaps']:>12,}")
    print(f"   • Mints:      {stats['total_mints']:>12,}")
    print(f"   • Burns:      {stats['total_burns']:>12,}")
    print(f"   • Collects:   {stats['total_collects']:>12,}")
    print(f"   • Flashes:    {stats['total_flashes']:>12,}")

    total_events = (
        stats["total_transfers"]
        + stats["total_swaps"]
        + stats["total_mints"]
        + stats["total_burns"]
        + stats["total_collects"]
        + stats["total_flashes"]
    )

    print(f"   • TOTAL:      {total_events:>12,}")
    print(f"🏊 Pools Tracked: {stats['total_pairs']:,}")
    print(f"🧱 Block Metadata: {stats['total_blocks']:,}")
    print(f"✅ Completed Ranges: {stats['completed_ranges']:,}")
    print("=" * 50)

    return stats, current_block


def show_optimization_stats():
    print("\n🚀 OPTIMIZATION STATISTICS")
    print("=" * 50)
    print_optimization_stats()


def quick_scan_recent_blocks(hours_back=1, max_workers=3):
    print(f"\n⚡ QUICK SCAN - Last {hours_back} Hour{'s' if hours_back != 1 else ''}")
    print("=" * 50)

    current_block = w3.eth.block_number
    blocks_per_hour = 300
    start_block = max(
        current_block - (hours_back * blocks_per_hour), FACTORY_DEPLOYMENT_BLOCK
    )

    print(f"📍 Scanning blocks {start_block:,} → {current_block:,}")

    scan_blockchain_to_duckdb(
        start_block=start_block,
        end_block=current_block,
        chunk_size=1000,
        max_workers=max_workers,
    )


def comprehensive_metadata_update():
    print("\n📦 COMPREHENSIVE METADATA UPDATE")
    print("=" * 50)

    print("🔍 Collecting missing pair metadata...")
    collect_missing_pair_metadata(batch_size=100, max_workers=8)

    print("📊 Normalizing event values...")
    normalize_missing_pairs(max_workers=8)

    print("⏰ Denormalizing timestamps...")
    denormalize_timestamps()

    print("🏷️  Denormalizing pair symbols...")
    denormalize_pair_symbols()

    print("📈 Refreshing pool summaries...")
    refresh_pool_summary()

    print("🎯 Updating pool current states...")
    aggregate_all_pools(max_workers=8)

    print("✅ Metadata update complete!")


def run_targeted_pool_scan(pool_addresses, start_block=None, end_block=None):
    if not pool_addresses:
        print("❌ No pool addresses provided")
        return

    if start_block is None:
        start_block = FACTORY_DEPLOYMENT_BLOCK
    if end_block is None:
        end_block = w3.eth.block_number

    print(f"\n🎯 TARGETED POOL SCAN")
    print("=" * 50)
    print(f"🏊 Pools: {len(pool_addresses)}")
    print(f"📍 Blocks: {start_block:,} → {end_block:,}")

    scan_blockchain_to_duckdb(
        start_block=start_block,
        end_block=end_block,
        chunk_size=10000,
        max_workers=3,
        token_filter=pool_addresses,
    )


def run_historical_backfill(
    start_block=None, end_block=None, chunk_size=10000, max_workers=3
):
    if start_block is None:
        start_block = FACTORY_DEPLOYMENT_BLOCK
    if end_block is None:
        end_block = w3.eth.block_number

    print(f"\n📚 HISTORICAL BACKFILL")
    print("=" * 50)
    print(f"📍 Block Range: {start_block:,} → {end_block:,}")
    print(f"⚙️  Configuration: chunk={chunk_size:,}, workers={max_workers}")

    run_full_pipeline(
        start_block=start_block,
        end_block=end_block,
        chunk_size=chunk_size,
        max_workers=max_workers,
    )


def get_top_pools_by_volume(limit=20):
    conn = DB_POOL.get_connection()
    try:
        top_pools = conn.execute(
            f"""
            SELECT pair_address, token0_symbol, token1_symbol, fee_tier,
                   total_volume_token0, total_volume_token1, total_swaps
            FROM pool_summary
            WHERE total_volume_token0 > 0 AND token0_symbol IS NOT NULL
            ORDER BY total_volume_token0 DESC
            LIMIT {limit}
        """
        ).fetchall()

        print(f"\n🏆 TOP {limit} POOLS BY VOLUME")
        print("=" * 100)
        print(
            f"{'Rank':<4} {'Pool':<12} {'Pair':<20} {'Fee':<6} {'Volume Token0':<15} {'Swaps':<10}"
        )
        print("-" * 100)

        for i, pool in enumerate(top_pools, 1):
            (
                pair_address,
                token0_symbol,
                token1_symbol,
                fee_tier,
                volume0,
                volume1,
                swaps,
            ) = pool
            pair_name = f"{token0_symbol or 'UNKNOWN'}/{token1_symbol or 'UNKNOWN'}"
            fee_display = f"{fee_tier/10000:.2f}%" if fee_tier else "N/A"
            volume_display = f"{volume0:,.0f}" if volume0 else "0"

            print(
                f"{i:<4} {pair_address[:12]:<12} {pair_name:<20} {fee_display:<6} {volume_display:<15} {swaps or 0:<10,}"
            )

    except Exception as e:
        print(f"❌ Error querying pool summary: {e}")
        print("💡 Try running comprehensive_metadata_update() first")


def run_maintenance_tasks():
    print("\n🔧 RUNNING MAINTENANCE TASKS")
    print("=" * 50)

    try:
        conn = DB_POOL.get_connection()

        print("📊 Updating database statistics...")
        conn.execute("ANALYZE")

        print("🗂️  Checking table sizes...")
        tables = [
            "transfer",
            "swap",
            "mint",
            "burn",
            "collect",
            "flash",
            "pair_metadata",
            "block_metadata",
        ]

        for table in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"   • {table}: {count:,} rows")
            except Exception as e:
                print(f"   • {table}: Error - {e}")

        print("✅ Maintenance complete!")

    except Exception as e:
        print(f"❌ Maintenance error: {e}")


def export_data_sample(output_dir="exports", limit=1000):
    import os

    os.makedirs(output_dir, exist_ok=True)

    print(f"\n📤 EXPORTING DATA SAMPLES (limit: {limit:,})")
    print("=" * 50)

    try:
        conn = DB_POOL.get_connection()

        # Export recent swaps
        swaps = conn.execute(
            f"""
            SELECT * FROM swap
            WHERE block_timestamp IS NOT NULL
            ORDER BY block_number DESC
            LIMIT {limit}
        """
        ).fetchdf()

        swaps_file = f"{output_dir}/recent_swaps_{limit}.parquet"
        swaps.to_parquet(swaps_file)
        print(f"📊 Exported {len(swaps):,} recent swaps to {swaps_file}")

        # Export pool summary
        pools = conn.execute(
            "SELECT * FROM pool_summary ORDER BY total_volume_token0 DESC"
        ).fetchdf()
        pools_file = f"{output_dir}/pool_summary.parquet"
        pools.to_parquet(pools_file)
        print(f"🏊 Exported {len(pools):,} pool summaries to {pools_file}")

        print("✅ Export complete!")

    except Exception as e:
        print(f"❌ Export error: {e}")


def interactive_pipeline_menu():
    while True:
        print("\n" + "=" * 60)
        print("🦄 UNISWAP V3 PIPELINE - INTERACTIVE MENU")
        print("=" * 60)
        print("1. 📊 Show Pipeline Status")
        print("2. 🚀 Show Optimization Stats")
        print("3. ⚡ Quick Scan (Recent 1 Hour)")
        print("4. ⚡ Quick Scan (Recent 24 Hours)")
        print("5. 📦 Update All Metadata")
        print("6. 🏆 Show Top 20 Pools by Volume")
        print("7. 📚 Historical Backfill (Custom Range)")
        print("8. 🎯 Targeted Pool Scan")
        print("9. 🔧 Run Full Pipeline (All Stages)")
        print("10. 🔧 Run Maintenance Tasks")
        print("11. 📤 Export Data Sample")
        print("12. 🏷️  Generate Pool List with Token Symbols")
        print("0. 🚪 Exit")
        print("=" * 60)

        try:
            choice = input("Select option (0-12): ").strip()

            if choice == "0":
                print("👋 Goodbye!")
                break
            elif choice == "1":
                get_pipeline_status()
            elif choice == "2":
                show_optimization_stats()
            elif choice == "3":
                quick_scan_recent_blocks(hours_back=1)
            elif choice == "4":
                quick_scan_recent_blocks(hours_back=24)
            elif choice == "5":
                comprehensive_metadata_update()
            elif choice == "6":
                get_top_pools_by_volume()
            elif choice == "7":
                try:
                    start = input("Start block (default: factory deployment): ").strip()
                    end = input("End block (default: current): ").strip()
                    chunk = input("Chunk size (default: 10000): ").strip()
                    workers = input("Max workers (default: 3): ").strip()

                    start_block = int(start) if start else None
                    end_block = int(end) if end else None
                    chunk_size = int(chunk) if chunk else 10000
                    max_workers = int(workers) if workers else 3

                    run_historical_backfill(
                        start_block, end_block, chunk_size, max_workers
                    )
                except ValueError:
                    print("❌ Invalid input. Please enter valid numbers.")
            elif choice == "8":
                pools_input = input("Enter pool addresses (comma-separated): ").strip()
                if pools_input:
                    pool_addresses = [addr.strip() for addr in pools_input.split(",")]
                    try:
                        start = input("Start block (optional): ").strip()
                        end = input("End block (optional): ").strip()
                        start_block = int(start) if start else None
                        end_block = int(end) if end else None
                        run_targeted_pool_scan(pool_addresses, start_block, end_block)
                    except ValueError:
                        print("❌ Invalid block numbers")
                else:
                    print("❌ No pool addresses provided")
            elif choice == "9":
                try:
                    start = input("Start block (default: factory deployment): ").strip()
                    end = input("End block (default: current): ").strip()
                    chunk = input("Chunk size (default: 10000): ").strip()
                    workers = input("Max workers (default: 3): ").strip()

                    start_block = int(start) if start else FACTORY_DEPLOYMENT_BLOCK
                    end_block = int(end) if end else None
                    chunk_size = int(chunk) if chunk else 10000
                    max_workers = int(workers) if workers else 3

                    run_full_pipeline(start_block, end_block, chunk_size, max_workers)
                except ValueError:
                    print("❌ Invalid input. Please enter valid numbers.")
            elif choice == "10":
                run_maintenance_tasks()
            elif choice == "11":
                try:
                    limit = input("Sample size (default: 1000): ").strip()
                    limit = int(limit) if limit else 1000
                    export_data_sample(limit=limit)
                except ValueError:
                    print("❌ Invalid sample size")
            elif choice == "12":
                try:
                    output_file = input(
                        f"Output file (default: {V3_EVENT_BY_CONTRACTS}): "
                    ).strip()
                    if not output_file:
                        output_file = V3_EVENT_BY_CONTRACTS

                    print("🏷️  Generating pool list with token symbols...")
                    pools = generate_v3_pool_list_with_symbols(
                        output_file, include_symbols=True
                    )
                    print(
                        f"✅ Generated {len(pools)} pools with symbols in {output_file}"
                    )
                except Exception as e:
                    print(f"❌ Error generating pool list: {e}")
            else:
                print("❌ Invalid option. Please select 0-12.")

        except KeyboardInterrupt:
            print("\n\n👋 Interrupted by user. Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")


def main_orchestrator():
    try:
        print_pipeline_banner()

        # Initial status check
        stats, current_block = get_pipeline_status()
        interactive_pipeline_menu()
        # Show available operations
        print("\n🎛️  AVAILABLE OPERATIONS:")
        print("=" * 50)
        print("• interactive_pipeline_menu() - Full interactive menu")
        print("• quick_scan_recent_blocks(hours=1) - Scan recent blocks")
        print("• comprehensive_metadata_update() - Update all metadata")
        print("• run_historical_backfill(start, end) - Backfill historical data")
        print("• get_top_pools_by_volume(limit=20) - Show top pools")
        print("• run_maintenance_tasks() - Database maintenance")
        print("• export_data_sample(limit=1000) - Export sample data")
        print("• run_full_pipeline() - Complete ETL pipeline")
        print("=" * 50)

        return stats, current_block

    except Exception as e:
        print(f"❌ Main orchestrator error: {e}")
        logging.error(f"Main orchestrator error: {e}", exc_info=True)
        return None, None


# Convenience functions for common operations
def quick_start():
    print_pipeline_banner()
    get_pipeline_status()
    print("\n💡 Starting interactive menu...")
    interactive_pipeline_menu()


def dev_mode():
    print_pipeline_banner()
    print("\n🛠️  DEVELOPER MODE ACTIVATED")
    print("=" * 50)
    print("Available functions loaded:")
    print("• main_orchestrator() - Main entry point")
    print("• quick_start() - Interactive mode")
    print("• test_optimizations() - Run optimization tests")
    print("• print_optimization_stats() - Show cache stats")
    get_pipeline_status()


def test_optimizations():
    print("\n🧪 TESTING OPTIMIZATIONS")
    print("=" * 50)
    test_optimizations()
    show_optimization_stats()


# Pipeline presets for common use cases
def preset_daily_maintenance():
    print("🔄 DAILY MAINTENANCE PRESET")
    print("=" * 40)
    quick_scan_recent_blocks(hours_back=24, max_workers=2)
    comprehensive_metadata_update()
    run_maintenance_tasks()
    print("✅ Daily maintenance complete!")


def preset_full_historical_sync():
    print("📚 FULL HISTORICAL SYNC PRESET")
    print("=" * 40)
    run_full_pipeline(
        start_block=FACTORY_DEPLOYMENT_BLOCK,
        end_block=None,
        chunk_size=10000,
        max_workers=3,
    )


def preset_quick_analysis():
    print("🔍 QUICK ANALYSIS PRESET")
    print("=" * 40)
    get_pipeline_status()
    show_optimization_stats()
    get_top_pools_by_volume(limit=10)


def add_symbols_to_existing_pools_file(input_file=None, output_file=None):
    """
    Convenience function to add token symbols to an existing pools JSON file
    """
    if input_file is None:
        input_file = V3_EVENT_BY_CONTRACTS
    if output_file is None:
        output_file = input_file

    if not os.path.exists(input_file):
        print(f"❌ Input file {input_file} does not exist")
        return

    print(f"🔄 Adding token symbols to existing pools from {input_file}")

    with open(input_file, "r") as f:
        pools_dict = json.load(f)

    enhanced_pools = _add_symbols_to_pools_dict(pools_dict, skip_existing=True)

    with open(output_file, "w") as f:
        json.dump(enhanced_pools, f, indent=2)

    print(f"✅ Enhanced {len(enhanced_pools)} pools with token symbols")
    print(f"📄 Saved to {output_file}")

    # Show a sample of enhanced pools
    sample_pools = list(enhanced_pools.items())[:3]
    print("\n📋 Sample enhanced pools:")
    for pool_addr, pool_data in sample_pools:
        token0_symbol = pool_data.get("token0_symbol", "UNKNOWN")
        token1_symbol = pool_data.get("token1_symbol", "UNKNOWN")
        fee = pool_data.get("fee", 0)
        print(
            f"  • {pool_addr[:10]}... {token0_symbol}/{token1_symbol} ({fee/10000:.2f}%)"
        )

    return enhanced_pools


# Auto-execute notification when cell is run
def main():
    print("🦄 Uniswap V3 Pipeline Orchestrator Loaded!")
    print("=" * 50)
    print("🚀 Ready to process Uniswap V3 data with multicall optimization")
    print("📊 Available quick commands:")
    print("   • quick_start() - Interactive menu")
    print("   • dev_mode() - Developer mode")
    print("   • preset_daily_maintenance() - Daily sync")
    print("   • preset_quick_analysis() - Status overview")
    print("💡 All systems initialized and ready!")
    print("=" * 50)

    try:
        main_orchestrator()
    except KeyboardInterrupt:
        print("\n👋 Exiting...")
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.error(f"Main error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
