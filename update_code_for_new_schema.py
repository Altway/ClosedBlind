#!/usr/bin/env python3

import sys

INSTRUCTIONS = """
================================================================================
CODE UPDATE INSTRUCTIONS FOR NEW OPTIMIZED SCHEMA
================================================================================

The new schema requires changes to how we insert data. Here are the key changes:

OLD SCHEMA:
- amount0 HUGEINT (raw value)
- amount0_normalized DOUBLE (normalized value)
- sqrt_price_x96 HUGEINT (overflows!)

NEW SCHEMA:
- amount0_raw VARCHAR (raw value as string)
- amount0 DOUBLE (normalized value - FAST queries)
- abs_amount0 DOUBLE (precomputed ABS for volume)
- price_token0_token1 DOUBLE (precomputed price)
- sqrt_price_x96 VARCHAR (no overflow)

================================================================================
REQUIRED CHANGES TO main.py:
================================================================================

1. UPDATE batch_insert_events() function (around line 1631):
   - Calculate normalized values at insertion time
   - Calculate abs_amount* values
   - Calculate price from sqrtPriceX96
   - Store raw values as strings
   - Store normalized as DOUBLE

2. UPDATE SQL INSERT statements:
   - swap: 16 columns → 18 columns (add abs_amount*, price)
   - mint: 14 columns → 16 columns (add abs_amount*)
   - burn: 13 columns → 15 columns (add abs_amount*)
   - collect: 13 columns → 15 columns (add abs_amount*)
   - flash: 11 columns → 15 columns (add raw + normalized)
   - transfer: 11 columns → 12 columns (add abs_value)

3. HELPER FUNCTIONS TO ADD:

# Add after line 1573 (before _insert_one_by_one):

def calculate_price_from_sqrt(sqrt_price_x96):
    '''Calculate readable price from sqrtPriceX96'''
    if sqrt_price_x96 is None or sqrt_price_x96 == 0:
        return None
    try:
        return (float(sqrt_price_x96) / (2 ** 96)) ** 2
    except:
        return None

def safe_normalize(raw_value, decimals):
    '''Safely normalize raw value by decimals'''
    if raw_value is None or decimals is None:
        return 0.0
    try:
        return float(raw_value) / (10 ** decimals)
    except:
        return 0.0

def safe_str(value):
    '''Safely convert to string, handling None'''
    return str(value) if value is not None else None


4. UPDATE SWAP INSERTION (around line 1667):

OLD CODE:
    elif event_type == "Swap":
        swaps.append(
            base_data
            + (
                args.get("sender"),
                args.get("recipient"),
                args.get("amount0"),
                args.get("amount1"),
                args.get("sqrtPriceX96"),
                args.get("liquidity"),
                args.get("tick"),
                None,  # amount0_normalized
                None,  # amount1_normalized
                None,  # block_timestamp
                None,  # token0_symbol
                None,  # token1_symbol
            )
        )

NEW CODE:
    elif event_type == "Swap":
        amount0_raw = args.get("amount0")
        amount1_raw = args.get("amount1")
        sqrt_price = args.get("sqrtPriceX96")

        # Calculate normalized values (will be updated by normalize_event_values later if 0)
        amount0_norm = safe_normalize(amount0_raw, 18)  # Default 18 decimals
        amount1_norm = safe_normalize(amount1_raw, 18)

        swaps.append(
            base_data
            + (
                args.get("sender"),
                args.get("recipient"),
                safe_str(amount0_raw),                    # amount0_raw
                safe_str(amount1_raw),                    # amount1_raw
                amount0_norm,                             # amount0
                amount1_norm,                             # amount1
                abs(amount0_norm),                        # abs_amount0
                abs(amount1_norm),                        # abs_amount1
                safe_str(sqrt_price),                     # sqrt_price_x96
                safe_str(args.get("liquidity")),          # liquidity
                args.get("tick"),                         # tick
                calculate_price_from_sqrt(sqrt_price),    # price_token0_token1
                None,                                     # block_timestamp
                None,                                     # token0_symbol
                None,                                     # token1_symbol
            )
        )


5. UPDATE SQL INSERT FOR SWAP (around line 1726):

OLD:
    if swaps:
        conn.executemany(
            '''INSERT INTO swap
            (transaction_hash, log_index, block_number, pair_address, sender, recipient,
             amount0, amount1, sqrt_price_x96, liquidity, tick,
             amount0_normalized, amount1_normalized, block_timestamp, token0_symbol, token1_symbol)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING''',
            swaps,
        )

NEW:
    if swaps:
        try:
            conn.executemany(
                '''INSERT INTO swap
                (transaction_hash, log_index, block_number, pair_address, sender, recipient,
                 amount0_raw, amount1_raw, amount0, amount1, abs_amount0, abs_amount1,
                 sqrt_price_x96, liquidity, tick, price_token0_token1,
                 block_timestamp, token0_symbol, token1_symbol)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_hash, log_index) DO NOTHING''',
                swaps,
            )
            successfully_inserted += len(swaps)
        except Exception as e:
            if "Conversion Error" in str(e) or "out of range" in str(e):
                logging.warning(f"[{worker_id}] Batch swap insert failed, trying one-by-one")
                successfully_inserted += _insert_one_by_one(conn, swaps, "swap", worker_id)
            else:
                raise


6. SIMILAR UPDATES NEEDED FOR:
   - Mint events (add abs_amount0, abs_amount1)
   - Burn events (add abs_amount0, abs_amount1)
   - Collect events (add abs_amount0, abs_amount1)
   - Flash events (add raw + normalized columns)
   - Transfer events (add abs_value)

7. UPDATE _insert_one_by_one() SQL templates (around line 1574):

    sql_templates = {
        "swap": '''INSERT INTO swap
            (transaction_hash, log_index, block_number, pair_address, sender, recipient,
             amount0_raw, amount1_raw, amount0, amount1, abs_amount0, abs_amount1,
             sqrt_price_x96, liquidity, tick, price_token0_token1,
             block_timestamp, token0_symbol, token1_symbol)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (transaction_hash, log_index) DO NOTHING''',
        # ... update other templates similarly
    }

================================================================================
AUTOMATED PATCH (Too Complex - Manual Review Recommended)
================================================================================

Due to the complexity and importance of these changes, I recommend:

1. BACKUP YOUR CURRENT main.py:
   cp main.py main.py.backup

2. RUN THE MIGRATION SCRIPT FIRST:
   python migrate_schema.py

3. AFTER MIGRATION, manually update main.py following the guide above

4. TEST with a small block range:
   - Option 3: Quick scan 1 hour
   - Verify data is inserted correctly
   - Check that abs_amount* and price columns are populated

5. Once verified, run full pipeline

================================================================================
VERIFICATION QUERIES
================================================================================

After updating code, test with:

import duckdb
conn = duckdb.connect('out/V3/uniswap_v3.duckdb')

# Check schema
print(conn.execute("DESCRIBE swap").fetchall())

# Verify data
print(conn.execute('''
    SELECT
        COUNT(*) as total,
        COUNT(amount0_raw) as has_raw,
        COUNT(amount0) as has_normalized,
        COUNT(abs_amount0) as has_abs,
        COUNT(price_token0_token1) as has_price
    FROM swap
''').fetchone())

# Should show all columns populated after new insertions

================================================================================
"""

def main():
    print(INSTRUCTIONS)
    print("\n⚠️  WARNING: This requires manual code changes to main.py")
    print("The changes are too complex for automated patching.")
    print("\nNext steps:")
    print("1. Run: python migrate_schema.py (to upgrade existing data)")
    print("2. Manually update main.py following the guide above")
    print("3. Test with small block range")
    print("4. Run full pipeline")

if __name__ == "__main__":
    main()
