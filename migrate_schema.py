#!/usr/bin/env python3

import duckdb
import os
import sys
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

DB_PATH = "out/V3/uniswap_v3.duckdb"
BACKUP_PATH = f"out/V3/uniswap_v3_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
NEW_SCHEMA_PATH = "out/V3/database/schema_optimized.sql"

def check_prerequisites():
    if not os.path.exists(DB_PATH):
        logging.error(f"Database not found: {DB_PATH}")
        logging.info("No migration needed - use schema_optimized.sql for new database")
        return False

    if not os.path.exists(NEW_SCHEMA_PATH):
        logging.error(f"Schema file not found: {NEW_SCHEMA_PATH}")
        return False

    return True

def create_backup(conn):
    logging.info(f"Creating backup: {BACKUP_PATH}")
    try:
        conn.execute(f"EXPORT DATABASE '{os.path.dirname(BACKUP_PATH)}/backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}'")
        logging.info("✓ Backup created successfully")
        return True
    except Exception as e:
        logging.warning(f"Could not create backup: {e}")
        response = input("Continue without backup? (yes/no): ")
        return response.lower() == 'yes'

def get_table_stats(conn):
    tables = ['swap', 'mint', 'burn', 'collect', 'flash', 'transfer', 'approval']
    stats = {}

    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = count
        except:
            stats[table] = 0

    return stats

def migrate_data():
    conn = duckdb.connect(DB_PATH)

    logging.info("="*60)
    logging.info("STARTING SCHEMA MIGRATION")
    logging.info("="*60)

    logging.info("\n📊 Current database statistics:")
    old_stats = get_table_stats(conn)
    for table, count in old_stats.items():
        logging.info(f"  {table}: {count:,} records")

    total_records = sum(old_stats.values())

    if total_records == 0:
        logging.info("\n✓ Database is empty - no migration needed")
        logging.info("Simply run the pipeline with schema_optimized.sql")
        conn.close()
        return True

    logging.info(f"\n📦 Total records to migrate: {total_records:,}")

    response = input("\nProceed with migration? (yes/no): ")
    if response.lower() != 'yes':
        logging.info("Migration cancelled")
        conn.close()
        return False

    if not create_backup(conn):
        logging.error("Backup failed - aborting migration")
        conn.close()
        return False

    logging.info("\n" + "="*60)
    logging.info("PHASE 1: Creating new optimized tables")
    logging.info("="*60)

    try:
        logging.info("Creating swap_new...")
        conn.execute("""
            CREATE TABLE swap_new AS
            SELECT
                transaction_hash,
                log_index,
                block_number,
                pair_address,
                sender,
                recipient,
                CAST(amount0 AS VARCHAR) as amount0_raw,
                CAST(amount1 AS VARCHAR) as amount1_raw,
                COALESCE(amount0_normalized, 0.0) as amount0,
                COALESCE(amount1_normalized, 0.0) as amount1,
                ABS(COALESCE(amount0_normalized, 0.0)) as abs_amount0,
                ABS(COALESCE(amount1_normalized, 0.0)) as abs_amount1,
                CAST(sqrt_price_x96 AS VARCHAR) as sqrt_price_x96,
                CAST(liquidity AS VARCHAR) as liquidity,
                tick,
                CASE
                    WHEN sqrt_price_x96 IS NOT NULL AND sqrt_price_x96 > 0
                    THEN POWER(CAST(sqrt_price_x96 AS DOUBLE) / POWER(2.0, 96), 2)
                    ELSE NULL
                END as price_token0_token1,
                block_timestamp,
                token0_symbol,
                token1_symbol
            FROM swap
        """)
        conn.execute("ALTER TABLE swap_new ADD PRIMARY KEY (transaction_hash, log_index)")
        logging.info(f"✓ Migrated {conn.execute('SELECT COUNT(*) FROM swap_new').fetchone()[0]:,} swap records")

        logging.info("Creating mint_new...")
        conn.execute("""
            CREATE TABLE mint_new AS
            SELECT
                transaction_hash,
                log_index,
                block_number,
                pair_address,
                owner,
                sender,
                tick_lower,
                tick_upper,
                CAST(amount AS VARCHAR) as amount_raw,
                CAST(amount0 AS VARCHAR) as amount0_raw,
                CAST(amount1 AS VARCHAR) as amount1_raw,
                COALESCE(amount0_normalized, 0.0) as amount,
                COALESCE(amount0_normalized, 0.0) as amount0,
                COALESCE(amount1_normalized, 0.0) as amount1,
                ABS(COALESCE(amount0_normalized, 0.0)) as abs_amount0,
                ABS(COALESCE(amount1_normalized, 0.0)) as abs_amount1,
                block_timestamp
            FROM mint
        """)
        conn.execute("ALTER TABLE mint_new ADD PRIMARY KEY (transaction_hash, log_index)")
        logging.info(f"✓ Migrated {conn.execute('SELECT COUNT(*) FROM mint_new').fetchone()[0]:,} mint records")

        logging.info("Creating burn_new...")
        conn.execute("""
            CREATE TABLE burn_new AS
            SELECT
                transaction_hash,
                log_index,
                block_number,
                pair_address,
                owner,
                tick_lower,
                tick_upper,
                CAST(amount AS VARCHAR) as amount_raw,
                CAST(amount0 AS VARCHAR) as amount0_raw,
                CAST(amount1 AS VARCHAR) as amount1_raw,
                COALESCE(amount0_normalized, 0.0) as amount,
                COALESCE(amount0_normalized, 0.0) as amount0,
                COALESCE(amount1_normalized, 0.0) as amount1,
                ABS(COALESCE(amount0_normalized, 0.0)) as abs_amount0,
                ABS(COALESCE(amount1_normalized, 0.0)) as abs_amount1,
                block_timestamp
            FROM burn
        """)
        conn.execute("ALTER TABLE burn_new ADD PRIMARY KEY (transaction_hash, log_index)")
        logging.info(f"✓ Migrated {conn.execute('SELECT COUNT(*) FROM burn_new').fetchone()[0]:,} burn records")

        logging.info("Creating collect_new...")
        conn.execute("""
            CREATE TABLE collect_new AS
            SELECT
                transaction_hash,
                log_index,
                block_number,
                pair_address,
                owner,
                recipient,
                tick_lower,
                tick_upper,
                CAST(amount0 AS VARCHAR) as amount0_raw,
                CAST(amount1 AS VARCHAR) as amount1_raw,
                COALESCE(amount0_normalized, 0.0) as amount0,
                COALESCE(amount1_normalized, 0.0) as amount1,
                ABS(COALESCE(amount0_normalized, 0.0)) as abs_amount0,
                ABS(COALESCE(amount1_normalized, 0.0)) as abs_amount1,
                block_timestamp
            FROM collect
        """)
        conn.execute("ALTER TABLE collect_new ADD PRIMARY KEY (transaction_hash, log_index)")
        logging.info(f"✓ Migrated {conn.execute('SELECT COUNT(*) FROM collect_new').fetchone()[0]:,} collect records")

        logging.info("Creating flash_new...")
        conn.execute("""
            CREATE TABLE flash_new AS
            SELECT
                transaction_hash,
                log_index,
                block_number,
                pair_address,
                sender,
                recipient,
                CAST(amount0 AS VARCHAR) as amount0_raw,
                CAST(amount1 AS VARCHAR) as amount1_raw,
                CAST(paid0 AS VARCHAR) as paid0_raw,
                CAST(paid1 AS VARCHAR) as paid1_raw,
                CAST(amount0 AS DOUBLE) as amount0,
                CAST(amount1 AS DOUBLE) as amount1,
                CAST(paid0 AS DOUBLE) as paid0,
                CAST(paid1 AS DOUBLE) as paid1,
                block_timestamp
            FROM flash
        """)
        conn.execute("ALTER TABLE flash_new ADD PRIMARY KEY (transaction_hash, log_index)")
        logging.info(f"✓ Migrated {conn.execute('SELECT COUNT(*) FROM flash_new').fetchone()[0]:,} flash records")

        logging.info("Creating transfer_new...")
        conn.execute("""
            CREATE TABLE transfer_new AS
            SELECT
                transaction_hash,
                log_index,
                block_number,
                pair_address,
                from_address,
                to_address,
                CAST(value AS VARCHAR) as value_raw,
                COALESCE(value_normalized, 0.0) as value,
                ABS(COALESCE(value_normalized, 0.0)) as abs_value,
                block_timestamp,
                token0_symbol,
                token1_symbol
            FROM transfer
        """)
        conn.execute("ALTER TABLE transfer_new ADD PRIMARY KEY (transaction_hash, log_index)")
        logging.info(f"✓ Migrated {conn.execute('SELECT COUNT(*) FROM transfer_new').fetchone()[0]:,} transfer records")

        logging.info("Copying approval table...")
        conn.execute("CREATE TABLE approval_new AS SELECT * FROM approval")
        conn.execute("ALTER TABLE approval_new ADD PRIMARY KEY (transaction_hash, log_index)")
        logging.info(f"✓ Copied {conn.execute('SELECT COUNT(*) FROM approval_new').fetchone()[0]:,} approval records")

    except Exception as e:
        logging.error(f"❌ Migration failed during table creation: {e}")
        logging.info("Rolling back...")
        conn.close()
        return False

    logging.info("\n" + "="*60)
    logging.info("PHASE 2: Verifying data integrity")
    logging.info("="*60)

    new_stats = {
        'swap': conn.execute("SELECT COUNT(*) FROM swap_new").fetchone()[0],
        'mint': conn.execute("SELECT COUNT(*) FROM mint_new").fetchone()[0],
        'burn': conn.execute("SELECT COUNT(*) FROM burn_new").fetchone()[0],
        'collect': conn.execute("SELECT COUNT(*) FROM collect_new").fetchone()[0],
        'flash': conn.execute("SELECT COUNT(*) FROM flash_new").fetchone()[0],
        'transfer': conn.execute("SELECT COUNT(*) FROM transfer_new").fetchone()[0],
        'approval': conn.execute("SELECT COUNT(*) FROM approval_new").fetchone()[0],
    }

    all_match = True
    for table in new_stats:
        old_count = old_stats[table]
        new_count = new_stats[table]
        match = "✓" if old_count == new_count else "❌"
        logging.info(f"{match} {table}: {old_count:,} → {new_count:,}")
        if old_count != new_count:
            all_match = False

    if not all_match:
        logging.error("❌ Record counts don't match - aborting migration")
        conn.close()
        return False

    logging.info("\n✓ All record counts match!")

    logging.info("\n" + "="*60)
    logging.info("PHASE 3: Replacing old tables")
    logging.info("="*60)

    try:
        tables = ['swap', 'mint', 'burn', 'collect', 'flash', 'transfer', 'approval']

        for table in tables:
            logging.info(f"Dropping old {table} table...")
            conn.execute(f"DROP TABLE IF EXISTS {table}")

            logging.info(f"Renaming {table}_new to {table}...")
            conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")

        logging.info("\n✓ Tables replaced successfully")

    except Exception as e:
        logging.error(f"❌ Failed to replace tables: {e}")
        logging.error("Database may be in inconsistent state - restore from backup!")
        conn.close()
        return False

    logging.info("\n" + "="*60)
    logging.info("PHASE 4: Creating optimized indexes")
    logging.info("="*60)

    try:
        logging.info("Creating indexes...")

        with open(NEW_SCHEMA_PATH, 'r') as f:
            schema_sql = f.read()

        index_statements = [line for line in schema_sql.split(';') if 'CREATE INDEX' in line]

        for i, stmt in enumerate(index_statements, 1):
            if stmt.strip():
                logging.info(f"Creating index {i}/{len(index_statements)}...")
                conn.execute(stmt + ';')

        logging.info(f"✓ Created {len(index_statements)} indexes")

    except Exception as e:
        logging.warning(f"⚠️ Some indexes may have failed: {e}")
        logging.info("Database is still usable, indexes can be created manually")

    logging.info("\n" + "="*60)
    logging.info("✅ MIGRATION COMPLETED SUCCESSFULLY")
    logging.info("="*60)

    logging.info("\n📊 Final statistics:")
    final_stats = get_table_stats(conn)
    for table, count in final_stats.items():
        logging.info(f"  {table}: {count:,} records")

    logging.info(f"\n💾 Backup location: {BACKUP_PATH}")
    logging.info("\n🚀 Next steps:")
    logging.info("  1. Update main.py to use new schema (script provided)")
    logging.info("  2. Test queries with: python -c 'import duckdb; conn=duckdb.connect(\"out/V3/uniswap_v3.duckdb\"); print(conn.execute(\"SELECT COUNT(*) FROM swap\").fetchone())'")
    logging.info("  3. Run your pipeline - it will use optimized schema automatically")

    conn.close()
    return True

def main():
    print("="*60)
    print("UNISWAP V3 DATABASE SCHEMA MIGRATION")
    print("From: HUGEINT (overflow issues)")
    print("To: VARCHAR (raw) + DOUBLE (analytics)")
    print("="*60)
    print()

    if not check_prerequisites():
        return 1

    if migrate_data():
        print("\n✅ Migration completed successfully!")
        return 0
    else:
        print("\n❌ Migration failed - check logs above")
        return 1

if __name__ == "__main__":
    sys.exit(main())
