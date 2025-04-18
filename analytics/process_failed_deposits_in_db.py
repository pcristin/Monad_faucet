#!/usr/bin/env python3
import os
import json
import argparse
from dotenv import load_dotenv
from web3 import Web3
import psycopg2
from psycopg2.extras import execute_batch

def load_env():
    load_dotenv()
    return {
        "RPC_URL":            os.getenv("MONAD_RPC_URL"),
        "CONTRACT_ADDRESS":   os.getenv("CONTRACT_ADDRESS", "0xc11350Fd29aC48181b0117bd1935dBE781cdd03d"),
        "ABI_PATH":           os.getenv("ABI_PATH", "distributor_abi.json"),
        "DATABASE_URL":       os.getenv("DATABASE_URL")
    }

def parse_args():
    p = argparse.ArgumentParser(
        description="Process a Monad distribution TX and update Postgres"
    )
    p.add_argument(
        "--tx", "-t", required=True,
        help="Transaction hash to fetch and process"
    )
    return p.parse_args()

def main():
    cfg = load_env()
    args = parse_args()

    # 1) Load ABI
    with open(cfg["ABI_PATH"], "r") as f:
        abi = json.load(f)

        # 2) Connect to RPC
    w3 = Web3(Web3.HTTPProvider(cfg["RPC_URL"]))

    # 2a) Precompute the Distribution event signature (topic0)
    DISTRO_SIG_HASH = w3.keccak(text="Distribution(address,uint256,uint256)").hex()

    # 2b) Whitelist your distributor address(es) via .env
    #    e.g. DISTRIBUTOR_ADDRESSES=0xAbc...,0xDef...
    allowed = {
        Web3.to_checksum_address(a.strip())
        for a in os.getenv("DISTRIBUTOR_ADDRESSES", "").split(",")
        if a.strip()
    }
    
    # If DISTRIBUTOR_ADDRESSES is not set, try to use MONAD_DISTRIBUTOR_ADDR as fallback
    if not allowed and os.getenv("MONAD_DISTRIBUTOR_ADDR"):
        allowed = {Web3.to_checksum_address(os.getenv("MONAD_DISTRIBUTOR_ADDR"))}
    
    if not allowed:
        print("⚠️  No distributor addresses configured. Set DISTRIBUTOR_ADDRESSES or MONAD_DISTRIBUTOR_ADDR in .env")
        return

    # 2c) Minimal ABI for decoding Distribution events
    distribution_abi = next(
        item for item in abi
        if item["type"] == "event" and item["name"] == "Distribution"
    )
    contract = w3.eth.contract(abi=[distribution_abi])


    # 3) Fetch receipt & decode events
    print(f"⏳ Fetching receipt for {args.tx}…")
    receipt = w3.eth.get_transaction_receipt(args.tx)
    print(f"✅ Found receipt with {len(receipt.logs)} logs")
    print(f"🔑 Looking for events from addresses: {', '.join(allowed)}")
    print(f"🔑 Event signature: {DISTRO_SIG_HASH}")
    
    evts = []
    for i, log in enumerate(receipt.logs):
        topic0 = log["topics"][0].hex() if log["topics"] else "None"
        log_addr = Web3.to_checksum_address(log["address"]) if log["address"] else "None"
        print(f"Log #{i}: from={log_addr}, topic0={topic0}")
        
        # 1) filter by the exact Distribution signature
        if log["topics"][0].hex() != DISTRO_SIG_HASH:
            print(f"  ❌ Skipping: signature mismatch")
            continue
        # 2) filter by allowed contract addresses
        if Web3.to_checksum_address(log["address"]) not in allowed:
            print(f"  ❌ Skipping: address not in allowed list")
            continue

        # 3) now safe to decode
        print(f"  ✅ Processing: signature and address match")
        ev = contract.events.Distribution().process_log(log)
        evts.append(ev)


    if not evts:
        print("⚠️  No Distribution events found in that TX.")
        return

    print(f"✔️  Found {len(evts)} distribution event(s). Preparing DB updates…")

    # 4) Open DB and run all updates in one transaction
    conn = psycopg2.connect(cfg["DATABASE_URL"])
    try:
        with conn:
            with conn.cursor() as cur:
                for ev in evts:
                    args_ = ev["args"]
                    deposit_id     = str(args_["id"])
                    wallet_address = args_["recipient"]
                    mon_amount     = str(args_["amount"])
                    monad_tx_hash  = args.tx  # from your script's --tx argument


                    # Update deposits table
                    cur.execute("""
                        UPDATE deposits
                           SET status = 'processed',
                               updated_at = CURRENT_TIMESTAMP
                         WHERE deposit_id = %s;
                    """, (deposit_id,))

                    # Update transaction_history
                    cur.execute("""
                        UPDATE transaction_history
                           SET status = 'completed',
                               mon_amount = %s,
                               monad_tx_hash = %s,
                               updated_at = CURRENT_TIMESTAMP
                         WHERE deposit_id = %s;
                    """, (mon_amount, monad_tx_hash, deposit_id))

                    # Upsert distributions
                    cur.execute("""
                        INSERT INTO distributions
                            (deposit_id, wallet_address, mon_amount, status, monad_tx_hash, created_at, updated_at)
                        VALUES
                            (%s, %s, %s, 'completed', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (deposit_id) DO UPDATE
                            SET status        = EXCLUDED.status,
                                mon_amount    = EXCLUDED.mon_amount,
                                monad_tx_hash = EXCLUDED.monad_tx_hash,
                                updated_at    = EXCLUDED.updated_at;
                    """, (deposit_id, wallet_address, mon_amount, monad_tx_hash))

        print(f"✅ Successfully processed {len(evts)} distribution(s).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
