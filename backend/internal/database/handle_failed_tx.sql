-- update_failed_tx.sql

-- 🎯 1) Fill in your values here (no extra quotes):
\set deposit_id      84532816
\set wallet_address  0xab35888ebaa1c8381ab1202549bc90e0e39148fb
\set mon_amount      15000000000000000000
\set monad_tx_hash   0xe98df5f3fd5c1286f36b1af4067c7bf50af72e24c5a58ae857b42c2eb6d61721

BEGIN;

-- 2) Mark the deposit as processed
UPDATE deposits
   SET status = 'processed'
 WHERE deposit_id = :'deposit_id';

-- 3) Update the transaction_history to completed
UPDATE transaction_history
   SET status        = 'completed',
       mon_amount    = :'mon_amount',
       monad_tx_hash = :'monad_tx_hash',
       updated_at    = CURRENT_TIMESTAMP
 WHERE deposit_id = :'deposit_id';

-- 4) Insert (or update) the distributions record as completed
INSERT INTO distributions (
    deposit_id,
    wallet_address,
    mon_amount,
    status,
    monad_tx_hash,
    created_at,
    updated_at
)
VALUES (
    :'deposit_id',
    :'wallet_address',
    :'mon_amount',
    'completed',
    :'monad_tx_hash',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (deposit_id) DO UPDATE
   SET status        = EXCLUDED.status,
       mon_amount    = EXCLUDED.mon_amount,
       monad_tx_hash = EXCLUDED.monad_tx_hash,
       updated_at    = EXCLUDED.updated_at;

COMMIT;

\echo '✅ Updated deposit_id=' :'deposit_id'
\echo '   • transaction_history → completed'
\echo '   • distributions       → inserted/updated for wallet=' :'wallet_address'
