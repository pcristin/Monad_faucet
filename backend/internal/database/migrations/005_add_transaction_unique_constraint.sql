-- First, remove duplicate records keeping only the most recent one
DELETE FROM transaction_history a 
USING (
    SELECT MAX(id) as max_id, deposit_id
    FROM transaction_history
    GROUP BY deposit_id
    HAVING COUNT(*) > 1
) b
WHERE a.deposit_id = b.deposit_id AND a.id < b.max_id;

-- Add unique constraint on deposit_id
ALTER TABLE transaction_history 
ADD CONSTRAINT IF NOT EXISTS deposit_id_unique_tx UNIQUE (deposit_id); 