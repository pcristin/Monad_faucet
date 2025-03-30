-- First, remove duplicate records keeping only the most recent one
DELETE FROM transaction_history a 
USING (
    SELECT MAX(id) as max_id, deposit_id
    FROM transaction_history
    GROUP BY deposit_id
    HAVING COUNT(*) > 1
) b
WHERE a.deposit_id = b.deposit_id AND a.id < b.max_id;

-- Add unique constraint with compatibility check
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'deposit_id_unique_tx'
    ) THEN
        ALTER TABLE transaction_history 
        ADD CONSTRAINT deposit_id_unique_tx UNIQUE (deposit_id);
    END IF;
END $$; 