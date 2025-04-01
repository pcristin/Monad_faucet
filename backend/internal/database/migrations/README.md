# Database Migrations

This directory contains manual migration scripts that can be run if the automatic migrations in the application fail.

## Migration: Add Source Chain Support

The migration in `add_source_chain_columns.sql` adds the necessary database columns to support multiple L2 networks (Arbitrum, Base, Optimism) in the bridge system.

This migration:
1. Adds a `source_chain` column to the `transaction_history` table
2. Adds a `source_chain` column to the `deposits` table (if it exists)
3. Creates indexes for more efficient lookups

### Running the Migration Manually

#### Option 1: Using the Shell Script

Make the script executable:
```
chmod +x run_migration.sh
```

Then run it with your database parameters:
```
./run_migration.sh -h hostname -p port -d database_name -u username
```

The script will prompt for a password if not provided with the `-w` flag.

#### Option 2: Running SQL Directly

You can also run the SQL script directly using the `psql` command:
```
psql -h hostname -p port -U username -d database_name -f add_source_chain_columns.sql
```

Or connect to your database using any PostgreSQL client and run the contents of the SQL file.

## Verifying the Migration

After running the migration, you can verify the changes with the following SQL commands:

```sql
-- Check if source_chain column exists in transaction_history
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'transaction_history' AND column_name = 'source_chain';

-- Check if source_chain column exists in deposits
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'deposits' AND column_name = 'source_chain';

-- Check if indexes were created
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'transaction_history' 
  AND indexname = 'idx_transaction_history_deposit_id_source_chain';

SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'deposits' 
  AND indexname = 'idx_deposits_deposit_id_source_chain';
``` 