-- SAFELY add ipfs_cid column to existing flashes table
-- This is a non-breaking change that adds a nullable column

BEGIN;

-- Add the column as nullable (safe for existing data)
ALTER TABLE flashes 
ADD COLUMN IF NOT EXISTS ipfs_cid VARCHAR(255);

-- Add an index for faster lookups (optional but recommended)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_flashes_ipfs_cid 
ON flashes(ipfs_cid) 
WHERE ipfs_cid IS NOT NULL;

-- Add a comment for documentation
COMMENT ON COLUMN flashes.ipfs_cid IS 'IPFS Content Identifier hash for the image, populated when uploaded to IPFS';

COMMIT;

-- Verify the change (informational)
SELECT column_name, data_type, is_nullable, column_default 
FROM information_schema.columns 
WHERE table_name = 'flashes' 
AND column_name = 'ipfs_cid';