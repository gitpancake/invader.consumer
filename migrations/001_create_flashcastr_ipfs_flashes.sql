-- Create flashcastr_ipfs_flashes table
-- This table stores IPFS metadata for flashes that have been migrated from S3
CREATE TABLE IF NOT EXISTS flashcastr_ipfs_flashes (
    id SERIAL PRIMARY KEY,
    flash_id INTEGER NOT NULL,
    ipfs_url VARCHAR(255) NOT NULL,
    ipfs_cid VARCHAR(255) NOT NULL,
    original_s3_key VARCHAR(255) NOT NULL,
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_size BIGINT,
    content_type VARCHAR(100),
    
    -- Foreign key constraint to flashes table
    CONSTRAINT fk_flash_id FOREIGN KEY (flash_id) REFERENCES flashes(flash_id),
    
    -- Ensure one record per flash
    CONSTRAINT unique_flash_id UNIQUE (flash_id)
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_flashcastr_ipfs_flash_id ON flashcastr_ipfs_flashes(flash_id);
CREATE INDEX IF NOT EXISTS idx_flashcastr_ipfs_cid ON flashcastr_ipfs_flashes(ipfs_cid);

-- Add comment for documentation
COMMENT ON TABLE flashcastr_ipfs_flashes IS 'Stores IPFS migration data for flashes, linking original S3 keys to IPFS URLs';