-- Migration: Create email deduplication schema
-- This migration creates the email_duplicate_groups table and adds the duplicate_group_id column to classified_emails

-- Step 1: Create the email_duplicate_groups table
CREATE TABLE IF NOT EXISTS email_duplicate_groups (
    id SERIAL PRIMARY KEY,
    content_fingerprint VARCHAR(64) NOT NULL UNIQUE,
    -- The ID of the first email found with this fingerprint
    primary_email_id INTEGER NOT NULL REFERENCES classified_emails(id),
    -- The total number of emails that share this fingerprint
    member_count INTEGER DEFAULT 1,
    first_seen TIMESTAMP WITH TIME ZONE,
    last_seen TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for fast fingerprint lookups
CREATE INDEX idx_duplicate_groups_fingerprint ON email_duplicate_groups(content_fingerprint);

-- Create index for finding groups by primary email
CREATE INDEX idx_duplicate_groups_primary_email ON email_duplicate_groups(primary_email_id);

-- Step 2: Add duplicate_group_id column to classified_emails
ALTER TABLE classified_emails
ADD COLUMN IF NOT EXISTS duplicate_group_id INTEGER
REFERENCES email_duplicate_groups(id) ON DELETE SET NULL;

-- Create index for fast duplicate group lookups
CREATE INDEX idx_classified_emails_duplicate_group_id ON classified_emails(duplicate_group_id);

-- Step 3: Add content_fingerprint column to classified_emails for efficient processing
ALTER TABLE classified_emails
ADD COLUMN IF NOT EXISTS content_fingerprint VARCHAR(64);

-- Create index for fingerprint lookups during backfill
CREATE INDEX idx_classified_emails_fingerprint ON classified_emails(content_fingerprint);

-- Step 4: Create a view for easy duplicate analysis
CREATE OR REPLACE VIEW email_duplicate_analysis AS
SELECT 
    edg.id as group_id,
    edg.content_fingerprint,
    edg.member_count,
    edg.first_seen,
    edg.last_seen,
    ce_primary.subject as primary_subject,
    ce_primary.sender_email as primary_sender,
    ce_primary.date_sent as primary_date,
    ARRAY_AGG(DISTINCT ce_all.sender_email) as all_senders,
    ARRAY_AGG(DISTINCT ce_all.subject) as all_subjects
FROM email_duplicate_groups edg
JOIN classified_emails ce_primary ON edg.primary_email_id = ce_primary.id
JOIN classified_emails ce_all ON ce_all.duplicate_group_id = edg.id
GROUP BY edg.id, edg.content_fingerprint, edg.member_count, edg.first_seen, edg.last_seen,
         ce_primary.subject, ce_primary.sender_email, ce_primary.date_sent
HAVING edg.member_count > 1
ORDER BY edg.member_count DESC;

-- Step 5: Create function to update timestamps
CREATE OR REPLACE FUNCTION update_duplicate_group_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for automatic timestamp updates
CREATE TRIGGER update_email_duplicate_groups_timestamp
BEFORE UPDATE ON email_duplicate_groups
FOR EACH ROW
EXECUTE FUNCTION update_duplicate_group_timestamp();

-- Step 6: Create function to update member count and timestamps
CREATE OR REPLACE FUNCTION update_duplicate_group_stats()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        -- Update member count and last_seen for the group
        UPDATE email_duplicate_groups
        SET 
            member_count = (
                SELECT COUNT(*) 
                FROM classified_emails 
                WHERE duplicate_group_id = NEW.duplicate_group_id
            ),
            last_seen = GREATEST(
                last_seen,
                (SELECT MAX(date_sent) 
                 FROM classified_emails 
                 WHERE duplicate_group_id = NEW.duplicate_group_id)
            ),
            updated_at = NOW()
        WHERE id = NEW.duplicate_group_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to maintain statistics
CREATE TRIGGER maintain_duplicate_group_stats
AFTER INSERT OR UPDATE OF duplicate_group_id ON classified_emails
FOR EACH ROW
WHEN (NEW.duplicate_group_id IS NOT NULL)
EXECUTE FUNCTION update_duplicate_group_stats();