#!/usr/bin/env python3
# scripts/create_email_chunks_table.py
import sys
import os
import psycopg
from pgvector.psycopg import register_vector

# Configuration from environment or defaults
DB_NAME = os.getenv("DB_NAME", "limrose_email_pipeline")
DB_USER = os.getenv("DB_USER", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
POSTGRES_DSN = f"dbname={DB_NAME} user={DB_USER} host={DB_HOST} connect_timeout=30"
EMBEDDING_DIMENSION = 384

def main():
    """Creates the email_chunks table with proper constraints and thread metadata."""
    conn = psycopg.connect(POSTGRES_DSN)
    register_vector(conn)
    
    with conn.cursor() as cur:
        dim = EMBEDDING_DIMENSION
        print(f"Creating 'email_chunks' table with vector dimension {dim}...")
        
        # Create the email_chunks table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS email_chunks (
                id BIGSERIAL PRIMARY KEY,
                email_id INTEGER NOT NULL REFERENCES classified_emails(id) ON DELETE CASCADE,
                chunk_index INTEGER NOT NULL,
                chunk_type VARCHAR(50) DEFAULT 'body' CHECK (chunk_type IN ('body', 'quoted', 'signature', 'attachment')),
                text TEXT NOT NULL,
                embedding VECTOR({dim}) NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{{}}',
                created_at TIMESTAMP DEFAULT NOW(),
                
                -- Unique constraint to prevent duplicate chunks
                CONSTRAINT uk_email_chunk UNIQUE(email_id, chunk_index)
            );
        """)
        
        print("Creating indexes for performance...")
        
        # HNSW index for vector similarity search
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_email_chunks_embedding
            ON email_chunks
            USING hnsw (embedding vector_cosine_ops);
        """)
        
        # Regular indexes for common queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_email_chunks_email_id ON email_chunks(email_id);
            CREATE INDEX IF NOT EXISTS idx_email_chunks_type ON email_chunks(chunk_type);
            CREATE INDEX IF NOT EXISTS idx_email_chunks_created ON email_chunks(created_at);
            
            -- JSONB indexes for metadata queries
            CREATE INDEX IF NOT EXISTS idx_email_chunks_thread_id ON email_chunks((metadata->>'thread_id'));
            CREATE INDEX IF NOT EXISTS idx_email_chunks_sender ON email_chunks((metadata->>'sender_email'));
            CREATE INDEX IF NOT EXISTS idx_email_chunks_classification ON email_chunks((metadata->>'classification'));
        """)
        
        # Create a cleanup function for failed partial insertions
        cur.execute("""
            CREATE OR REPLACE FUNCTION cleanup_incomplete_email_chunks()
            RETURNS void AS $$
            BEGIN
                -- Delete chunks where not all expected chunks exist
                DELETE FROM email_chunks ec1
                WHERE EXISTS (
                    SELECT 1
                    FROM email_chunks ec2
                    WHERE ec2.email_id = ec1.email_id
                    GROUP BY ec2.email_id, (ec2.metadata->>'chunk_position')::text
                    HAVING 
                        -- Extract total expected chunks from 'X/Y' format
                        MAX(CAST(SPLIT_PART((ec2.metadata->>'chunk_position')::text, '/', 2) AS INTEGER)) > 
                        COUNT(DISTINCT ec2.chunk_index)
                );
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # Add chunks_created column to classified_emails if it doesn't exist
        cur.execute("""
            DO $$ 
            BEGIN 
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='classified_emails' AND column_name='chunks_created'
                ) THEN
                    ALTER TABLE classified_emails ADD COLUMN chunks_created BOOLEAN DEFAULT FALSE;
                END IF;
            END $$;
        """)
        
        # Create a trigger to maintain consistency
        cur.execute("""
            CREATE OR REPLACE FUNCTION update_email_chunks_status()
            RETURNS TRIGGER AS $$
            BEGIN
                -- When chunks are inserted, update the parent email
                UPDATE classified_emails 
                SET chunks_created = true,
                    embeddings_created = true
                WHERE id = NEW.email_id;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS email_chunks_status_trigger ON email_chunks;
            CREATE TRIGGER email_chunks_status_trigger
            AFTER INSERT ON email_chunks
            FOR EACH ROW
            EXECUTE FUNCTION update_email_chunks_status();
        """)
        
        conn.commit()
        print("✅ 'email_chunks' table, indexes, and cleanup functions are ready.")
        
        # Show table statistics
        cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM classified_emails) as total_emails,
                (SELECT COUNT(*) FROM classified_emails WHERE chunks_created = true) as chunked_emails,
                (SELECT COUNT(*) FROM email_chunks) as total_chunks;
        """)
        stats = cur.fetchone()
        print(f"\n📊 Current statistics:")
        print(f"   Total emails: {stats[0]}")
        print(f"   Chunked emails: {stats[1]}")
        print(f"   Total chunks: {stats[2]}")

if __name__ == "__main__":
    main()