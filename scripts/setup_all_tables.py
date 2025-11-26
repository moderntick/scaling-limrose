#!/usr/bin/env python3
"""
Setup all database tables in the correct order.
This script ensures all tables are created with proper foreign key dependencies.
"""
import os
import sys
import psycopg
from pgvector.psycopg import register_vector

# Configuration
DB_NAME = os.getenv("DB_NAME", "email_pipeline")
DB_USER = os.getenv("DB_USER", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
POSTGRES_DSN = f"dbname={DB_NAME} user={DB_USER} host={DB_HOST} connect_timeout=30"
EMBEDDING_DIMENSION = 384

def create_classified_emails_table(cursor):
    """Create the main classified_emails table."""
    print("Creating classified_emails table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS classified_emails (
            id SERIAL PRIMARY KEY,
            gmail_id VARCHAR(255) UNIQUE NOT NULL,
            thread_id VARCHAR(255),
            subject TEXT,
            sender_email VARCHAR(255),
            sender_name TEXT,
            recipient_emails TEXT[],
            cc_emails TEXT[],
            bcc_emails TEXT[],
            date_sent TIMESTAMP WITH TIME ZONE,
            date_received TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            body_text TEXT,
            body_html TEXT,
            normalized_body_text TEXT,
            normalized_body_html TEXT,
            snippet TEXT,
            labels TEXT[],
            has_attachments BOOLEAN DEFAULT FALSE,
            attachment_count INTEGER DEFAULT 0,
            importance_score FLOAT,
            processed BOOLEAN DEFAULT FALSE,
            raw_size INTEGER,
            
            -- Email headers for threading
            message_id TEXT,
            in_reply_to TEXT,
            "references" TEXT[],
            
            -- Deduplication fields
            content_fingerprint VARCHAR(64),
            duplicate_group_id INTEGER,
            normalization_version INTEGER DEFAULT 2,
            
            -- Pipeline integration fields
            pipeline_processed BOOLEAN DEFAULT FALSE,
            embeddings_created BOOLEAN DEFAULT FALSE,
            enhanced_embedding_created BOOLEAN DEFAULT FALSE,
            human_verified BOOLEAN DEFAULT FALSE,
            
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_classified_emails_gmail_id ON classified_emails(gmail_id);
        CREATE INDEX IF NOT EXISTS idx_classified_emails_thread ON classified_emails(thread_id);
        CREATE INDEX IF NOT EXISTS idx_classified_emails_date ON classified_emails(date_sent);
        CREATE INDEX IF NOT EXISTS idx_classified_emails_sender ON classified_emails(sender_email);
        CREATE INDEX IF NOT EXISTS idx_classified_emails_processed ON classified_emails(pipeline_processed);
        CREATE INDEX IF NOT EXISTS idx_classified_emails_fingerprint ON classified_emails(content_fingerprint);
        CREATE INDEX IF NOT EXISTS idx_classified_emails_duplicate_group ON classified_emails(duplicate_group_id);
    """)
    print("✓ classified_emails table created")

def create_email_chunks_table(cursor, dim):
    """Create the email_chunks table."""
    print("Creating email_chunks table...")
    cursor.execute(f"""
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
        
        CREATE INDEX IF NOT EXISTS idx_email_chunks_embedding
        ON email_chunks
        USING hnsw (embedding vector_cosine_ops);
        
        CREATE INDEX IF NOT EXISTS idx_email_chunks_email_id ON email_chunks(email_id);
        CREATE INDEX IF NOT EXISTS idx_email_chunks_chunk_type ON email_chunks(chunk_type);
        CREATE INDEX IF NOT EXISTS idx_email_chunks_created_at ON email_chunks(created_at);
        CREATE INDEX IF NOT EXISTS idx_email_chunks_metadata ON email_chunks USING gin(metadata);
    """)
    print("✓ email_chunks table created")

def create_email_fingerprints_table(cursor):
    """Create the email_fingerprints_v2 table."""
    print("Creating email_fingerprints_v2 table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_fingerprints_v2 (
            email_id INTEGER PRIMARY KEY REFERENCES classified_emails(id) ON DELETE CASCADE,
            new_content_hash VARCHAR(64),
            quoted_content_hash VARCHAR(64),
            full_content_hash VARCHAR(64),
            structure_hash VARCHAR(64),
            thread_hash VARCHAR(64),
            recipient_set_hash VARCHAR(64),
            has_meaningful_new_content BOOLEAN DEFAULT TRUE,
            new_content_intent VARCHAR(50),
            email_type VARCHAR(20) DEFAULT 'original',
            parsing_confidence FLOAT DEFAULT 1.0,
            is_canonical BOOLEAN DEFAULT TRUE,
            canonical_email_id INTEGER,
            fingerprint_version INTEGER DEFAULT 5,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_fingerprints_v2_full_content ON email_fingerprints_v2(full_content_hash);
        CREATE INDEX IF NOT EXISTS idx_fingerprints_v2_structure ON email_fingerprints_v2(structure_hash);
        CREATE INDEX IF NOT EXISTS idx_fingerprints_v2_composite ON email_fingerprints_v2(full_content_hash, structure_hash);
    """)
    print("✓ email_fingerprints_v2 table created")

def create_email_duplicate_groups_table(cursor):
    """Create the email_duplicate_groups table."""
    print("Creating email_duplicate_groups table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_duplicate_groups (
            id SERIAL PRIMARY KEY,
            content_fingerprint VARCHAR(64),
            primary_email_id INTEGER REFERENCES classified_emails(id),
            member_count INTEGER DEFAULT 1,
            first_seen TIMESTAMP WITH TIME ZONE,
            last_seen TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            normalization_version INTEGER DEFAULT 5
        );
        
        CREATE INDEX IF NOT EXISTS idx_duplicate_groups_fingerprint ON email_duplicate_groups(content_fingerprint);
        CREATE INDEX IF NOT EXISTS idx_duplicate_groups_primary ON email_duplicate_groups(primary_email_id);
    """)
    print("✓ email_duplicate_groups table created")

def create_customer_issues_table(cursor):
    """Create the customer_issues table."""
    print("Creating customer_issues table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_issues (
            id SERIAL PRIMARY KEY,
            email_id INTEGER REFERENCES classified_emails(id),
            customer_email VARCHAR(255) NOT NULL,
            customer_name VARCHAR(255),
            issue_description TEXT,
            urgency_score FLOAT CHECK (urgency_score >= 0 AND urgency_score <= 1),
            sentiment_score FLOAT CHECK (sentiment_score >= -1 AND sentiment_score <= 1),
            category VARCHAR(100),
            subcategory VARCHAR(100),
            product_name VARCHAR(255),
            order_number VARCHAR(100),
            priority VARCHAR(20) CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
            status VARCHAR(50) DEFAULT 'new',
            thread_id VARCHAR(255),
            related_emails INTEGER[],
            extracted_entities JSONB,
            ai_summary TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_customer_issues_email ON customer_issues(customer_email);
        CREATE INDEX IF NOT EXISTS idx_customer_issues_status ON customer_issues(status);
        CREATE INDEX IF NOT EXISTS idx_customer_issues_priority ON customer_issues(priority);
        CREATE INDEX IF NOT EXISTS idx_customer_issues_created ON customer_issues(created_at);
        CREATE INDEX IF NOT EXISTS idx_customer_issues_thread ON customer_issues(thread_id);
    """)
    print("✓ customer_issues table created")

def create_parsed_emails_table(cursor):
    """Create the parsed_emails table."""
    print("Creating parsed_emails table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS parsed_emails (
            id SERIAL PRIMARY KEY,
            email_id INTEGER REFERENCES classified_emails(id) ON DELETE CASCADE,
            new_content TEXT,
            quoted_content TEXT,
            quote_headers TEXT[],
            parsing_method VARCHAR(50),
            confidence_score FLOAT DEFAULT 1.0,
            metadata JSONB DEFAULT '{}',
            created_at TIMESTAMP DEFAULT NOW(),
            
            CONSTRAINT uk_parsed_email UNIQUE(email_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_parsed_emails_email_id ON parsed_emails(email_id);
        CREATE INDEX IF NOT EXISTS idx_parsed_emails_method ON parsed_emails(parsing_method);
    """)
    print("✓ parsed_emails table created")

def main():
    """Main function to create all tables in correct order."""
    try:
        print(f"Connecting to database: {DB_NAME} as user: {DB_USER}")
        conn = psycopg.connect(POSTGRES_DSN)
        register_vector(conn)
        
        with conn.cursor() as cursor:
            # Create tables in dependency order
            create_classified_emails_table(cursor)
            create_email_fingerprints_table(cursor)
            create_email_duplicate_groups_table(cursor)
            create_customer_issues_table(cursor)
            create_parsed_emails_table(cursor)
            create_email_chunks_table(cursor, EMBEDDING_DIMENSION)
            
            conn.commit()
            print("\n✅ All tables created successfully!")
            
    except Exception as e:
        print(f"\n❌ Error creating tables: {str(e)}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()