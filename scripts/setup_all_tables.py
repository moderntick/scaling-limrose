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
DB_NAME = os.getenv("DB_NAME", "limrose_email_pipeline")
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

def create_email_pipeline_routes_table(cursor):
    """Create the email_pipeline_routes table."""
    print("Creating email_pipeline_routes table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_pipeline_routes (
            id SERIAL PRIMARY KEY,
            email_id INTEGER REFERENCES classified_emails(id),
            pipeline_type VARCHAR(50),
            priority_score FLOAT,
            status VARCHAR(20) DEFAULT 'pending',
            assigned_to VARCHAR(255),
            processing_notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_pipeline_routes_email ON email_pipeline_routes(email_id);
        CREATE INDEX IF NOT EXISTS idx_pipeline_routes_type ON email_pipeline_routes(pipeline_type);
        CREATE INDEX IF NOT EXISTS idx_pipeline_routes_status ON email_pipeline_routes(status);
    """)
    print("✓ email_pipeline_routes table created")

def create_email_classifications_table(cursor):
    """Create the email_classifications table."""
    print("Creating email_classifications table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_classifications (
            id SERIAL PRIMARY KEY,
            email_id INTEGER REFERENCES classified_emails(id),
            classification_type VARCHAR(50),
            confidence_score FLOAT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(email_id, classification_type)
        );

        CREATE INDEX IF NOT EXISTS idx_email_classifications_email ON email_classifications(email_id);
        CREATE INDEX IF NOT EXISTS idx_email_classifications_type ON email_classifications(classification_type);
    """)
    print("✓ email_classifications table created")

def create_pipeline_outcomes_table(cursor):
    """Create the pipeline_outcomes table."""
    print("Creating pipeline_outcomes table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_outcomes (
            id SERIAL PRIMARY KEY,
            email_id INTEGER REFERENCES classified_emails(id),
            pipeline_type VARCHAR(50),
            outcome_type VARCHAR(50),
            outcome_details JSONB,
            revenue_generated DECIMAL(10,2),
            articles_published INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_pipeline_outcomes_email ON pipeline_outcomes(email_id);
        CREATE INDEX IF NOT EXISTS idx_pipeline_outcomes_type ON pipeline_outcomes(outcome_type);
    """)
    print("✓ pipeline_outcomes table created")

def create_classification_performance_table(cursor):
    """Create the classification_performance table."""
    print("Creating classification_performance table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS classification_performance (
            id SERIAL PRIMARY KEY,
            classification_type VARCHAR(50),
            true_positives INTEGER DEFAULT 0,
            false_positives INTEGER DEFAULT 0,
            false_negatives INTEGER DEFAULT 0,
            precision_score FLOAT,
            recall_score FLOAT,
            f1_score FLOAT,
            last_updated TIMESTAMP DEFAULT NOW()
        );
    """)
    print("✓ classification_performance table created")

def create_enhanced_email_embeddings_table(cursor, dim):
    """Create the enhanced_email_embeddings table."""
    print("Creating enhanced_email_embeddings table...")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS enhanced_email_embeddings (
            id SERIAL PRIMARY KEY,
            email_id INTEGER REFERENCES classified_emails(id) ON DELETE CASCADE,
            gmail_id VARCHAR(255),
            embedding_type VARCHAR(50) NOT NULL,
            embedding VECTOR({dim}),
            embedding_text TEXT,

            thread_id VARCHAR(255),
            sender_email VARCHAR(255),
            pipeline_classification VARCHAR(50),
            sender_interaction_count INTEGER,
            thread_message_count INTEGER,

            includes_response BOOLEAN DEFAULT FALSE,
            includes_thread_context BOOLEAN DEFAULT FALSE,
            includes_sender_history BOOLEAN DEFAULT FALSE,
            includes_pipeline_context BOOLEAN DEFAULT FALSE,
            related_article_count INTEGER DEFAULT 0,

            search_keywords TEXT[],
            business_context TEXT,
            context_summary JSONB,

            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),

            UNIQUE(email_id, embedding_type)
        );

        CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_email ON enhanced_email_embeddings(email_id);
        CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_type ON enhanced_email_embeddings(embedding_type);
        CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_sender ON enhanced_email_embeddings(sender_email);
        CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_pipeline ON enhanced_email_embeddings(pipeline_classification);
        CREATE INDEX IF NOT EXISTS idx_enhanced_embeddings_vector ON enhanced_email_embeddings
            USING hnsw (embedding vector_cosine_ops);
    """)
    print("✓ enhanced_email_embeddings table created")

def create_sender_interaction_history_table(cursor):
    """Create the sender_interaction_history table."""
    print("Creating sender_interaction_history table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sender_interaction_history (
            id SERIAL PRIMARY KEY,
            sender_email VARCHAR(255),
            sender_name TEXT,

            total_emails_sent INTEGER DEFAULT 0,
            total_emails_responded INTEGER DEFAULT 0,
            total_emails_received INTEGER DEFAULT 0,
            first_contact_date TIMESTAMP WITH TIME ZONE,
            last_contact_date TIMESTAMP WITH TIME ZONE,

            relationship_type VARCHAR(50),
            interaction_quality VARCHAR(50),
            response_rate FLOAT,

            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),

            UNIQUE(sender_email)
        );

        CREATE INDEX IF NOT EXISTS idx_sender_history_email ON sender_interaction_history(sender_email);
        CREATE INDEX IF NOT EXISTS idx_sender_history_type ON sender_interaction_history(relationship_type);
    """)
    print("✓ sender_interaction_history table created")

def create_customer_issues_v2_table(cursor, dim):
    """Create the customer_issues_v2 table."""
    print("Creating customer_issues_v2 table...")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS customer_issues_v2 (
            id SERIAL PRIMARY KEY,
            email_id INTEGER REFERENCES classified_emails(id),
            thread_id VARCHAR(255),
            issue_type VARCHAR(100),
            issue_category VARCHAR(100),
            issue_summary TEXT,
            has_resolution BOOLEAN DEFAULT FALSE,
            resolution_summary TEXT,
            fix_instructions TEXT,
            issue_embedding VECTOR({dim}),
            resolution_embedding VECTOR({dim}),
            similarity_score FLOAT,
            based_on_issues INTEGER[],
            confidence_level VARCHAR(20),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_email ON customer_issues_v2(email_id);
        CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_thread ON customer_issues_v2(thread_id);
        CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_type ON customer_issues_v2(issue_type);
        CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_category ON customer_issues_v2(issue_category);
        CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_resolution ON customer_issues_v2(has_resolution);
        CREATE INDEX IF NOT EXISTS idx_customer_issues_v2_issue_vector ON customer_issues_v2
            USING hnsw (issue_embedding vector_cosine_ops);
    """)
    print("✓ customer_issues_v2 table created")

def main():
    """Main function to create all tables in correct order."""
    try:
        print(f"Connecting to database: {DB_NAME} as user: {DB_USER}")
        conn = psycopg.connect(POSTGRES_DSN)
        register_vector(conn)
        
        with conn.cursor() as cursor:
            # Create tables in dependency order
            # Core email tables
            create_classified_emails_table(cursor)
            create_email_fingerprints_table(cursor)
            create_email_duplicate_groups_table(cursor)
            create_customer_issues_table(cursor)
            create_parsed_emails_table(cursor)
            create_email_chunks_table(cursor, EMBEDDING_DIMENSION)

            # Pipeline routing tables
            create_email_pipeline_routes_table(cursor)
            create_email_classifications_table(cursor)
            create_pipeline_outcomes_table(cursor)
            create_classification_performance_table(cursor)

            # Enhanced embedding tables
            create_enhanced_email_embeddings_table(cursor, EMBEDDING_DIMENSION)
            create_sender_interaction_history_table(cursor)

            # Issue tracking v2
            create_customer_issues_v2_table(cursor, EMBEDDING_DIMENSION)

            conn.commit()
            print("\n✅ All tables created successfully!")
            print(f"   Total tables: 14 (core + pipeline + embeddings + issue tracking)")
            
    except Exception as e:
        print(f"\n❌ Error creating tables: {str(e)}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()