#!/bin/bash
#
# Email Update Pipeline V2 - With Classification & Entity Intelligence
# 
# This script runs the complete email update pipeline:
# 1. Extracts new emails from Gmail using service account
# 2. Creates chunks and basic embeddings for RAG search
# 3. Classifies emails using LLM (Gemini Flash) and creates enhanced embeddings
# 4. Extracts entities using SpaCy NER with duplicate prevention
# 5. Extracts email participants for intelligence system
#
# The pipeline includes:
# - Email deduplication and fingerprinting
# - Chunk creation with embeddings (all-MiniLM-L6-v2)
# - Email classification into pipelines (journalism, newsletter, sales, etc.)
# - Enhanced embeddings with classification context
# - Entity extraction with database constraints and duplicate prevention
# - Entity disambiguation and alias management
# - Real-time entity system health monitoring
# - RAG system integration
#
# Usage:
#   ./update_emails_v2.sh                           # Extract all new emails
#   ./update_emails_v2.sh --start-date 2024/01/01   # Extract emails from specific date
#   ./update_emails_v2.sh --max-results 100         # Limit number of emails
#   ./update_emails_v2.sh --setup                   # Run initial setup

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if running setup
if [[ "$1" == "--setup" ]]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}Email Pipeline Initial Setup${NC}"
    echo -e "${GREEN}========================================${NC}\n"
    
    # Check Python
    echo -e "${YELLOW}Checking Python installation...${NC}"
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}Error: Python 3 is not installed${NC}"
        echo "Please install Python 3.8 or higher"
        exit 1
    fi
    python3 --version
    
    # Create virtual environment
    if [ ! -d "venv" ]; then
        echo -e "\n${YELLOW}Creating virtual environment...${NC}"
        python3 -m venv venv
    fi
    
    # Activate virtual environment
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source venv/bin/activate
    
    # Install dependencies
    echo -e "\n${YELLOW}Installing Python dependencies...${NC}"
    pip install --upgrade pip
    pip install -r requirements.txt
    
    # Check PostgreSQL
    echo -e "\n${YELLOW}Checking PostgreSQL...${NC}"
    if ! command -v psql &> /dev/null; then
        echo -e "${RED}Error: PostgreSQL is not installed${NC}"
        echo "Please install PostgreSQL 17 with pgvector extension"
        echo "On macOS: brew install postgresql@17 pgvector"
        echo "On Ubuntu: apt-get install postgresql-17 postgresql-17-pgvector"
        exit 1
    fi
    psql --version
    
    # Check Redis
    echo -e "\n${YELLOW}Checking Redis...${NC}"
    if ! command -v redis-cli &> /dev/null; then
        echo -e "${YELLOW}Warning: Redis is not installed${NC}"
        echo "Redis is optional but recommended for background tasks"
        echo "On macOS: brew install redis"
        echo "On Ubuntu: apt-get install redis-server"
    else
        redis-cli --version
    fi
    
    # Create .env file if it doesn't exist
    if [ ! -f ".env" ]; then
        echo -e "\n${YELLOW}Creating .env file from template...${NC}"
        cp .env.example .env
        echo -e "${GREEN}Created .env file. Please edit it with your configuration:${NC}"
        echo "  - SERVICE_ACCOUNT_FILE: Path to your Gmail service account JSON"
        echo "  - DELEGATE_EMAIL: Email address to access"
        echo "  - LLM_API_KEY: Your Gemini API key"
        echo "  - DB_NAME, DB_USER: Database configuration"
    fi
    
    # Load environment variables
    if [ -f ".env" ]; then
        export $(grep -v '^#' .env | xargs)
    fi
    
    # Create database
    echo -e "\n${YELLOW}Setting up PostgreSQL database...${NC}"
    DB_NAME=${DB_NAME:-email_pipeline}
    DB_USER=${DB_USER:-postgres}
    
    # Check if database exists
    if psql -U $DB_USER -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; then
        echo "Database '$DB_NAME' already exists"
    else
        echo "Creating database '$DB_NAME'..."
        createdb -U $DB_USER $DB_NAME
        if [ $? -ne 0 ]; then
            echo -e "${RED}Error: Failed to create database${NC}"
            echo "You may need to run: sudo -u postgres createdb $DB_NAME"
            exit 1
        fi
    fi
    
    # Create pgvector extension
    echo "Creating pgvector extension..."
    psql -U $DB_USER -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS vector;"
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: Failed to create pgvector extension${NC}"
        echo "Make sure pgvector is installed"
        exit 1
    fi
    
    # Create initial tables
    echo -e "\n${YELLOW}Creating database tables...${NC}"
    python scripts/create_email_chunks_table.py
    
    # Download models
    echo -e "\n${YELLOW}Downloading ML models (this may take a few minutes)...${NC}"
    python -c "
from sentence_transformers import SentenceTransformer
print('Downloading sentence transformer model...')
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
print('Model downloaded successfully!')
"
    
    # Check service account file
    echo -e "\n${YELLOW}Checking Gmail service account...${NC}"
    if [ -z "$SERVICE_ACCOUNT_FILE" ] || [ ! -f "$SERVICE_ACCOUNT_FILE" ]; then
        echo -e "${RED}Error: Service account file not found${NC}"
        echo "Please:"
        echo "1. Create a service account in Google Cloud Console"
        echo "2. Enable Gmail API"
        echo "3. Download the service account key JSON"
        echo "4. Update SERVICE_ACCOUNT_FILE in .env"
        exit 1
    else
        echo -e "${GREEN}Service account file found${NC}"
    fi
    
    # Final instructions
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}Setup Complete!${NC}"
    echo -e "${GREEN}========================================${NC}\n"
    echo -e "${YELLOW}Next steps:${NC}"
    echo "1. Edit .env file with your configuration"
    echo "2. Ensure your service account has Gmail delegation permissions"
    echo "3. Run the pipeline: ./update_emails_v2.sh"
    echo ""
    echo -e "${BLUE}For more help, see README.md${NC}"
    exit 0
fi

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
    echo "Loading environment variables from .env file..."
    export $(grep -v '^#' .env | xargs)
fi

# Check required environment variables
check_environment() {
    echo -e "${YELLOW}Checking configuration...${NC}"
    
    # Check for .env file
    if [ ! -f ".env" ]; then
        echo -e "${RED}Error: .env file not found${NC}"
        echo "Run './update_emails_v2.sh --setup' to create initial configuration"
        exit 1
    fi
    
    # Check required variables
    if [ -z "$SERVICE_ACCOUNT_FILE" ]; then
        echo -e "${RED}Error: SERVICE_ACCOUNT_FILE not set${NC}"
        echo "Please set SERVICE_ACCOUNT_FILE in .env"
        exit 1
    fi
    
    if [ ! -f "$SERVICE_ACCOUNT_FILE" ]; then
        echo -e "${RED}Error: Service account file not found at: $SERVICE_ACCOUNT_FILE${NC}"
        exit 1
    fi
    
    if [ -z "$DELEGATE_EMAIL" ]; then
        echo -e "${RED}Error: DELEGATE_EMAIL not set${NC}"
        echo "Please set DELEGATE_EMAIL in .env"
        exit 1
    fi
    
    if [ -z "$LLM_API_KEY" ]; then
        echo -e "${RED}Error: LLM_API_KEY not set${NC}"
        echo "Please set your Gemini API key in .env"
        exit 1
    fi
    
    echo -e "${GREEN}Configuration OK${NC}"
}

# Ensure services are running for entity pipeline
check_services() {
    # Check Redis
    if ! redis-cli ping > /dev/null 2>&1; then
        echo -e "${YELLOW}Warning: Redis is not running. Starting Redis...${NC}"
        if command -v brew &> /dev/null; then
            brew services start redis
        else
            sudo systemctl start redis
        fi
        sleep 2
    fi
    
    # Check PostgreSQL
    PSQL=$(which psql 2>/dev/null || echo "/usr/local/Cellar/postgresql@17/17.5/bin/psql")
    DB_NAME=${DB_NAME:-email_pipeline}
    if ! $PSQL -d $DB_NAME -c "SELECT 1" > /dev/null 2>&1; then
        echo -e "${RED}Error: PostgreSQL is not running or database '$DB_NAME' not accessible${NC}"
        echo "Please ensure PostgreSQL is running and database exists"
        echo "Run './update_emails_v2.sh --setup' if you haven't set up the database"
        exit 1
    fi
}

# Check environment before starting
check_environment

# Check services before starting
check_services

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Email Update Pipeline V2${NC}"
echo -e "${GREEN}With Classification & Entity Intelligence${NC}"
echo -e "${GREEN}========================================${NC}"

# Step 1: Extract new emails from Gmail
echo -e "\n${YELLOW}Step 1: Extracting new emails from Gmail...${NC}"
# Debug: Check if environment variables are set
echo "DEBUG: HF_HUB_OFFLINE=$HF_HUB_OFFLINE"
python gmail_service_account_extractor_with_dedup.py "$@"

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Gmail extraction failed!${NC}"
    exit 1
fi

# Step 2: Create chunks and embeddings from extracted emails
echo -e "\n${YELLOW}Step 2: Creating chunks and embeddings from emails...${NC}"
echo "This will chunk emails and generate embeddings for RAG search"

# Run batch processing to create email_chunks with embeddings
python batch_process_all_emails.py

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Email chunking and embedding failed!${NC}"
    exit 1
fi

# Step 3: Classify emails using LLM
echo -e "\n${YELLOW}Step 3: Classifying emails into pipelines...${NC}"
echo "Using Gemini Flash to classify emails..."
echo "Classifications: journalism, newsletter, sales, story leads, etc."
echo "This also creates enhanced embeddings with classification context"

# Run classifier to process ALL unclassified emails in batches
python batch_llm_classifier_optimized.py --all --batch-size 50

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Email classification failed!${NC}"
    echo "Note: This step requires Gemini API key to be configured"
    exit 1
fi

# Step 4: Run the BULLETPROOF entity extraction and disambiguation system
echo -e "\n${YELLOW}Step 4: Running BULLETPROOF entity extraction system...${NC}"
echo "This will:"
echo "  - Extract entities using SpaCy NER with duplicate prevention"
echo "  - Use database constraints to prevent duplicate entity mentions"
echo "  - Process with idempotent operations and parameter versioning"
echo "  - Build entity aliases and identify collisions"
echo "  - Setup disambiguation system with robust state tracking"

# Note: The new entity system doesn't use Celery, but keep workers running for other tasks
# ensure_celery_workers is still available if needed for article processing

# Set environment for offline mode
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1

# Run the FIXED incremental entity system - bulletproof duplicate prevention
# This uses the Phase 4.2 robust incremental extractor with constraint protection
echo -e "${GREEN}Running BULLETPROOF entity extraction with duplicate prevention...${NC}"
echo "Features: Idempotent processing, constraint protection, parameter versioning"
python entity_extraction/build_entity_system_incremental_fixed.py --emails-only

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Entity extraction and disambiguation failed!${NC}"
    exit 1
fi

# Step 4b: Entity inheritance for duplicate chunks
echo -e "\n${YELLOW}Step 4b: Running entity inheritance for duplicate chunks...${NC}"
echo "This ensures all duplicate chunks inherit entities from their originals"
echo "Provides complete entity location tracking across all email content"

python entity_inheritance_final_fix.py

if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Warning: Entity inheritance had issues${NC}"
    echo "Note: This is supplemental - core entity extraction completed successfully"
    echo "Manual inheritance can be run later: python entity_inheritance_final_fix.py"
    # Don't exit - inheritance issues shouldn't stop the pipeline
fi

# Step 5: Extract people entities from email senders (if this is still needed)
echo -e "\n${YELLOW}Step 5: Extracting people entities from email senders...${NC}"
echo "This will extract and disambiguate people mentioned in email sender fields"

# Check if this script exists and is still needed
if [ -f "entity_extraction/extract_email_people_entities.py" ]; then
    python entity_extraction/extract_email_people_entities.py
    
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}Warning: Email people entity extraction had issues${NC}"
        # Don't exit - this might be supplemental
    fi
else
    echo -e "${YELLOW}Skipping email people extraction (script not found)${NC}"
fi

# Step 6: Extract email participants for intelligence system
echo -e "\n${YELLOW}Step 6: Extracting email participants for intelligence system...${NC}"
echo "This will:"
echo "  - Extract participants (sender, recipients, CC, BCC) from new emails"
echo "  - Generate participant fingerprints for cross-email linking"
echo "  - Link participants to sender_profiles for enhanced intelligence"
echo "  - Enable Complete Participant Intelligence API functionality"

# Run incremental participant extraction for new emails
python extract_email_participants.py --incremental

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Email participant extraction failed!${NC}"
    echo "Note: This may impact participant intelligence features"
    echo "Manual fix: python extract_email_participants.py --incremental"
    exit 1
fi

# Validate extraction results
echo -e "${GREEN}Validating participant extraction...${NC}"
python -c "
import psycopg2
import os
conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME', 'email_pipeline'), 
    user=os.getenv('DB_USER', 'postgres'), 
    host=os.getenv('DB_HOST', 'localhost')
)
cursor = conn.cursor()

# Check for emails missing participants
cursor.execute('''
    SELECT COUNT(*) FROM classified_emails ce
    LEFT JOIN email_participants ep ON ce.id = ep.email_id
    WHERE ep.email_id IS NULL
''')
missing = cursor.fetchone()[0]

if missing > 10:
    print(f'❌ WARNING: {missing} emails missing participants')
    exit(1)
else:
    print(f'✅ Participant extraction healthy: {missing} emails missing participants')

conn.close()
"

if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Warning: Participant extraction validation failed${NC}"
    echo "System may need manual participant extraction review"
    # Don't exit - allow pipeline to continue
fi

# Step 7: Process emails through LangChain Agent System
echo -e "\n${YELLOW}Step 7: Running LangChain Agent System...${NC}"
echo "This will:"
echo "  - Analyze emails with AI agents (Triage, Journalist, Sales)"
echo "  - Generate actionable insights and recommendations"
echo "  - Create draft responses and follow-ups"
echo "  - Flag urgent items for immediate attention"

# Check if OPENAI_API_KEY is set
if [ -z "$OPENAI_API_KEY" ]; then
    echo -e "${YELLOW}Warning: OPENAI_API_KEY not set. Agent processing will be skipped.${NC}"
    echo "To enable agent processing, set your OpenAI API key:"
    echo "  export OPENAI_API_KEY='your-api-key'"
else
    # Run agent processing
    python run_agent_processing.py --batch-size 50
    
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}Warning: Agent processing encountered issues${NC}"
        echo "This is non-critical - emails have been processed successfully otherwise"
        # Don't exit - agent processing is supplemental
    else
        # Show agent processing stats
        echo -e "\n${BLUE}Agent Processing Summary:${NC}"
        python run_agent_processing.py --stats
    fi
fi

# Success
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Email update pipeline completed successfully!${NC}"
echo -e "${GREEN}========================================${NC}"

# Show comprehensive summary statistics
echo -e "\n${YELLOW}Summary:${NC}"
python -c "
import sys
import os
try:
    import psycopg2
    from datetime import datetime, timedelta
    
    # Connect to database
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME', 'email_pipeline'),
        user=os.getenv('DB_USER', 'postgres'),
        host=os.getenv('DB_HOST', 'localhost')
    )
    cur = conn.cursor()

    # Total emails
    cur.execute('SELECT COUNT(*) FROM classified_emails')
    total_emails = cur.fetchone()[0]

    # Recent emails (last 24 hours)
    cur.execute('''
        SELECT COUNT(*) FROM classified_emails 
        WHERE created_at >= NOW() - INTERVAL '24 hours'
    ''')
    recent_emails = cur.fetchone()[0]

    # Email classifications
    cur.execute('''
        SELECT pipeline_type, COUNT(DISTINCT email_id) as count
        FROM email_pipeline_routes
        GROUP BY pipeline_type
        ORDER BY count DESC
        LIMIT 10
    ''')
    classifications = cur.fetchall()

    # Emails with embeddings
    cur.execute('SELECT COUNT(DISTINCT email_id) FROM email_chunks')
    emails_with_chunks = cur.fetchone()[0]
    
    # Enhanced embeddings
    cur.execute('SELECT COUNT(DISTINCT email_id) FROM enhanced_email_embeddings')
    emails_with_enhanced = cur.fetchone()[0]

    # Emails with entities - check from entity_mentions_v2
    cur.execute('''
        SELECT COUNT(DISTINCT em.chunk_id) 
        FROM entity_mentions_v2 em
        JOIN email_chunks ec ON em.chunk_id = ec.id
    ''')
    emails_with_entities = cur.fetchone()[0]

    # Total entities found
    cur.execute('''
        SELECT COUNT(DISTINCT entity_hash) 
        FROM entities_v2
    ''')
    unique_entities = cur.fetchone()[0]

    # Recent entity mentions
    cur.execute('''
        SELECT COUNT(*) 
        FROM entity_mentions_v2
        WHERE extracted_at >= NOW() - INTERVAL '24 hours'
    ''')
    recent_mentions = cur.fetchone()[0]

    # Participant intelligence statistics
    cur.execute('''
        SELECT 
            COUNT(DISTINCT email_id) as emails_with_participants,
            COUNT(*) as total_participant_records,
            COUNT(DISTINCT participant_fingerprint) as unique_participants
        FROM email_participants
    ''')
    participant_stats = cur.fetchone()

    # Participant linkage quality
    cur.execute('''
        SELECT
            COUNT(*) FILTER (WHERE sp.fingerprint IS NOT NULL) as linked_to_profiles,
            COUNT(*) as total_participants,
            ROUND(100.0 * COUNT(*) FILTER (WHERE sp.fingerprint IS NOT NULL) / COUNT(*), 1) as linkage_rate
        FROM email_participants ep
        LEFT JOIN sender_profiles sp ON ep.participant_fingerprint = sp.fingerprint
    ''')
    linkage_stats = cur.fetchone()

    # Entity type breakdown (using new schema)
    cur.execute('''
        SELECT 
            entity_type,
            COUNT(DISTINCT entity_hash) as count
        FROM entities_v2
        GROUP BY entity_type
        ORDER BY count DESC
    ''')
    entity_types = cur.fetchall()
    
    # Collision statistics
    cur.execute('''
        SELECT 
            COUNT(*) as total_entities,
            COUNT(CASE WHEN has_collision = TRUE THEN 1 END) as collision_entities
        FROM entities_v2
    ''')
    entity_stats = cur.fetchone()
    
    # Entity alias statistics
    cur.execute('''
        SELECT COUNT(*) FROM entity_aliases_v2
    ''')
    alias_count = cur.fetchone()[0]

    # Display statistics
    print(f'Total emails: {total_emails:,}')
    print(f'New emails (24h): {recent_emails:,}')
    
    if total_emails > 0:
        print(f'Emails with embeddings: {emails_with_chunks:,} ({emails_with_chunks/total_emails*100:.1f}%)')
        print(f'Enhanced embeddings: {emails_with_enhanced:,} ({emails_with_enhanced/total_emails*100:.1f}%)')
        print(f'Emails with entities: {emails_with_entities:,} ({emails_with_entities/total_emails*100:.1f}%)')
    else:
        print(f'Emails with embeddings: {emails_with_chunks:,}')
        print(f'Enhanced embeddings: {emails_with_enhanced:,}')
        print(f'Emails with entities: {emails_with_entities:,}')
    
    print(f'\\nEmail Classifications:')
    if classifications:
        for pipeline_type, count in classifications[:5]:
            print(f'  {pipeline_type}: {count:,}')
    else:
        print('  No classifications yet')
    
    print(f'\\nEntity Intelligence:')
    print(f'  Unique entities found: {unique_entities:,}')
    print(f'  Entity mentions (24h): {recent_mentions:,}')
    print(f'  Entities with collisions: {entity_stats[1]:,}')
    print(f'  Entity aliases: {alias_count:,}')

    print(f'\\nParticipant Intelligence:')
    print(f'  Emails with participants: {participant_stats[0]:,}')
    print(f'  Total participant records: {participant_stats[1]:,}')
    print(f'  Unique participants: {participant_stats[2]:,}')
    print(f'  Profile linkage rate: {linkage_stats[2]}%')
    
    print(f'\\nEntity Types:')
    if entity_types:
        for entity_type, count in entity_types[:5]:
            print(f'  {entity_type}: {count:,}')
    else:
        print('  No entity types found')
    
    # Agent processing statistics
    cur.execute('''
        SELECT 
            COUNT(CASE WHEN agent_processing_status = 'completed' THEN 1 END) as processed,
            COUNT(CASE WHEN agent_processing_status = 'pending' THEN 1 END) as pending,
            COUNT(CASE WHEN agent_processing_status = 'error' THEN 1 END) as errors
        FROM classified_emails
        WHERE agent_processing_status IS NOT NULL
    ''')
    agent_stats = cur.fetchone()
    
    if agent_stats and (agent_stats[0] or agent_stats[1] or agent_stats[2]):
        print(f'\\nAgent Processing:')
        print(f'  Processed: {agent_stats[0]:,}')
        print(f'  Pending: {agent_stats[1]:,}')
        print(f'  Errors: {agent_stats[2]:,}')
        
        # Get action counts
        cur.execute('''
            SELECT COUNT(*) FROM agent_actions
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        ''')
        recent_actions = cur.fetchone()[0]
        if recent_actions:
            print(f'  Actions created (24h): {recent_actions:,}')

    # Entity system health check
    cur.execute('SELECT check_entity_system_alerts()')
    health_status = cur.fetchone()[0]
    print(f'\\\\nEntity System Health: {health_status}')
    
    conn.close()
    
except psycopg2.OperationalError as e:
    print('ERROR: Could not connect to database')
    print('Please ensure PostgreSQL is running and accessible')
    sys.exit(1)
    
except Exception as e:
    print(f'ERROR: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

echo -e "\n${GREEN}The RAG system now has BULLETPROOF classification & entity intelligence!${NC}"
echo -e "${BLUE}Entity System: BULLETPROOF SpaCy NER with duplicate prevention & health monitoring${NC}"
echo -e "${GREEN}Features: Database constraints, idempotent processing, real-time monitoring${NC}"
echo -e "${YELLOW}Note: For article entity extraction, run: python entity_extraction/build_entity_system_incremental_fixed.py --articles-only${NC}"

# Note: Celery workers may still be needed for other tasks (article processing, etc.)
# so we don't automatically clean them up here