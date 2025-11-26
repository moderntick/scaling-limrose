# Email Pipeline Project

A comprehensive email processing pipeline that extracts emails from Gmail, performs deduplication, creates embeddings, classifies emails into business pipelines, and extracts entities using NLP.

## Overview

This pipeline processes emails through several stages:
1. **Email Extraction**: Fetches emails from Gmail using service account authentication
2. **Deduplication**: Advanced fingerprinting to identify duplicate emails
3. **Chunking & Embeddings**: Creates searchable chunks with vector embeddings
4. **Classification**: Uses LLM to classify emails into business pipelines
5. **Customer Issue Tracking**: Analyzes customer complaints, tracks resolutions, and generates fix documentation
6. **Enhanced Embeddings**: Creates context-aware embeddings with thread and sender history

## Architecture

### Core Components

- **`gmail_service_account_extractor_with_dedup.py`**: Gmail extraction with complete deduplication
- **`batch_process_all_emails.py`**: Creates email chunks and embeddings for RAG
- **`batch_llm_classifier_optimized.py`**: LLM-based email classification (Gemini)
- **`customer_issue_tracker.py`**: Analyzes customer issues and tracks resolutions
- **`customer_issue_dashboard.py`**: Web dashboard for viewing customer issues
- **`enhanced_email_embeddings.py`**: Context-aware embeddings with full thread context
- **`email_deduplication_complete.py`**: Advanced email fingerprinting system
- **`email_pipeline_router.py`**: Multi-classification routing system

### Key Features

- **Deduplication**: Content normalization, HTML extraction, alias resolution
- **Vector Search**: Uses pgvector for semantic search capabilities
- **Multi-Classification**: Emails can belong to multiple pipelines
- **Thread Intelligence**: Full conversation context and participant tracking
- **Entity System**: Bulletproof NER with duplicate prevention

## Prerequisites

### System Requirements
- **Python**: 3.8 or higher
- **PostgreSQL**: 17.x with pgvector extension
- **Redis**: (optional but recommended)
- **RAM**: 4GB minimum (8GB recommended)
- **Disk**: 2GB for models + space for email data

### Service Requirements
- Gmail account with admin access (for delegation)
- Google Cloud project with Gmail API enabled
- Gemini API key (free tier available)

## Quick Start

The easiest way to set up the pipeline is using the automated setup script:

```bash
# Clone and setup
git clone https://github.com/moderntick/scaling-limrose.git
cd scaling-limrose
./update_emails_v2.sh --setup
```

This will:
- Check all dependencies
- Create virtual environment
- Install Python packages
- Set up PostgreSQL database
- Download ML models
- Create configuration files

## Installation

1. **Clone the repository**:
```bash
git clone https://github.com/moderntick/scaling-limrose.git
cd scaling-limrose
```

2. **Create virtual environment**:
```bash
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Set up PostgreSQL**:
```bash
createdb nyc_news
psql -d nyc_news -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

5. **Configure environment**:
```bash
cp .env.example .env
# Edit .env with your configuration:
# - SERVICE_ACCOUNT_FILE: Path to your Gmail service account JSON
# - DELEGATE_EMAIL: Email address to access
# - LLM_API_KEY: Your Gemini API key
# - DB_USER: PostgreSQL username
```

## Configuration

### Environment Variables

Create a `.env` file with:
```env
# Gmail Configuration
SERVICE_ACCOUNT_FILE=/path/to/service-account-key.json
DELEGATE_EMAIL=your-email@example.com

# Database Configuration
DB_NAME=email_pipeline
DB_USER=postgres
DB_HOST=localhost
DB_PORT=5432

# LLM Configuration
LLM_PROVIDER=GEMINI
LLM_API_KEY=your-gemini-api-key

# Optional: Offline mode for embeddings
HF_HUB_OFFLINE=0
TRANSFORMERS_OFFLINE=0
```

### Service Account Setup

1. Create a Google Cloud project
2. Enable Gmail API
3. Create a service account with domain-wide delegation
4. Download the service account key JSON
5. Place it in `config/` directory (excluded from git)

## Detailed Setup Guide

### Step 1: Install System Dependencies

**macOS:**
```bash
# Install Homebrew if needed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install PostgreSQL with pgvector
brew install postgresql@17 pgvector

# Install Redis (optional)
brew install redis

# Start services
brew services start postgresql@17
brew services start redis
```

**Ubuntu/Debian:**
```bash
# Add PostgreSQL APT repository
sudo sh -c 'echo "deb https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update

# Install PostgreSQL 17 with pgvector
sudo apt-get install postgresql-17 postgresql-17-pgvector

# Install Redis (optional)
sudo apt-get install redis-server

# Start services
sudo systemctl start postgresql
sudo systemctl start redis
```

### Step 2: Create Google Cloud Service Account

1. **Go to Google Cloud Console**: https://console.cloud.google.com
2. **Create a new project** or select existing
3. **Enable Gmail API**:
   - Navigate to "APIs & Services" > "Library"
   - Search for "Gmail API"
   - Click "Enable"

4. **Create Service Account**:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "Service Account"
   - Name: `email-pipeline-service`
   - Click "Create and Continue"
   - Skip optional permissions
   - Click "Done"

5. **Enable Domain-Wide Delegation**:
   - Click on the created service account
   - Click "Edit"
   - Check "Enable G Suite Domain-wide Delegation"
   - Save

6. **Download Key**:
   - In the service account details, go to "Keys" tab
   - Click "Add Key" > "Create new key"
   - Choose "JSON" format
   - Save as `config/service-account-key.json`

### Step 3: Configure Gmail Delegation

1. **In Google Workspace Admin** (admin.google.com):
   - Navigate to "Security" > "API controls" > "Domain-wide delegation"
   - Click "Add new"
   - Client ID: (copy from service account details)
   - Scopes: `https://www.googleapis.com/auth/gmail.readonly,https://www.googleapis.com/auth/gmail.modify`
   - Click "Authorize"

### Step 4: Get Gemini API Key

1. Visit https://makersuite.google.com/app/apikey
2. Click "Create API key"
3. Copy the key for your `.env` file

### Step 5: Database Setup

```bash
# Create database user (if needed)
sudo -u postgres createuser --interactive --pwprompt
# Enter username: postgres (or your preferred username)

# Create database
createdb -U postgres email_pipeline

# Verify pgvector is installed
psql -U postgres -d email_pipeline -c "CREATE EXTENSION IF NOT EXISTS vector;"

# Test connection
psql -U postgres -d email_pipeline -c "SELECT version();"
```

### Step 6: Run Initial Setup

```bash
# Make script executable
chmod +x update_emails_v2.sh

# Run automated setup
./update_emails_v2.sh --setup

# This will:
# - Create virtual environment
# - Install all Python dependencies
# - Set up database tables
# - Download ML models (~90MB)
# - Verify configuration
```

### Step 7: Configure Environment

Edit `.env` with your actual values:

```env
# Required configurations
SERVICE_ACCOUNT_FILE=config/service-account-key.json
DELEGATE_EMAIL=your-email@yourdomain.com  # Email to access
LLM_API_KEY=your-actual-gemini-api-key

# Database (update if different)
DB_NAME=email_pipeline
DB_USER=postgres  # Your PostgreSQL username
DB_HOST=localhost
```

### Step 8: Verify Setup

```bash
# Test database
psql -U postgres -d email_pipeline -c "\dt"  # Should show tables

# Test Gmail authentication
python -c "
from gmail_service_account_extractor_with_dedup import GmailServiceAccountExtractor
e = GmailServiceAccountExtractor()
print('✅ Gmail auth successful!')
"

# Test LLM
python -c "
import os, requests
from dotenv import load_dotenv
load_dotenv()
url = f\"https://generativelanguage.googleapis.com/v1beta/models?key={os.getenv('LLM_API_KEY')}\"
print('✅ Gemini API OK!' if requests.get(url).status_code == 200 else '❌ Gemini API error')
"
```

## Usage

### Full Pipeline

Run the complete pipeline:
```bash
./update_emails_v2.sh
```

Options:
```bash
# Process emails from specific date
./update_emails_v2.sh --start-date 2024/01/01

# Limit number of emails
./update_emails_v2.sh --max-results 100
```

### Individual Components

```bash
# Step 1: Extract emails
python gmail_service_account_extractor_with_dedup.py

# Step 2: Create chunks and embeddings
python batch_process_all_emails.py

# Step 3: Classify emails
python batch_llm_classifier_optimized.py --all --batch-size 50

# Create database tables
python scripts/create_email_chunks_table.py
```

## Database Schema

### Main Tables

- **`classified_emails`**: Main email storage with metadata
- **`email_chunks`**: Chunked email content with embeddings
- **`enhanced_email_embeddings`**: Context-aware embeddings
- **`email_pipeline_routes`**: Pipeline classifications
- **`email_fingerprints_v2`**: Deduplication fingerprints
- **`sender_interaction_history`**: Sender relationship tracking
- **`thread_context`**: Conversation thread analysis

## Pipeline Classifications

Emails are classified into:
- `editorial_collaboration`
- `freelance_pitch`
- `story_lead_or_tip`
- `press_release`
- `sales_or_advertising_inquiry`
- `strategic_partnership`
- `legal_or_corporate`
- `human_resources`
- `financial_admin`
- `marketing_or_newsletter`
- And more...

## Security Notes

1. **Never commit credentials**:
   - Service account keys
   - API keys
   - Database passwords

2. **Use environment variables** for all sensitive configuration

3. **The `.gitignore` file excludes**:
   - `config/service-account-key.json`
   - `.env` files
   - API key tracking files

## Performance

- Processes ~1800+ emails/minute with batch embedding
- Deduplication runs in real-time during extraction
- LLM classification: ~5 emails/second with Gemini Flash

## Troubleshooting

### Common Issues

1. **PostgreSQL connection errors**: 
   - Ensure PostgreSQL is running
   - Check database exists: `psql -l | grep nyc_news`

2. **Service account authentication**:
   - Verify service account has Gmail API access
   - Check domain-wide delegation is enabled

3. **Embedding model download**:
   - First run downloads ~90MB model
   - Use `HF_HUB_OFFLINE=1` for offline mode

### Debug Commands

```bash
# Check email count
psql -d nyc_news -c "SELECT COUNT(*) FROM classified_emails;"

# View recent classifications
psql -d nyc_news -c "SELECT pipeline_type, COUNT(*) FROM email_pipeline_routes GROUP BY pipeline_type;"

# Check for duplicates
psql -d nyc_news -c "SELECT COUNT(*) FROM email_duplicate_groups WHERE member_count > 1;"
```

## Contributing

1. Create a feature branch
2. Make changes
3. Ensure credentials are not exposed
4. Run the full pipeline to test
5. Submit a pull request

## License

This project is licensed under the Applequist Open Source License (AOSL) - see the [LICENSE](LICENSE) file for details.

**Important**: Commercial use requires visible attribution to Alec Meeker and Applequist Inc. on all user-facing pages. See [ATTRIBUTION.md](ATTRIBUTION.md) for implementation guidelines.

### Quick Summary:
- ✅ Free for personal, educational, and non-commercial use
- ✅ Free for commercial use with attribution
- ✅ Can modify and distribute
- ⚠️ Must display "Powered by Email Pipeline by Alec Meeker and Applequist Inc." on all pages for commercial use
- ⚠️ Must retain copyright notices

For questions about the license or commercial use, contact Applequist Inc.
