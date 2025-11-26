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

## Installation Options

### Option 1: Quick Install (If you have Python & PostgreSQL)
If you already have Python 3.8+ and PostgreSQL installed, skip to [Quick Start](#quick-start).

### Option 2: Complete Fresh Install (Recommended for new systems)
If you're starting with a fresh macOS system, follow the [Complete macOS Setup Guide](#complete-macos-setup-guide-from-fresh-system) below.

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

## Complete macOS Setup Guide (From Fresh System)

### Prerequisites for Fresh macOS

This guide assumes you have a fresh macOS system with no development tools installed.

### Step 1: Install Xcode Command Line Tools

Open Terminal and run:
```bash
xcode-select --install
```
Click "Install" when prompted. This installs essential development tools.

### Step 2: Install Homebrew

Homebrew is the package manager for macOS:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

After installation, add Homebrew to your PATH:
```bash
# For Apple Silicon Macs (M1/M2/M3)
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"

# For Intel Macs
echo 'eval "$(/usr/local/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/usr/local/bin/brew shellenv)"
```

### Step 3: Install Python 3

```bash
# Install Python 3.11 (recommended)
brew install python@3.11

# Verify installation
python3 --version
# Should show: Python 3.11.x

# Install pip if not included
python3 -m ensurepip --upgrade
```

### Step 4: Install PostgreSQL with pgvector

```bash
# Install PostgreSQL 17
brew install postgresql@17

# Install pgvector extension
brew install pgvector

# Add PostgreSQL to PATH
echo 'export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Initialize the database
initdb --locale=C -E UTF-8 /opt/homebrew/var/postgresql@17

# Start PostgreSQL service
brew services start postgresql@17

# Wait a moment for the service to start
sleep 3

# Create a database user (optional, but recommended)
createuser -s postgres

# Verify PostgreSQL is running
psql postgres -c "SELECT version();"
```

### Step 5: Install Git (if needed)

```bash
# Git is usually included with Xcode tools, but if not:
brew install git

# Configure git (replace with your info)
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

### Step 6: Clone and Set Up the Email Pipeline Project

```bash
# Clone the repository
git clone https://github.com/moderntick/scaling-limrose.git
cd scaling-limrose

# Create Python virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install Python dependencies
pip install -r requirements.txt
```

### Step 7: Create the Database

```bash
# Create the email_pipeline database
createdb -U postgres email_pipeline

# Verify the database was created
psql -U postgres -l | grep email_pipeline

# Enable pgvector extension
psql -U postgres -d email_pipeline -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

### Step 8: Set Up Environment Variables

```bash
# Copy the example environment file
cp .env.example .env

# Edit the .env file with your settings
nano .env  # or use your preferred editor
```

Required settings in `.env`:
```env
# Database (usually these defaults work)
DB_NAME=email_pipeline
DB_USER=postgres
DB_HOST=localhost

# Gmail Service Account (you'll need to create this)
SERVICE_ACCOUNT_FILE=config/service-account-key.json
DELEGATE_EMAIL=your-email@yourdomain.com

# Gemini API Key (get from https://makersuite.google.com/app/apikey)
LLM_API_KEY=your-gemini-api-key-here
LLM_PROVIDER=GEMINI
```

### Step 9: Run the Automated Setup

```bash
# This will verify everything is installed correctly
./update_emails_v2.sh --setup
```

## Troubleshooting Fresh macOS Install

### If PostgreSQL won't start:
```bash
# Check if another PostgreSQL is running
brew services list

# Stop all PostgreSQL services
brew services stop postgresql
brew services stop postgresql@17

# Remove old PostgreSQL data if exists
rm -rf /opt/homebrew/var/postgres

# Reinitialize
initdb --locale=C -E UTF-8 /opt/homebrew/var/postgresql@17

# Start again
brew services restart postgresql@17
```

### If Python commands fail:
```bash
# Make sure you're using the right Python
which python3
# Should show: /opt/homebrew/bin/python3

# If not, fix your PATH
echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### If pip install fails with SSL errors:
```bash
# Upgrade certificates
brew install ca-certificates
pip install --upgrade certifi
```

### Common Permission Issues:
```bash
# If you get permission errors with PostgreSQL
sudo chown -R $(whoami) /opt/homebrew/var/postgresql@17
```

## Quick Start After Installation

Once everything is installed:

```bash
# 1. Activate Python environment
cd scaling-limrose
source venv/bin/activate

# 2. Run the pipeline
./update_emails_v2.sh

# 3. View customer issues dashboard (optional)
python customer_issue_dashboard.py
# Then open http://localhost:5000 in your browser
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
