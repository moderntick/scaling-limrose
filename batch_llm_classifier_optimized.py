#!/usr/bin/env python3
"""
Optimized Batch LLM Email Classifier
This version avoids subprocess overhead by directly integrating the embedding system.
"""

import psycopg2
import psycopg2.extras
import requests
import json
import os
import sys
import time
from typing import List, Dict, Any, Optional

# Import the enhanced email embeddings directly
from enhanced_email_embeddings import EnhancedEmailEmbeddings

# --- Configuration ---
DB_NAME = os.getenv("DB_NAME", "limrose_email_pipeline")
DB_USER = os.getenv("DB_USER", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")

# Choose your LLM provider and set the API key
# Options: "GEMINI" or "DEEPSEEK"
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "GEMINI")  # or "DEEPSEEK"
LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "gemini-2.0-flash-lite")  # Configurable model

if LLM_PROVIDER == "GEMINI":
    LLM_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{LLM_MODEL}:generateContent?key={LLM_API_KEY}"
else:  # DEEPSEEK
    LLM_API_URL = os.getenv("LLM_API_URL", "https://api.deepseek.com/v1/chat/completions")

# The final, approved list of classification labels
CLASSIFICATION_LABELS = [
    "editorial_collaboration", "freelance_pitch", "story_lead_or_tip",
    "opinion_or_letter_to_editor", "press_release", "strategic_partnership",
    "legal_or_corporate", "sales_or_advertising_inquiry", "human_resources",
    "financial_admin", "community_event_listing", "customer_service_or_feedback",
    "public_or_legal_notice", "internal_communication", "general_inquiry",
    "marketing_or_newsletter", "customer_issue", "customer_complaint"
]

class OptimizedLLMBatchClassifier:
    # --- Deterministic Rule Sets ---
    FINANCIAL_SENDERS = [
        # Banks (specific no-reply addresses to avoid phishing)
        'noreply@td.com',
        'email@e.tdbank.com',
        'no-reply@chase.com',
        'alerts@bankofamerica.com',
        'onlinebanking@wellsfargo.com',
        'alerts@wellsfargo.com',
        
        # Personal payment services (not business revenue)
        '@venmo.com',  # Personal payments only
        '@zelle.com',
        '@cashapp.com',
        
        # Food delivery services (receipts for meals ordered)
        '@ubereats.com',
        '@uber.com',  # Uber rides/eats
        'noreply@ubereats.com',
        '@doordash.com',
        'no-reply@doordash.com',
        '@grubhub.com',
        '@eat.grubhub.com',
        '@seamless.com',
        '@customers.instacartemail.com',
        
        # Expense management
        '@expensify.com',
        '@concur.com',
        
        # Rent/utilities
        '@cozy.co',  # Rent payment service
        
        # Transportation
        '@lyftmail.com',
        
        # Additional POS/receipt services
        '@printwithme.com',  # Printing services
        '@account.etsy.com',  # Etsy purchases
        '@chipotle.com',
        '@email.chipotle.com',
        '@e.fiverr.com',  # Fiverr purchases
        '@twitch.tv',  # Twitch subscriptions
        '@email.peacocktv.com',  # Peacock TV subscription
        '@notify.chime.com',  # Chime banking
        
        # EXCLUDED from auto-filter (need case-by-case review):
        # - @paypal.com (except "Receipt for Your Payment to" emails)
        # - @shopify.com (business revenue)
        # - @stripe.com (business revenue)
        # - @square.com/@squareup.com (business revenue)
        # - @messaging.squareup.com (business notifications)
        # - @intuit.com (could be QuickBooks business data)
    ]
    
    # Technical/Development services that should be general_inquiry
    TECH_SERVICES = [
        '@github.com',
        'notifications@github.com',
        '@patreon.com',
        'bingo@patreon.com',
        '@discord.com',
        '@slack.com',
        '@trello.com',
        '@asana.com'
    ]

    NEWSWIRE_DOMAINS = [
        'globenewswire.com', 'prnewswire.com', 'businesswire.com',
        'einpresswire.com', 'prweb.com', 'prlog.org', 'pr.com',
        'marketwired.com', 'cision.com', 'newswire.ca', 'presswire.com',
        'pr-inside.com', '24-7pressrelease.com'
    ]

    MARKETING_DOMAINS = [
        'mailchimp.com', 'constantcontact.com', 'campaign-archive.com',
        'list-manage.com', 'sendgrid.net', 'klaviyo.com', 'convertkit.com',
        'activecampaign.com', 'getresponse.com', 'aweber.com', 'mailerlite.com',
        'sendinblue.com', 'hubspot.com', 'salesforce.com', 'marketo.com',
        'substack.com', 'buttondown.email', 'revue.co', 'ghost.io',
        'eventbrite.com', 'meetup.com', 'lu.ma', 'partiful.com',
        'politico.com', 'email.politico.com', 'nytimes.com', 'wsj.com',
        'ft.com', 'economist.com', 'bloomberg.com', 'thecity.nyc',
        'gothamist.com', 'timeout.com', 'theskimm.com', 'morningbrew.com',
        'axios.com', 'hyperallergic.com', 'citylimits.org', 'newyorkyimby.com',
        'theskint.com', 'hellgatenyc.com',
        'hinge.com',
        'mail.hinge.co',
        # Additional marketing domains
        'promo.ra.co',  # RA event promotions
        'liveauctioneers.com'  # Auction notifications
    ]

    MARKETING_PATTERNS = [
        'subscribe', 'subscription', 'premium', 'member', 'join us', 'join our',
        'early bird', 'limited time', 'special offer', 'exclusive', 'save now',
        "don't miss", 'last chance', 'ending soon', 'flash sale', 'today only',
        'free trial', 'discount', '% off', 'newsletter', 'weekly update',
        'monthly roundup', 'in case you missed', 'icymi', 'this week in',
        'daily brief', 'morning brief', 'evening brief'
    ]

    MARKETING_EXCLUDE_DOMAINS = [
        'jotform.com', 'gmail.com', 'bushwickdaily.com'
    ]

    def __init__(self):
        """Initializes the database connection and embedding system."""
        try:
            self.conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, host=DB_HOST
            )
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            print("✅ Database connection established.")
        except psycopg2.OperationalError as e:
            print(f"❌ CRITICAL: Could not connect to database '{DB_NAME}'. Please check connection settings.", file=sys.stderr)
            raise e
        
        # Initialize the embedding system once
        print("🔧 Initializing enhanced embedding system...")
        self.embedding_system = EnhancedEmailEmbeddings()
        print("✅ Embedding system ready.")
        
        # Initialize HTTP session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        print("✅ HTTP session initialized with connection pooling.")
        
        # Token and cost tracking
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cost = 0.0
        
        # Load model pricing ONCE during initialization
        self.model_name = LLM_API_URL.split('/models/')[1].split(':')[0] if 'models/' in LLM_API_URL else 'unknown'
        self._load_pricing_once()

    def _load_pricing_once(self):
        """Load pricing from config file ONCE during initialization"""
        try:
            with open('model_pricing.json', 'r') as f:
                pricing_data = json.load(f)
                if self.model_name in pricing_data:
                    pricing = pricing_data[self.model_name]
                    self.GEMINI_INPUT_PRICE_PER_1M = pricing['input_price_per_1m']
                    self.GEMINI_OUTPUT_PRICE_PER_1M = pricing['output_price_per_1m']
                    print(f"💰 Loaded pricing for {self.model_name}: ${self.GEMINI_INPUT_PRICE_PER_1M}/{self.GEMINI_OUTPUT_PRICE_PER_1M} per 1M tokens")
                else:
                    print(f"⚠️ No pricing found for {self.model_name}, using defaults")
                    self.GEMINI_INPUT_PRICE_PER_1M = 0.075
                    self.GEMINI_OUTPUT_PRICE_PER_1M = 0.30
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"⚠️ Could not load pricing file: {e}, using defaults")
            # Default pricing fallback
            self.GEMINI_INPUT_PRICE_PER_1M = 0.075
            self.GEMINI_OUTPUT_PRICE_PER_1M = 0.30

    def _apply_deterministic_rules(self, email: Dict[str, Any]) -> Optional[List[str]]:
        """
        Applies deterministic rules based on sender email, subject, and body.
        Returns a list of classifications if a rule matches, otherwise None.
        Order of rules matters (more specific/critical first).
        """
        sender_email_lower = (email.get('sender_email') or '').lower()
        subject_lower = (email.get('subject') or '').lower()
        subject = email.get('subject') or ''

        # 1. Spam by Sender (requires DB lookup)
        # This rule is based on previous manual spam classifications.
        self.cursor.execute("""
            SELECT 1 FROM email_pipeline_routes
            WHERE email_id = %s AND pipeline_type = 'spam'
            LIMIT 1
        """, (email['id'],))
        if self.cursor.fetchone():
            return ["spam"]

        # 2. Financial Admin - Special handling for PayPal
        # Only filter outgoing payment receipts, not incoming business notifications
        if 'paypal' in sender_email_lower and subject.startswith('Receipt for Your Payment to'):
            return ["financial_admin"]
        
        # 3. Financial Admin - General financial senders
        for sender in self.FINANCIAL_SENDERS:
            if sender.startswith('@'): # Domain match
                if sender_email_lower.endswith(sender):
                    return ["financial_admin"]
            elif sender_email_lower == sender:
                return ["financial_admin"]

        # 3. Newswire
        for domain in self.NEWSWIRE_DOMAINS:
            if sender_email_lower.endswith(f'@{domain}'):
                return ["press_release"]

        # 4. Marketing/Newsletter
        # Check exclusions first
        for exclude_domain in self.MARKETING_EXCLUDE_DOMAINS:
            if sender_email_lower.endswith(f'@{exclude_domain}'):
                return None # Exclude from marketing classification

        # Check domains
        for domain in self.MARKETING_DOMAINS:
            if sender_email_lower.endswith(f'@{domain}'):
                return ["marketing_or_newsletter"]
        
        # Check subject patterns - DISABLED: Too broad, causes false positives
        # for pattern in self.MARKETING_PATTERNS:
        #     if pattern in subject_lower:
        #         return ["marketing_or_newsletter"]

        return None # No deterministic rule matched

    def get_emails_to_classify(self, batch_size: int = 10) -> List[Dict[str, Any]]:
        """
        Fetches a batch of the most recent emails that have not yet been classified
        with our new, detailed classification schema.
        """
        print(f"🔍 Fetching {batch_size} recent emails for classification...")
        
        # This query finds emails that do not have any of our new pipeline routes.
        # This is the safest way to find "unprocessed" emails for this script.
        query = """
            SELECT
                ce.id,
                ce.subject,
                ce.sender_email,
                ce.body_text,
                ce.sender_name,
                ce.thread_id,
                ce.date_sent
            FROM
                classified_emails ce
            WHERE NOT EXISTS (
                SELECT 1
                FROM email_pipeline_routes epr
                WHERE epr.email_id = ce.id AND epr.pipeline_type IN %s
            )
            ORDER BY
                ce.id DESC
            LIMIT %s;
        """
        self.cursor.execute(query, (tuple(CLASSIFICATION_LABELS), batch_size))
        emails = self.cursor.fetchall()
        print(f"  Found {len(emails)} emails to process.")
        return [dict(row) for row in emails]

    def classify_with_llm(self, email: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calls the configured LLM API to get classifications for a single email.
        """
        print(f"  🧠 Classifying email ID: {email['id']} ('{email['subject'][:50]}...')")
        
        # Optimized for paid tier with Flash Lite
        # 0.2 seconds = 5 requests/second = 300 RPM (safe with paid account)
        time.sleep(0.2)

        # Optimized prompt - shorter but maintains multi-label capability
        prompt = f"""Classify this email into ALL applicable categories:
{json.dumps(CLASSIFICATION_LABELS)}

Important: Select ALL relevant labels. An email can belong to multiple categories.
Default to "general_inquiry" if unclear.

Subject: {email.get('subject', '')[:200]}
From: {email.get('sender_email', '')}
Body: {email.get('body_text', '')[:2000]}

Return JSON only: {{"classifications": ["label1", "label2", ...]}}"""
        # API Call
        try:
            # Example for Gemini API
            if LLM_PROVIDER == "GEMINI":
                payload = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "temperature": 0.1,  # Low temperature for consistent classification
                        "topP": 0.95,
                        "topK": 40,
                        "maxOutputTokens": 256,  # Classifications only need short output
                        "responseMimeType": "application/json"  # Ensure JSON response
                    }
                }
                response = self.session.post(LLM_API_URL, json=payload, timeout=30)
                response.raise_for_status()
                response_json = response.json()
                
                # Extract token usage from response
                if 'usageMetadata' in response_json:
                    usage = response_json['usageMetadata']
                    input_tokens = usage.get('promptTokenCount', 0)
                    output_tokens = usage.get('candidatesTokenCount', 0)
                    
                    # Update totals
                    self.total_input_tokens += input_tokens
                    self.total_output_tokens += output_tokens
                    
                    # Calculate cost for this request
                    input_cost = (input_tokens / 1_000_000) * self.GEMINI_INPUT_PRICE_PER_1M
                    output_cost = (output_tokens / 1_000_000) * self.GEMINI_OUTPUT_PRICE_PER_1M
                    request_cost = input_cost + output_cost
                    self.total_cost += request_cost
                
                # Extract the JSON string from the response
                api_result_text = response_json['candidates'][0]['content']['parts'][0]['text']
                
            # Example for DeepSeek API
            elif LLM_PROVIDER == "DEEPSEEK":
                payload = {
                    "model": "deepseek-chat",
                    "messages": [{"role": "user", "content": prompt}],
                    "response_format": {"type": "json_object"}
                }
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {LLM_API_KEY}'
                }
                response = requests.post(LLM_API_URL, headers=headers, json=payload, timeout=60)
                response.raise_for_status()
                api_result_text = response.json()['choices'][0]['message']['content']

            # Clean the text and parse JSON
            clean_json_text = api_result_text.strip().replace("```json", "").replace("```", "")
            return json.loads(clean_json_text)

        except requests.exceptions.RequestException as e:
            # Handle timeout errors specifically
            if isinstance(e, requests.exceptions.ReadTimeout):
                print(f"    ⚠️ Request timed out. Retrying with longer timeout...", file=sys.stderr)
                retry_attempts = getattr(self, 'timeout_retries', 0)
                if retry_attempts < 2:
                    self.timeout_retries = retry_attempts + 1
                    # Increase timeout for retry (45s, 60s)
                    new_timeout = 45 if retry_attempts == 0 else 60
                    print(f"    ⏳ Retrying with {new_timeout}s timeout (attempt {retry_attempts + 1}/2)...")
                    try:
                        response = self.session.post(LLM_API_URL, json=payload, timeout=new_timeout)
                        response.raise_for_status()
                        self.timeout_retries = 0  # Reset on success
                        response_json = response.json()
                        
                        # Continue with normal processing
                        if 'usageMetadata' in response_json:
                            usage = response_json['usageMetadata']
                            input_tokens = usage.get('promptTokenCount', 0)
                            output_tokens = usage.get('candidatesTokenCount', 0)
                            self.total_input_tokens += input_tokens
                            self.total_output_tokens += output_tokens
                            input_cost = (input_tokens / 1_000_000) * self.GEMINI_INPUT_PRICE_PER_1M
                            output_cost = (output_tokens / 1_000_000) * self.GEMINI_OUTPUT_PRICE_PER_1M
                            request_cost = input_cost + output_cost
                            self.total_cost += request_cost
                        
                        api_result_text = response_json['candidates'][0]['content']['parts'][0]['text']
                        clean_json_text = api_result_text.strip().replace("```json", "").replace("```", "")
                        return json.loads(clean_json_text)
                    except Exception as retry_error:
                        print(f"    ❌ Retry failed: {retry_error}", file=sys.stderr)
                        self.timeout_retries = 0
                        return {"classifications": ["api_error"], "reasoning": f"Timeout after retries: {retry_error}"}
                else:
                    self.timeout_retries = 0
                    return {"classifications": ["api_error"], "reasoning": "Request timeout after 2 retries"}
            
            # Handle rate limit errors (429)
            elif hasattr(e, 'response') and e.response and e.response.status_code == 429:
                print(f"    ⚠️ Rate limit hit. Waiting before retry...", file=sys.stderr)
                retry_attempts = getattr(self, 'retry_attempts', 0)
                if retry_attempts < 3:
                    wait_time = 2 ** retry_attempts  # 1s, 2s, 4s
                    print(f"    ⏳ Waiting {wait_time} seconds before retry {retry_attempts + 1}/3...")
                    time.sleep(wait_time)
                    self.retry_attempts = retry_attempts + 1
                    return self.classify_with_llm(email)  # Recursive retry
                else:
                    self.retry_attempts = 0
                    print(f"    ❌ Max retries exceeded. Marking as api_error.", file=sys.stderr)
                    return {"classifications": ["api_error"], "reasoning": "Rate limit exceeded after 3 retries"}
            
            # Handle all other request errors
            else:
                print(f"    ❌ LLM API Error: {e}", file=sys.stderr)
                return {"classifications": ["api_error"], "reasoning": f"API call failed: {e}"}
        except (json.JSONDecodeError, KeyError) as e:
            print(f"    ❌ LLM Response Parsing Error: {e}", file=sys.stderr)
            return {"classifications": ["api_error"], "reasoning": f"Could not parse LLM JSON response: {e}"}

    def create_enhanced_embedding(self, email_data: Dict[str, Any], classifications: List[str]):
        """
        Directly calls the embedding system instead of using subprocess.
        """
        print(f"  ⚡ Creating enhanced embedding for email ID: {email_data['id']} with classifications: {classifications}")
        try:
            # Directly call the embedding system method
            self.embedding_system.create_embedding_for_classified_email(
                email_id=email_data['id'],
                classifications=classifications
            )
            print(f"    ✅ Embedding successful for email {email_data['id']}.")
        except Exception as e:
            print(f"    ❌ ERROR creating embedding for email {email_data['id']}: {e}", file=sys.stderr)
            raise

    def update_pipeline_routes(self, email_id: int, result: Dict[str, Any]):
        """
        Updates the email_pipeline_routes table with the classifications from the LLM.
        """
        classifications = result.get("classifications", [])
        if not classifications:
            print(f"  ⚠️ No classifications returned for email {email_id}. Skipping database update.")
            return

        print(f"  💾 Storing pipeline routes for email ID: {email_id} -> {classifications}")
        
        # No need to handle 'unclassified' anymore - LLM always returns valid classifications

        insert_query = """
            INSERT INTO email_pipeline_routes (email_id, pipeline_type, status, priority_score)
            VALUES %s
            ON CONFLICT (email_id, pipeline_type) DO NOTHING;
        """
        # Create a list of tuples for the executemany call
        values = [(email_id, c, 'pending', 0.8) for c in classifications]
        psycopg2.extras.execute_values(self.cursor, insert_query, values)
        print(f"    -> Stored {len(values)} routes.")

    def run(self, batch_size: int = 10, dry_run: bool = False, process_all: bool = False):
        """Main execution loop."""
        print("="*80)
        print("🚀 Starting Optimized Batch LLM Email Classifier")
        print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE'}")
        print(f"   Batch Size: {batch_size}")
        print(f"   Process All: {'YES' if process_all else 'NO (single batch only)'}")
        print("="*80)

        if not LLM_API_KEY:
            print("❌ CRITICAL: LLM_API_KEY environment variable not set. Exiting.", file=sys.stderr)
            return

        # Get total count of unclassified emails
        self.cursor.execute("""
            SELECT COUNT(*) FROM classified_emails ce
            WHERE NOT EXISTS (
                SELECT 1 FROM email_pipeline_routes epr
                WHERE epr.email_id = ce.id AND epr.pipeline_type = ANY(%s)
            )
        """, (CLASSIFICATION_LABELS,))
        total_unclassified = self.cursor.fetchone()[0]
        
        if total_unclassified == 0:
            print("✅ No emails to process. System is up-to-date.")
            return
            
        print(f"\n📊 Total unclassified emails: {total_unclassified:,}")
        print(f"Processing in batches of {batch_size}\n")
        
        overall_processed = 0
        batch_num = 0
        skipped_due_to_errors = 0
        
        # Keep processing batches until all emails are classified (or just one batch if not process_all)
        while True:
            batch_num += 1
            emails = self.get_emails_to_classify(batch_size)
            if not emails:
                break
                
            print(f"\n📦 BATCH {batch_num} - Processing {len(emails)} emails")
            print(f"Overall progress: {overall_processed:,} / {total_unclassified:,} ({overall_processed/total_unclassified*100:.1f}%)")
            
            batch_skipped_in_loop = 0
            for idx, email in enumerate(emails, 1):
                # Show progress within batch
                print(f"\n[{overall_processed + idx}/{total_unclassified}] Processing email {email['id']}")
                
                # 1. Apply deterministic rules first
                deterministic_classifications = self._apply_deterministic_rules(email)

                if deterministic_classifications:
                    print(f"  ✅ Deterministically classified email ID: {email['id']} -> {deterministic_classifications}")
                    llm_result = {"classifications": deterministic_classifications}
                else:
                    # 2. Classify with LLM if no deterministic rule matched
                    llm_result = self.classify_with_llm(email)
                
                if not llm_result.get("classifications"):
                    print(f"  ❌ Failed to get valid classification for email {email['id']}. Skipping.")
                    continue

                if dry_run:
                    print(f"  [DRY RUN] Would store routes: {llm_result['classifications']}")
                    print(f"  [DRY RUN] Would create embedding for email {email['id']}")
                else:
                    # 2. Update the database with the new pipeline routes
                    self.update_pipeline_routes(email['id'], llm_result)
                    
                    # 3. Create enhanced embedding directly (no subprocess)
                    try:
                        self.create_enhanced_embedding(email, llm_result.get("classifications", []))
                    except Exception as e:
                        print(f"❌ CRITICAL: Failed to create embedding for email {email['id']}: {e}")
                        self.conn.rollback()
                        raise RuntimeError(f"Embedding creation failed for email {email['id']}") from e
                
                # Commit every 50 emails to prevent transaction conflicts
                if idx % 50 == 0:
                    print(f"💾 Committing batch progress at {idx} emails...")
                    try:
                        self.conn.commit()
                        # Verify the commit worked by checking embeddings
                        self.cursor.execute("""
                            SELECT COUNT(*) FROM enhanced_email_embeddings 
                            WHERE email_id = %s
                        """, (email['id'],))
                        if self.cursor.fetchone()[0] == 0:
                            raise RuntimeError(f"Embedding for email {email['id']} not found after commit!")
                    except Exception as e:
                        print(f"❌ CRITICAL: Database commit failed: {e}")
                        raise
                
                print("-" * 50)
            
            # Update overall counter after batch completes
            overall_processed += len(emails)

            # Commit all database changes at the end of the batch
            if not dry_run:
                print("✅ Committing final changes to the database.")
                self.conn.commit()
            
            # If not processing all, stop after first batch
            if not process_all:
                break
            
            # Verify embeddings were actually saved
            self.cursor.execute("""
                SELECT COUNT(*) FROM enhanced_email_embeddings 
                WHERE email_id IN (
                    SELECT id FROM classified_emails 
                    WHERE id >= %s AND id <= %s
                )
            """, (emails[-1]['id'], emails[0]['id']))
            saved_count = self.cursor.fetchone()[0]
            
            expected_count = len(emails) - batch_skipped_in_loop
            print(f"📊 Verification: {saved_count}/{len(emails)} embeddings saved ({batch_skipped_in_loop} skipped due to errors)")
            
            # Only fail if we have a mismatch in successfully processed emails
            if saved_count < expected_count:
                missing = expected_count - saved_count
                raise RuntimeError(f"❌ CRITICAL: {missing} embeddings failed to save! Expected {expected_count} but only {saved_count} were persisted.")

        # Print token usage and cost summary
        print("\n" + "="*50)
        print("📊 Processing Summary")
        print("="*50)
        print(f"Total Processed:     {overall_processed - skipped_due_to_errors:,}")
        print(f"Skipped (API errors): {skipped_due_to_errors:,}")
        print(f"\nToken Usage:")
        print(f"Total Input Tokens:  {self.total_input_tokens:,}")
        print(f"Total Output Tokens: {self.total_output_tokens:,}")
        print(f"Total Tokens:        {self.total_input_tokens + self.total_output_tokens:,}")
        print(f"Total Cost:          ${self.total_cost:.4f}")
        print("="*50)
        
        # Save cumulative costs to file
        self._update_cumulative_costs()

        self.cursor.close()
        self.conn.close()
        print("🏁 Batch processing complete.")

    def _update_cumulative_costs(self):
        """Update cumulative cost tracking file"""
        import json
        from datetime import datetime
        
        costs_file = "gemini_costs_tracking.json"
        
        # Load existing data or create new
        try:
            with open(costs_file, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            data = {
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "total_cost": 0.0,
                "batches": []
            }
        
        # Update totals
        data["total_input_tokens"] += self.total_input_tokens
        data["total_output_tokens"] += self.total_output_tokens
        data["total_cost"] += self.total_cost
        
        # Add this batch
        data["batches"].append({
            "timestamp": datetime.now().isoformat(),
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "cost": self.total_cost
        })
        
        # Save updated data
        with open(costs_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\n💾 Cumulative costs saved to {costs_file}")
        print(f"📈 All-time total cost: ${data['total_cost']:.4f}")

    def __del__(self):
        """Clean up resources"""
        if hasattr(self, 'session'):
            self.session.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()


if __name__ == "__main__":
    # Example of how to run the script from the command line
    # python batch_llm_classifier_optimized.py --batch-size 50
    # python batch_llm_classifier_optimized.py --dry-run
    # python batch_llm_classifier_optimized.py --all --batch-size 50
    import argparse
    parser = argparse.ArgumentParser(description="Run the optimized batch LLM email classifier.")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of emails to process per batch.")
    parser.add_argument("--dry-run", action="store_true", help="Run without making any API calls or DB changes.")
    parser.add_argument("--all", action="store_true", help="Process ALL unclassified emails (multiple batches).")
    args = parser.parse_args()

    classifier = OptimizedLLMBatchClassifier()
    classifier.run(batch_size=args.batch_size, dry_run=args.dry_run, process_all=args.all)