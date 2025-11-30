#!/usr/bin/env python3
"""
Installation Validation Script
Validates that the email pipeline is properly set up and ready to run
"""
import sys
import os
import json
import subprocess
from pathlib import Path
import importlib.util

def test_python_version():
    """Test Python version compatibility"""
    print("🔍 Checking Python version...")
    version = sys.version_info
    if version < (3, 7):
        print(f"❌ Python {version.major}.{version.minor} detected. Python 3.7+ required.")
        return False
    print(f"✓ Python {version.major}.{version.minor}.{version.micro} (compatible)")
    return True

def test_required_packages():
    """Test that all required packages are installed"""
    print("\n🔍 Checking required packages...")
    
    required_packages = [
        'cryptography',
        'aiohttp',
        'google.oauth2',
        'googleapiclient',
        'psycopg2',
        'sentence_transformers',
        'python_dotenv'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            if '.' in package:
                # Handle sub-modules like google.oauth2
                parent = package.split('.')[0]
                __import__(parent)
            else:
                __import__(package)
            print(f"✓ {package}")
        except ImportError:
            print(f"❌ {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n❌ Missing packages: {missing_packages}")
        print("💡 Run: pip install -r requirements.txt")
        return False
    
    return True

def test_oauth_configuration():
    """Test OAuth configuration"""
    print("\n🔍 Checking OAuth configuration...")
    
    config_path = Path.home() / '.email-pipeline' / 'config' / 'oauth_config.json'
    
    if not config_path.exists():
        print("❌ OAuth configuration not found")
        print("💡 Run: python setup_oauth.py")
        return False
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        required_keys = ['client_id', 'client_secret', 'redirect_uri', 'encryption_key', 'scopes']
        missing_keys = [key for key in required_keys if key not in config]
        
        if missing_keys:
            print(f"❌ OAuth config missing keys: {missing_keys}")
            return False
        
        print("✓ OAuth configuration found and valid")
        return True
        
    except json.JSONDecodeError:
        print("❌ OAuth configuration file is corrupted")
        return False
    except Exception as e:
        print(f"❌ Error reading OAuth configuration: {e}")
        return False

def test_database_connection():
    """Test database connection"""
    print("\n🔍 Checking database connection...")
    
    try:
        # Load environment variables
        env_path = Path('.env')
        if env_path.exists():
            from dotenv import load_dotenv
            load_dotenv()
        
        import psycopg2
        
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'email_pipeline'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
        
        # Test connection
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                print(f"✓ Database connection successful")
                print(f"  PostgreSQL version: {version.split(' ')[1]}")
                
                # Check for pgvector extension
                cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'vector');")
                has_vector = cursor.fetchone()[0]
                if has_vector:
                    print("✓ pgvector extension available")
                else:
                    print("⚠️  pgvector extension not found (optional for vector search)")
                
                # Check if raw_emails table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'raw_emails'
                    );
                """)
                has_table = cursor.fetchone()[0]
                if has_table:
                    print("✓ raw_emails table exists")
                else:
                    print("⚠️  raw_emails table not found (will be created if needed)")
        
        return True
        
    except ImportError:
        print("❌ psycopg2 not installed")
        return False
    except psycopg2.OperationalError as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Check database configuration in .env file")
        return False
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def test_llm_configuration():
    """Test LLM API configuration"""
    print("\n🔍 Checking LLM configuration...")
    
    api_key = os.getenv('LLM_API_KEY')
    provider = os.getenv('LLM_PROVIDER', 'GEMINI')
    
    if not api_key:
        print("❌ LLM_API_KEY not set in .env")
        print("💡 Get API key from: https://makersuite.google.com/app/apikey")
        return False
    
    if api_key.startswith('your-') or 'placeholder' in api_key.lower():
        print("❌ LLM_API_KEY appears to be a placeholder")
        print("💡 Set your actual Gemini API key in .env")
        return False
    
    print(f"✓ LLM configured ({provider})")
    return True

def test_environment_file():
    """Test .env file exists and is properly configured"""
    print("\n🔍 Checking .env file...")
    
    env_path = Path('.env')
    if not env_path.exists():
        print("❌ .env file not found")
        print("💡 Copy .env.example to .env and configure it")
        return False
    
    print("✓ .env file exists")
    
    # Load and check basic configuration
    from dotenv import load_dotenv
    load_dotenv()
    
    # Check for critical missing values
    critical_vars = ['LLM_API_KEY', 'DB_NAME']
    missing_vars = []
    
    for var in critical_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"⚠️  Missing environment variables: {missing_vars}")
        print("💡 Please configure these in your .env file")
        return False
    
    return True

def test_oauth_functionality():
    """Test OAuth functionality without authentication"""
    print("\n🔍 Testing OAuth functionality...")
    
    try:
        from local_oauth_service import LocalOAuth2Service
        
        # Test instantiation (will fail if no config, but that's expected)
        try:
            oauth = LocalOAuth2Service()
            print("✓ OAuth service can be instantiated")
        except FileNotFoundError:
            print("⚠️  OAuth not configured, but service code is functional")
        
        return True
        
    except Exception as e:
        print(f"❌ OAuth service error: {e}")
        return False

def test_gmail_extractor():
    """Test Gmail extractor can be imported"""
    print("\n🔍 Testing Gmail extractor...")
    
    try:
        from gmail_oauth_extractor import GmailOAuthExtractor
        extractor = GmailOAuthExtractor()
        print("✓ Gmail extractor can be instantiated")
        return True
    except Exception as e:
        print(f"❌ Gmail extractor error: {e}")
        return False

def main():
    """Run all validation tests"""
    print("🔍 Email Pipeline Installation Validation")
    print("=" * 50)
    
    tests = [
        ("Python Version", test_python_version),
        ("Required Packages", test_required_packages),
        ("Environment File", test_environment_file),
        ("OAuth Configuration", test_oauth_configuration),
        ("Database Connection", test_database_connection),
        ("LLM Configuration", test_llm_configuration),
        ("OAuth Functionality", test_oauth_functionality),
        ("Gmail Extractor", test_gmail_extractor),
    ]
    
    passed = 0
    total = len(tests)
    issues = []
    
    for test_name, test_func in tests:
        print(f"\n{'=' * 20} {test_name} {'=' * (30 - len(test_name))}")
        
        try:
            if test_func():
                passed += 1
            else:
                issues.append(test_name)
        except Exception as e:
            print(f"❌ {test_name} failed with error: {e}")
            issues.append(test_name)
    
    # Final summary
    print(f"\n{'=' * 50}")
    print("🎯 VALIDATION SUMMARY")
    print(f"{'=' * 50}")
    
    print(f"\n✓ Passed: {passed}/{total} tests")
    
    if passed == total:
        print("\n🎉 All validations passed! The email pipeline is ready to use.")
        print("\n🚀 Next steps:")
        print("   1. Run: python gmail_oauth_extractor.py --test")
        print("   2. If test works, run: ./update_emails_v2.sh")
        success = True
    else:
        print(f"\n❌ Failed tests: {issues}")
        print("\n🔧 Please fix the issues above before proceeding.")
        print("\nFor help:")
        print("   - See README.md for setup instructions")
        print("   - Run: python oauth_troubleshoot.py")
        print("   - Check docs/OAUTH_SETUP.md")
        success = False
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)