#!/usr/bin/env python3
"""
Test core OAuth functionality without ML dependencies
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

def test_oauth_core():
    """Test core OAuth components"""
    print("🔍 Testing Core OAuth Implementation")
    print("=" * 50)
    
    # Test 1: Basic imports
    print("\n--- Basic Imports ---")
    try:
        from setup_oauth import OAuthSetup
        print("✓ OAuthSetup import successful")
        
        from local_oauth_service import LocalOAuth2Service
        print("✓ LocalOAuth2Service import successful")
        
        from oauth_error_handler import oauth_error_handler
        print("✓ oauth_error_handler import successful")
        
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False
    
    # Test 2: Configuration directory creation
    print("\n--- Configuration Setup ---")
    try:
        setup = OAuthSetup()
        if setup.config_dir.exists():
            print(f"✓ Config directory exists: {setup.config_dir}")
        else:
            print(f"❌ Config directory not created: {setup.config_dir}")
            return False
    except Exception as e:
        print(f"❌ Setup error: {e}")
        return False
    
    # Test 3: Error handling
    print("\n--- Error Handling ---")
    try:
        error = oauth_error_handler.handle_exception(
            Exception("Test error"), 
            "test context"
        )
        if error.code and error.message and error.solution:
            print("✓ Error handler produces valid error objects")
        else:
            print("❌ Error handler produces invalid error objects")
            return False
    except Exception as e:
        print(f"❌ Error handler error: {e}")
        return False
    
    # Test 4: OAuth service configuration loading (expected to fail)
    print("\n--- OAuth Service Configuration ---")
    try:
        oauth = LocalOAuth2Service()
        print("❌ OAuth service should fail without configuration")
        return False
    except FileNotFoundError:
        print("✓ OAuth service correctly detects missing configuration")
    except Exception as e:
        print(f"❌ Unexpected OAuth error: {e}")
        return False
    
    print("\n✅ All core OAuth tests passed!")
    return True

def test_required_dependencies():
    """Test required dependencies for OAuth"""
    print("\n--- Required Dependencies ---")
    
    required = [
        'cryptography',
        'aiohttp', 
        'json',
        'pathlib',
        'asyncio'
    ]
    
    missing = []
    for dep in required:
        try:
            __import__(dep)
            print(f"✓ {dep}")
        except ImportError:
            print(f"❌ {dep}")
            missing.append(dep)
    
    if missing:
        print(f"❌ Missing dependencies: {missing}")
        return False
    
    return True

def main():
    """Run OAuth core tests"""
    
    success = True
    
    if not test_required_dependencies():
        success = False
    
    if not test_oauth_core():
        success = False
    
    if success:
        print("\n🎉 OAuth core implementation is working correctly!")
        print("\nNext steps for full setup:")
        print("1. Run: python setup_oauth.py")
        print("2. Configure database connection")
        print("3. Set LLM API key in .env")
    else:
        print("\n❌ OAuth core tests failed")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)