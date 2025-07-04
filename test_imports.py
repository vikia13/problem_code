#!/usr/bin/env python3
"""Test script to verify all imports work correctly"""

def test_basic_dependencies():
    """Test basic Python dependencies"""
    try:
        import numpy
        import pandas
        import requests
        import sqlite3
        print("✅ Basic dependencies available")
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        return False

def test_custom_modules():
    """Test custom module imports"""
    try:
        from telegram_adapter import TelegramAdapter
        from ai_model_enhanced import EnhancedAIModel
        from database_adapter import DatabaseAdapter
        print("✅ All custom modules import successfully")
        return True
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

def test_telegram_adapter():
    """Test TelegramAdapter functionality"""
    try:
        from telegram_adapter import TelegramAdapter
        adapter = TelegramAdapter("test_token", ["123456"])
        print("✅ TelegramAdapter instantiated successfully")
        return True
    except Exception as e:
        print(f"❌ TelegramAdapter error: {e}")
        return False

def test_database_adapter():
    """Test DatabaseAdapter functionality"""
    try:
        from database_adapter import DatabaseAdapter
        db = DatabaseAdapter("test_data")
        print("✅ DatabaseAdapter instantiated successfully")
        return True
    except Exception as e:
        print(f"❌ DatabaseAdapter error: {e}")
        return False

if __name__ == "__main__":
    print("Testing AI Binance Screener imports...")
    
    all_tests = [
        test_basic_dependencies,
        test_custom_modules,
        test_telegram_adapter,
        test_database_adapter
    ]
    
    passed = 0
    for test in all_tests:
        if test():
            passed += 1
    
    print(f"\n{passed}/{len(all_tests)} tests passed")
    
    if passed == len(all_tests):
        print("🎉 All tests passed! The bot is ready to run.")
    else:
        print("⚠️ Some tests failed. Check the errors above.")
