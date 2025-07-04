#!/usr/bin/env python3
"""Test script to verify bot functionality without requiring live credentials"""

import os
import sys
import logging
from unittest.mock import Mock, patch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_ai_model_initialization():
    """Test AI model can be initialized properly"""
    try:
        from ai_model_enhanced import EnhancedAIModel
        from database_adapter import DatabaseAdapter
        
        db_manager = DatabaseAdapter('test_data')
        
        ai_model = EnhancedAIModel(db_manager)
        
        print("✅ AI model initialized successfully")
        return True
    except Exception as e:
        print(f"❌ AI model initialization failed: {e}")
        return False

def test_telegram_signal_notification():
    """Test Telegram signal notification formatting"""
    try:
        from telegram_adapter import TelegramAdapter
        
        adapter = TelegramAdapter("test_token", ["123456"])
        
        test_signal = {
            'symbol': 'BTCUSDT',
            'direction': 'LONG',
            'confidence': 0.75,
            'entry_price': 45234.56,
            'signal_id': 123
        }
        
        with patch.object(adapter, 'send_message', return_value=True) as mock_send:
            result = adapter.send_signal_notification(test_signal)
            
            if result and mock_send.called:
                print("✅ Signal notification formatting works")
                return True
            else:
                print("❌ Signal notification failed")
                return False
                
    except Exception as e:
        print(f"❌ Telegram signal notification test failed: {e}")
        return False

def test_position_confirmation_parsing():
    """Test position confirmation message parsing"""
    try:
        from telegram_adapter import TelegramAdapter
        
        adapter = TelegramAdapter("test_token", ["123456"])
        
        with patch('database_adapter.DatabaseAdapter') as mock_db_class:
            mock_db = Mock()
            mock_db.confirm_position.return_value = True
            mock_db_class.return_value = mock_db
            
            with patch.object(adapter, 'send_message', return_value=True):
                adapter._handle_position_confirmation("id:123 ok", "test_chat")
                
                if mock_db.confirm_position.called:
                    print("✅ Position confirmation parsing works")
                    return True
                else:
                    print("❌ Position confirmation parsing failed")
                    return False
                    
    except Exception as e:
        print(f"❌ Position confirmation test failed: {e}")
        return False

def test_database_operations():
    """Test basic database operations"""
    try:
        from database_adapter import DatabaseAdapter
        
        db = DatabaseAdapter('test_data')
        
        position_id = db.add_position('BTCUSDT', 45000.0, 'LONG', 123)
        
        if position_id:
            success = db.confirm_position(1)
            
            if success:
                print("✅ Database operations work")
                return True
            else:
                print("❌ Position confirmation failed")
                return False
        else:
            print("❌ Position creation failed")
            return False
            
    except Exception as e:
        print(f"❌ Database operations test failed: {e}")
        return False

def test_main_enhanced_imports():
    """Test that main_enhanced.py can import all required components"""
    try:
        from main_enhanced import TradingBot
        
        print("✅ Main enhanced imports work")
        return True
    except Exception as e:
        print(f"❌ Main enhanced import failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing AI Binance Screener functionality...")
    
    tests = [
        test_main_enhanced_imports,
        test_ai_model_initialization,
        test_telegram_signal_notification,
        test_position_confirmation_parsing,
        test_database_operations
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
        print()  # Add spacing between tests
    
    print(f"Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All functionality tests passed! The bot is ready for integration testing.")
    else:
        print("⚠️ Some tests failed. Check the errors above.")
