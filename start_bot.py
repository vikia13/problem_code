#!/usr/bin/env python3
"""
Startup script for the AI Binance Screener Bot
"""
import os
import sys
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if all required dependencies are installed"""
    try:
        import numpy
        import pandas
        import requests
        import websocket
        import sqlite3
        logger.info("All dependencies are available")
        return True
    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        return False

def install_dependencies():
    """Install dependencies from requirements.txt"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        logger.info("Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install dependencies: {e}")
        return False

def main():
    """Main startup function"""
    logger.info("Starting AI Binance Screener Bot...")
    
    if not os.path.exists('.env'):
        logger.error(".env file not found. Please create it with TELEGRAM_TOKEN and TELEGRAM_CHAT_ID")
        return False
    
    if not check_dependencies():
        logger.info("Installing missing dependencies...")
        if not install_dependencies():
            return False
    
    try:
        from main_enhanced import TradingBot
        
        from dotenv import load_dotenv
        load_dotenv()
        
        telegram_token = os.getenv('TELEGRAM_TOKEN')
        telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not telegram_token:
            logger.error("TELEGRAM_TOKEN not found in .env file")
            return False
        
        allowed_users = [int(telegram_chat_id)] if telegram_chat_id else []
        
        bot = TradingBot(telegram_token=telegram_token, allowed_users=allowed_users)
        bot.run_forever()
        
    except Exception as e:
        logger.error(f"Error starting bot: {e}")
        return False

if __name__ == "__main__":
    main()
