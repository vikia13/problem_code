import logging
import threading
import time
import requests

logger = logging.getLogger(__name__)

class TelegramAdapter:
    """Simple Telegram bot implementation without external dependencies"""

    def __init__(self, token, allowed_users=None, components=None):
        self.token = token
        self.allowed_users = [str(user_id) for user_id in allowed_users] if allowed_users else []
        self.components = components or {}
        self.base_url = f"https://api.telegram.org/bot{token}/"
        self.running = False
        self.last_update_id = 0
        self.polling_thread = None

        logger.info("Telegram bot initialized successfully")

    def start(self):
        """Start the Telegram bot polling thread"""
        if self.running:
            return

        self.running = True
        self.polling_thread = threading.Thread(target=self._poll_updates)
        self.polling_thread.daemon = True
        self.polling_thread.start()

        # Send startup message to all allowed users
        for user_id in self.allowed_users:
            self.send_message(user_id, "🤖 Trading bot started and ready to receive commands!")

        return True

    def stop(self):
        """Stop the Telegram bot"""
        if not self.running:
            return

        self.running = False

        # Send shutdown message to all allowed users
        for user_id in self.allowed_users:
            self.send_message(user_id, "🛑 Trading bot is shutting down")

        if self.polling_thread and self.polling_thread.is_alive():
            self.polling_thread.join(timeout=5)

        logger.info("Telegram bot stopped")

    def _poll_updates(self):
        """Poll for updates from Telegram API"""
        logger.info("Starting Telegram update polling")

        while self.running:
            try:
                updates = self._get_updates()

                if updates:
                    for update in updates:
                        self._process_update(update)
                        self.last_update_id = max(self.last_update_id, update['update_id'] + 1)

                # Sleep to avoid hitting Telegram API limits
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error polling Telegram updates: {e}")
                time.sleep(5)  # Wait longer if there's an error

    def _get_updates(self):
        """Get updates from Telegram API"""
        params = {
            'offset': self.last_update_id,
            'timeout': 30
        }

        try:
            response = requests.get(f"{self.base_url}getUpdates", params=params, timeout=35)
            data = response.json()

            if data['ok']:
                return data['result']
            else:
                logger.error(f"Error getting updates: {data}")
                return []
        except Exception as e:
            logger.error(f"Error in getUpdates request: {e}")
            return []

    def _process_update(self, update):
        """Process an update from Telegram"""
        if 'message' not in update:
            return

        message = update['message']

        if 'text' not in message:
            return

        text = message['text']
        chat_id = str(message['chat']['id'])
        user_id = str(message['from']['id'])

        # Check if user is authorized
        if self.allowed_users and user_id not in self.allowed_users:
            logger.warning(f"Unauthorized access attempt from user ID: {user_id}")
            self.send_message(chat_id, "⛔ You are not authorized to use this bot.")
            return

        # Process commands
        if text.startswith('/'):
            command = text.split()[0].lower()

            if command == '/help':
                self._send_help(chat_id)
            elif command == '/status':
                self._send_status(chat_id)
            elif command == '/symbols':
                self._send_symbols(chat_id)
            else:
                self.send_message(chat_id, f"Unknown command: {command}\nType /help for available commands.")
        else:
            self._handle_position_confirmation(text, chat_id)

    def _send_help(self, chat_id):
        """Send help message"""
        help_text = (
            "🤖 *Trading Bot Commands*\n\n"
            "/status - Get current bot status\n"
            "/symbols - List active trading pairs\n"
            "/help - Show this help message"
        )
        self.send_message(chat_id, help_text)

    def _send_status(self, chat_id):
        """Send bot status"""
        status_text = "🤖 *Trading Bot Status*\n\n"

        # Get WebSocket status
        if 'websocket' in self.components:
            ws_status = "Running" if self.components['websocket'].is_running() else "Not running"
            status_text += f"WebSocket Client: {ws_status}\n"

        # Get active symbols count
        if 'websocket' in self.components:
            symbols = self.components['websocket'].get_active_symbols()
            status_text += f"Active Symbols: {len(symbols) if symbols else 0}\n"

        self.send_message(chat_id, status_text)

    def _send_symbols(self, chat_id):
        """Send list of active symbols"""
        if 'websocket' in self.components:
            symbols = self.components['websocket'].get_active_symbols()
            if symbols:
                symbols_text = "📊 *Active Trading Pairs*\n\n"
                symbols_text += "\n".join(symbols[:20])  # First 20 only

                if len(symbols) > 20:
                    symbols_text += f"\n\n...and {len(symbols) - 20} more"

                self.send_message(chat_id, symbols_text)
            else:
                self.send_message(chat_id, "No active symbols available")
        else:
            self.send_message(chat_id, "WebSocket client not available")

    def _handle_position_confirmation(self, text, chat_id):
        """Handle position confirmation messages like 'id:123 ok'"""
        import re

        pattern = r'id:(\d+)\s*ok'
        match = re.search(pattern, text.lower())

        if match:
            signal_id = int(match.group(1))

            if 'database' in self.components:
                try:
                    success = self.components['database'].confirm_position(signal_id)

                    if success:
                        self.send_message(chat_id, f"✅ Position confirmed for signal ID: {signal_id}")
                        logger.info(f"Position confirmed for signal ID: {signal_id}")
                    else:
                        self.send_message(chat_id, f"❌ Could not confirm position for signal ID: {signal_id}")
                        logger.warning(f"Failed to confirm position for signal ID: {signal_id}")
                except Exception as e:
                    logger.error(f"Error confirming position: {e}")
                    self.send_message(chat_id, f"❌ Error confirming position: {str(e)}")

    def send_signal_notification(self, signal):
        """Send a trading signal notification to all allowed users"""
        try:
            symbol = signal.get('symbol', 'Unknown')
            direction = signal.get('direction', 'Unknown')
            confidence = signal.get('confidence', 0)
            entry_price = signal.get('entry_price', 0)
            signal_id = signal.get('signal_id', 'Unknown')

            emoji = "🟢" if direction == "LONG" else "🔴"
            message = (
                f"{emoji} *{direction} Signal* - {symbol}\n\n"
                f"📊 Entry Price: ${entry_price:.4f}\n"
                f"🎯 Confidence: {confidence:.2f}\n"
                f"🆔 Signal ID: {signal_id}\n\n"
                f"Reply with `id:{signal_id} ok` to confirm position entry"
            )

            for user_id in self.allowed_users:
                self.send_message(user_id, message)

            logger.info(f"Signal notification sent for {symbol} {direction}")
            return True
        except Exception as e:
            logger.error(f"Error sending signal notification: {e}")
            return False

    def send_message(self, chat_id, text, parse_mode="Markdown"):
        """Send message to Telegram chat"""
        try:
            params = {
                'chat_id': chat_id,
                'text': text,
                'parse_mode': parse_mode
            }

            response = requests.post(f"{self.base_url}sendMessage", json=params, timeout=10)
            data = response.json()

            if not data['ok']:
                logger.error(f"Error sending message: {data}")

            return data['ok']
        except Exception as e:
            logger.error(f"Error sending message to {chat_id}: {e}")
            return False