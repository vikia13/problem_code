import logging
import threading
import time
import requests
import re  # Need to add this for regular expression matching
from datetime import datetime  # For timestamp formatting

logger = logging.getLogger(__name__)

class TelegramAdapter:
    """Simple Telegram bot implementation without external dependencies"""

    def __init__(self, token, allowed_users=None, components=None):
        self.token = token
        self.allowed_users = [str(user_id) for user_id in allowed_users] if allowed_users else []
        self.components = components or {}
        self.base_url = "https://api.telegram.org/bot{}/".format(token)  # Python 2.7 compatible
        self.running = False
        self.last_update_id = 0
        self.polling_thread = None

        logger.info("Telegram bot initialized successfully")

    def start(self):
        """Start the Telegram bot"""
        try:
            # First, delete any active webhook
            response = requests.get(f"{self.base_url}deleteWebhook")
            if response.ok:
                logger.info("Successfully deleted any existing webhook")
            else:
                logger.warning(f"Failed to delete webhook: {response.text}")

            # Wait a moment for webhook deletion to complete
            time.sleep(1)

            # Then proceed with normal startup
            self.running = True
            self.update_thread = threading.Thread(target=self._poll_updates)
            self.update_thread.daemon = True
            self.update_thread.start()

            self.initialized = True
            return True
        except Exception as e:
            logger.error(f"Failed to initialize TelegramBot: {e}")
            return False

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
                logger.error("Error polling Telegram updates: %s", e)
                time.sleep(5)  # Wait longer if there's an error

    def _get_updates(self):
        """Get updates from Telegram API"""
        params = {
            'offset': self.last_update_id,
            'timeout': 30
        }

        try:
            response = requests.get("{}getUpdates".format(self.base_url), params=params, timeout=35)
            data = response.json()

            if data['ok']:
                return data['result']
            else:
                logger.error("Error getting updates: %s", data)
                return []
        except Exception as e:
            logger.error("Error in getUpdates request: %s", e)
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
            logger.warning("Unauthorized access attempt from user ID: %s", user_id)
            self.send_message(chat_id, "⛔ You are not authorized to use this bot.")
            return

        # Process position confirmations (format: ID:ok)
        position_match = re.match(r'(\d+):ok', text)
        if position_match and 'positions' in self.components:
            position_id = int(position_match.group(1))
            if self.components['positions'].confirm_position(position_id):
                # Get position symbol
                symbol = None
                for sym, pos in self.components['positions'].active_positions.items():
                    if pos['id'] == position_id or pos['signal_id'] == position_id:
                        symbol = sym
                        break

                self.send_message(chat_id, "You have entered a position, in a {} coin with ID: {}.".format(symbol, position_id))
            else:
                self.send_message(chat_id, "Could not confirm position with ID: {}.".format(position_id))
            return

        # Process signal limit settings (format: Signal: N)
        signal_match = re.match(r'Signal:\s*(\d+)', text)
        if signal_match and 'positions' in self.components:
            limit = int(signal_match.group(1))

            # Find the most recently mentioned symbol
            recent_symbol = None

            # Try to get the most recent signal's symbol
            for sym in self.components['positions'].active_positions:
                recent_symbol = sym
                break

            if recent_symbol:
                self.components['positions'].update_signal_limit(recent_symbol, limit)
                self.send_message(chat_id, "for the same coin you put a limit on receiving a message in Telegram: maximum {} messages within 24 hours.".format(limit))
            else:
                self.send_message(chat_id, "Could not determine which symbol to set limit for.")
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
            elif command == '/positions':
                self._send_positions(chat_id)
            elif command == '/report':
                self._send_report(chat_id)
            else:
                self.send_message(chat_id, "Unknown command: {}\nType /help for available commands.".format(command))

    def _send_help(self, chat_id):
        """Send help message"""
        help_text = (
            "🤖 *Trading Bot Commands*\n\n"
            "/status - Get current bot status\n"
            "/symbols - List active trading pairs\n"
            "/positions - View your active positions\n"
            "/report - Generate weekly performance report\n"
            "/help - Show this help message\n\n"
            "*Position Responses:*\n"
            "- To confirm a position entry: `ID:ok`\n"
            "- To set signal limit: `Signal: N`"
        )
        self.send_message(chat_id, help_text)

    def _send_status(self, chat_id):
        """Send bot status"""
        status_text = "🤖 *Trading Bot Status*\n\n"

        # Get WebSocket status
        if 'websocket' in self.components:
            ws_status = "Running" if self.components['websocket'].is_running() else "Not running"
            status_text += "WebSocket Client: {}\n".format(ws_status)

        # Get active symbols count
        if 'websocket' in self.components:
            symbols = self.components['websocket'].get_active_symbols()
            status_text += "Active Symbols: {}\n".format(len(symbols) if symbols else 0)

        self.send_message(chat_id, status_text)

    def _send_symbols(self, chat_id):
        """Send list of active symbols"""
        if 'websocket' in self.components:
            symbols = self.components['websocket'].get_active_symbols()
            if symbols:
                symbols_text = "📊 *Active Trading Pairs*\n\n"
                symbols_text += "\n".join(symbols[:20])  # First 20 only

                if len(symbols) > 20:
                    symbols_text += "\n\n...and {} more".format(len(symbols) - 20)

                self.send_message(chat_id, symbols_text)
            else:
                self.send_message(chat_id, "No active symbols available")
        else:
            self.send_message(chat_id, "WebSocket client not available")

    def send_message(self, chat_id, text, parse_mode="Markdown"):
        """Send message to Telegram chat"""
        try:
            params = {
                'chat_id': chat_id,
                'text': text,
                'parse_mode': parse_mode
            }

            response = requests.post("{}sendMessage".format(self.base_url), json=params, timeout=10)
            data = response.json()

            if not data['ok']:
                logger.error("Error sending message: %s", data)

            return data['ok']
        except Exception as e:
            logger.error("Error sending message to %s: %s", chat_id, e)
            return False

    # Add new methods below
    def send_signal_notification(self, signal):
        """Send trading signal notification to all allowed users"""
        if not signal:
            return False

        symbol = signal.get('symbol')
        direction = signal.get('direction', 'NEUTRAL')
        price = signal.get('price', 0)
        price_change = signal.get('price_change', 0)
        signal_id = signal.get('id', 0)

        # Get signal count for the day
        signal_count = 1
        if 'positions' in self.components:
            # Increment count in position tracker
            self.components['positions'].increment_signal_count(symbol)

            # Get current count
            if symbol in self.components['positions'].signal_limits:
                signal_count = self.components['positions'].signal_limits[symbol]['count']

        # Format trend emoji
        trend_emoji = "🔴 " if direction == "SHORT" else ""

        # Calculate duration (placeholder)
        duration = "1:13 minutes"

        # Build message
        message = """📊 {0} +HTLM LINK binance futures

⏰ Time: {1}

📈 Trend: {2}{3}

💹 Price Change: {4:.2f}%, price: {5:.2f}

⏱ Duration: {6}

ID: {7}

Signal:24h: {8}

------------------------""".format(
            symbol,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            trend_emoji,
            direction,
            price_change,
            price,
            duration,
            signal_id,
            signal_count
        )

        # Send to all allowed users
        success = True
        for user_id in self.allowed_users:
            if not self.send_message(user_id, message):
                success = False

        return success

    def send_exit_notification(self, exit_data):
        """Send exit recommendation notification"""
        if not exit_data:
            return False

        position_id = exit_data.get('position_id', 0)
        symbol = exit_data.get('symbol', 'UNKNOWN')
        profit_pct = exit_data.get('profit_pct', 0)
        exit_price = exit_data.get('exit_price', 0)
        reason = exit_data.get('reason', 'Unknown reason')

        # Get signal count
        signal_count = 1
        if 'positions' in self.components and symbol in self.components['positions'].signal_limits:
            signal_count = self.components['positions'].signal_limits[symbol]['count']

        # Calculate duration
        duration = "5:43 minutes"  # Placeholder
        if 'positions' in self.components and symbol in self.components['positions'].active_positions:
            entry_time = self.components['positions'].active_positions[symbol].get('entry_time')
            if entry_time:
                try:
                    entry_dt = datetime.strptime(entry_time, '%Y-%m-%d %H:%M:%S')
                    duration_mins = int((datetime.now() - entry_dt).total_seconds() / 60)
                    duration_secs = int((datetime.now() - entry_dt).total_seconds() % 60)
                    duration = "{}:{:02d} minutes".format(duration_mins, duration_secs)
                except:
                    pass

        # Format trend emoji
        trend_emoji = "🔴 " if profit_pct < 0 else ""
        direction = "SHORT" if profit_pct < 0 else "LONG"

        message = """🚨 WARN - EXIT POSITION ID:{0} {1}🚨

📊 {1} +HTLM LINK binance futures

⏰ Time: {2}

📈 Trend: {3}{4}

💹 Price Change: {5:.2f}%, price: {6:.2f}

⏱ Duration: {7}

ID: {0}

Signal:24h: {8}

------------------------""".format(
            position_id,
            symbol,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            trend_emoji,
            direction,
            profit_pct,
            exit_price,
            duration,
            signal_count
        )

        # Send to all allowed users
        success = True
        for user_id in self.allowed_users:
            if not self.send_message(user_id, message):
                success = False

        return success

    def _send_positions(self, chat_id):
        """Send active positions information"""
        if 'positions' not in self.components:
            self.send_message(chat_id, "Position tracking is not available.")
            return

        positions = self.components['positions'].get_position_summary()

        if not positions:
            self.send_message(chat_id, "You have no active positions.")
            return

        message = "*Your Active Positions*\n\n"

        for symbol, position in positions.items():
            direction = position['direction']
            entry_price = position['entry_price']
            position_id = position['id']
            status = "✅ Confirmed" if position.get('confirmed', False) else "⏳ Pending"

            message += "ID: {} - {} {} @ ${:.4f} - {}\n".format(
                position_id, symbol, direction, entry_price, status
            )

        self.send_message(chat_id, message)

    def _send_report(self, chat_id):
        """Send weekly performance report"""
        if 'positions' not in self.components:
            self.send_message(chat_id, "Position tracking is not available.")
            return

        report = self.components['positions'].generate_weekly_report()
        self.send_message(chat_id, report)