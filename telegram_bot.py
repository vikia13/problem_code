import logging
import os
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler

logger = logging.getLogger(__name__)

# Conversation states
SYMBOL, DIRECTION, PRICE = range(3)

class TelegramBot:
    """Telegram bot for trading signals and position management"""

    def __init__(self, token, db_path, allowed_users=None, signal_generator=None, websocket_client=None, db_manager=None):
        self.token = token
        self.db_path = db_path
        self.allowed_users = allowed_users or []
        self.signal_generator = signal_generator
        self.websocket_client = websocket_client
        self.db_manager = db_manager
        self.app = None

        logger.info("Telegram bot initialized")

    async def start(self):
        """Start the Telegram bot"""
        try:
            self.app = Application.builder().token(self.token).build()

            # Add command handlers
            self.app.add_handler(CommandHandler("start", self._start_command))
            self.app.add_handler(CommandHandler("status", self._status_command))
            self.app.add_handler(CommandHandler("signals", self._signals_command))
            self.app.add_handler(CommandHandler("history", self._history_command))
            self.app.add_handler(CommandHandler("stats", self._stats_command))

            # Add position entry handler
            entry_conv_handler = ConversationHandler(
                entry_points=[CommandHandler("enter", self._enter_position_start)],
                states={
                    SYMBOL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._enter_position_symbol)],
                    DIRECTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._enter_position_direction)],
                    PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._enter_position_price)],
                },
                fallbacks=[CommandHandler("cancel", self._enter_position_cancel)],
            )
            self.app.add_handler(entry_conv_handler)

            # Add exit handler
            self.app.add_handler(CommandHandler("exit", self._exit_position))

            # Add help handler
            self.app.add_handler(CommandHandler("help", self._help_command))

            # Start the bot
            await self.app.initialize()
            await self.app.start()
            await self.app.updater.start_polling()

            logger.info("Telegram bot started")

        except Exception as e:
            logger.error(f"Error starting Telegram bot: {e}")
            raise

    async def stop(self):
        """Stop the Telegram bot"""
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
            logger.info("Telegram bot stopped")

    async def _start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user_id = update.effective_user.id
        if user_id not in self.allowed_users:
            await update.message.reply_text("You are not authorized to use this bot.")
            return

        await update.message.reply_text(
            "Welcome to Crypto Trading Bot!\n\n"
            "Available commands:\n"
            "/status - Get system status\n"
            "/signals - Get active signals\n"
            "/history - Get signal history\n"
            "/stats - Get performance statistics\n"
            "/enter - Enter a new position\n"
            "/exit <position_id> - Exit a position\n"
            "/help - Show this help message"
        )

    async def _status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
        user_id = update.effective_user.id
        if user_id not in self.allowed_users:
            return

        try:
            status = self._get_system_status()

            symbols_str = ", ".join(status['symbols'][:5]) + "..." if len(status['symbols']) > 5 else ", ".join(status['symbols'])

            message = f"*System Status*\n\n" \
                      f"Running: {'✅' if status['running'] else '❌'}\n" \
                      f"Active Symbols: {len(status['symbols'])}\n" \
                      f"Sample: {symbols_str}\n" \
                      f"Active Signals: {status['active_signals']}\n" \
                      f"Win Ratio: {status['win_ratio']:.2%}\n" \
                      f"Last Update: {status['last_update']}"

            await update.message.reply_text(message, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error handling status command: {e}")
            await update.message.reply_text("Error retrieving system status")

    async def _signals_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /signals command"""
        user_id = update.effective_user.id
        if user_id not in self.allowed_users:
            return

        try:
            active_signals = self._get_active_signals()

            if not active_signals:
                await update.message.reply_text("No active signals at the moment")
                return

            message = "*Active Signals*\n\n"

            for signal in active_signals:
                message += f"*#{signal['id']} {signal['symbol']}*\n" \
                           f"{signal['direction']} | Entry: {signal['entry_price']}\n" \
                           f"SL: {signal['stop_loss']} | TP: {signal['take_profit']}\n" \
                           f"Status: {signal['status']}\n\n"

            await update.message.reply_text(message, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error handling signals command: {e}")
            await update.message.reply_text("Error retrieving active signals")

    async def _history_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /history command"""
        user_id = update.effective_user.id
        if user_id not in self.allowed_users:
            return

        try:
            history = self._get_signal_history(5)

            if not history:
                await update.message.reply_text("No signal history available")
                return

            message = "*Recent Signals*\n\n"

            for signal in history:
                if signal['outcome'] == 'WIN':
                    emoji = "✅"
                else:
                    emoji = "❌"

                message += f"{emoji} *#{signal['id']} {signal['symbol']}*\n" \
                           f"{signal['direction']} | {signal['profit_pct']:.2f}%\n\n"

            await update.message.reply_text(message, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error handling history command: {e}")
            await update.message.reply_text("Error retrieving signal history")

    async def _stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command"""
        user_id = update.effective_user.id
        if user_id not in self.allowed_users:
            return

        try:
            stats = self._get_performance_stats()

            message = f"*Performance Statistics*\n\n" \
                      f"Total signals: {stats['total']}\n" \
                      f"Win/Loss: {stats['wins']}/{stats['losses']}\n" \
                      f"Win rate: {stats['win_rate']:.2f}%\n" \
                      f"Average profit: {stats['avg_profit']:.2f}%\n" \
                      f"Average loss: {stats['avg_loss']:.2f}%\n" \
                      f"Best trade: {stats['best_trade']:.2f}%\n" \
                      f"Worst trade: {stats['worst_trade']:.2f}%\n"

            await update.message.reply_text(message, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error handling stats command: {e}")
            await update.message.reply_text("Error retrieving performance statistics")

    async def _help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        user_id = update.effective_user.id
        if user_id not in self.allowed_users:
            return

        await update.message.reply_text(
            "Available commands:\n"
            "/status - Get system status\n"
            "/signals - Get active signals\n"
            "/history - Get signal history\n"
            "/stats - Get performance statistics\n"
            "/enter - Enter a new position\n"
            "/exit <position_id> - Exit a position\n"
            "/help - Show this help message"
        )

    # Position entry conversation handlers
    async def _enter_position_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start the enter position conversation"""
        user_id = update.effective_user.id
        if user_id not in self.allowed_users:
            return ConversationHandler.END

        await update.message.reply_text(
            "Let's record your position entry. What's the trading pair symbol? (e.g., BTCUSDT)"
        )
        return SYMBOL

    async def _enter_position_symbol(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the symbol input for position entry"""
        symbol = update.message.text.strip().upper()

        # Check if symbol exists
        if self.websocket_client and not self.websocket_client.get_current_price(symbol):
            await update.message.reply_text(f"Symbol {symbol} not found or not tracked. Please try again or /cancel.")
            return SYMBOL

        context.user_data['entry_symbol'] = symbol

        await update.message.reply_text(
            f"Trading pair: {symbol}\nWhat's your position direction? (LONG or SHORT)"
        )
        return DIRECTION

    async def _enter_position_direction(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the direction input for position entry"""
        direction = update.message.text.strip().upper()

        if direction not in ["LONG", "SHORT"]:
            await update.message.reply_text("Please enter either LONG or SHORT.")
            return DIRECTION

        context.user_data['entry_direction'] = direction

        await update.message.reply_text(
            f"Direction: {direction}\nWhat was your entry price?"
        )
        return PRICE

    async def _enter_position_price(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the price input for position entry"""
        try:
            price = float(update.message.text.strip())

            symbol = context.user_data['entry_symbol']
            direction = context.user_data['entry_direction']

            # Save the position
            position_id = self._save_manual_position(symbol, direction, price)

            if position_id:
                # Calculate suggested exit levels
                current_price = self.websocket_client.get_current_price(symbol) if self.websocket_client else price

                # Get suggestions from AI model if available
                if self.signal_generator:
                    suggestion = self._get_exit_suggestion(symbol, direction, price, current_price)

                    await update.message.reply_text(
                        f"Position #{position_id} recorded:\n"
                        f"Symbol: {symbol}\n"
                        f"Direction: {direction}\n"
                        f"Entry Price: {price}\n\n"
                        f"Current Price: {current_price}\n\n"
                        f"Suggested Take Profit: {suggestion['take_profit']}\n"
                        f"Suggested Stop Loss: {suggestion['stop_loss']}\n\n"
                        f"Use /exit {position_id} to close this position"
                    )
                else:
                    await update.message.reply_text(
                        f"Position #{position_id} recorded:\n"
                        f"Symbol: {symbol}\n"
                        f"Direction: {direction}\n"
                        f"Entry Price: {price}\n\n"
                        f"Use /exit {position_id} to close this position"
                    )
            else:
                await update.message.reply_text("Failed to record position. Please try again.")

        except ValueError:
            await update.message.reply_text("Invalid price. Please enter a numeric value.")
            return PRICE

        # Clear user data
        context.user_data.clear()
        return ConversationHandler.END

    async def _enter_position_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel the enter position conversation"""
        context.user_data.clear()
        await update.message.reply_text("Position entry cancelled.")
        return ConversationHandler.END

    async def _exit_position(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /exit command"""
        user_id = update.effective_user.id
        if user_id not in self.allowed_users:
            return

        try:
            # Get position ID from command
            args = context.args
            if not args or not args[0].isdigit():
                await update.message.reply_text("Please provide a position ID: /exit <position_id>")
                return

            position_id = int(args[0])

            # Get position details
            position = self._get_position(position_id)
            if not position:
                await update.message.reply_text(f"Position #{position_id} not found.")
                return

            # Get current price
            current_price = self.websocket_client.get_current_price(position['symbol']) if self.websocket_client else None

            # Calculate profit/loss
            if current_price:
                if position['direction'] == 'LONG':
                    profit_pct = (current_price - position['entry_price']) / position['entry_price'] * 100
                else:  # SHORT
                    profit_pct = (position['entry_price'] - current_price) / position['entry_price'] * 100

                # Close position
                self._close_position(position_id, current_price, profit_pct)

                # Determine outcome
                outcome = "WIN" if profit_pct > 0 else "LOSS"

                await update.message.reply_text(
                    f"Position #{position_id} closed:\n"
                    f"Symbol: {position['symbol']}\n"
                    f"Direction: {position['direction']}\n"
                    f"Entry Price: {position['entry_price']}\n"
                    f"Exit Price: {current_price}\n"
                    f"Profit/Loss: {profit_pct:.2f}%\n"
                    f"Outcome: {outcome}"
                )
            else:
                await update.message.reply_text(
                    f"Could not get current price for {position['symbol']}. "
                    f"Please provide the exit price as a second argument: /exit {position_id} <price>"
                )
        except Exception as e:
            logger.error(f"Error handling exit command: {e}")
            await update.message.reply_text("Error closing position")

    def _get_system_status(self):
        """Get system status information"""
        try:
            if not self.db_manager:
                logger.error("Database manager not available")
                return {
                    'running': False,
                    'symbols': [],
                    'active_signals': 0,
                    'win_ratio': 0,
                    'last_update': 'Unknown'
                }

            # Get active signals count
            active_count = self.db_manager.execute_query(
                'signals',
                "SELECT COUNT(*) FROM signals WHERE status IN ('PENDING', 'ACTIVE')",
                fetch='one'
            )
            active_count = active_count[0] if active_count else 0

            # Get win/loss counts
            win_count = self.db_manager.execute_query(
                'signals',
                "SELECT COUNT(*) FROM signals WHERE status = 'WIN'",
                fetch='one'
            )
            win_count = win_count[0] if win_count else 0

            loss_count = self.db_manager.execute_query(
                'signals',
                "SELECT COUNT(*) FROM signals WHERE status = 'LOSS'",
                fetch='one'
            )
            loss_count = loss_count[0] if loss_count else 0

            # Calculate win ratio
            win_ratio = win_count / (win_count + loss_count) if (win_count + loss_count) > 0 else 0

            # Get active symbols
            symbols = []
            if self.websocket_client:
                symbols = self.websocket_client.get_active_symbols() or []

            return {
                'running': True,  # Assume system is running if the bot is running
                'symbols': symbols,
                'active_signals': active_count,
                'win_ratio': win_ratio,
                'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                'running': False,
                'symbols': [],
                'active_signals': 0,
                'win_ratio': 0,
                'last_update': 'Unknown'
            }

    def _get_active_signals(self):
        """Get active trading signals"""
        try:
            if not self.db_manager:
                logger.error("Database manager not available")
                return []

            results = self.db_manager.execute_query(
                'signals',
                '''
                SELECT id, symbol, direction, entry_price, stop_loss, take_profit, status
                FROM signals
                WHERE status IN ('PENDING', 'ACTIVE')
                ORDER BY timestamp DESC
                ''',
                fetch='all'
            )

            if not results:
                return []

            return [
                {
                    'id': row[0],
                    'symbol': row[1],
                    'direction': row[2],
                    'entry_price': row[3],
                    'stop_loss': row[4],
                    'take_profit': row[5],
                    'status': row[6]
                }
                for row in results
            ]
        except Exception as e:
            logger.error(f"Error getting active signals: {e}")
            return []

    def _get_signal_history(self, limit=10):
        """Get recent signal history"""
        try:
            if not self.db_manager:
                logger.error("Database manager not available")
                return []

            results = self.db_manager.execute_query(
                'signals',
                '''
                SELECT id, symbol, direction, status as outcome, profit_pct
                FROM signals
                WHERE status IN ('WIN', 'LOSS')
                ORDER BY closed_at DESC
                LIMIT ?
                ''',
                params=(limit,),
                fetch='all'
            )

            if not results:
                return []

            return [
                {
                    'id': row[0],
                    'symbol': row[1],
                    'direction': row[2],
                    'outcome': row[3],
                    'profit_pct': row[4]
                }
                for row in results
            ]
        except Exception as e:
            logger.error(f"Error getting signal history: {e}")
            return []

    def _get_performance_stats(self):
        """Get performance statistics"""
        try:
            if not self.db_manager:
                logger.error("Database manager not available")
                return {
                    'total': 0,
                    'wins': 0,
                    'losses': 0,
                    'win_rate': 0,
                    'avg_profit': 0,
                    'avg_loss': 0,
                    'best_trade': 0,
                    'worst_trade': 0
                }

            # Get total signals
            total = self.db_manager.execute_query(
                'signals',
                "SELECT COUNT(*) FROM signals",
                fetch='one'
            )
            total = total[0] if total else 0

            # Get wins and losses
            wins = self.db_manager.execute_query(
                'signals',
                "SELECT COUNT(*) FROM signals WHERE status = 'WIN'",
                fetch='one'
            )
            wins = wins[0] if wins else 0

            losses = self.db_manager.execute_query(
                'signals',
                "SELECT COUNT(*) FROM signals WHERE status = 'LOSS'",
                fetch='one'
            )
            losses = losses[0] if losses else 0

            # Calculate win rate
            win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0

            # Get average profit
            avg_profit = self.db_manager.execute_query(
                'signals',
                "SELECT AVG(profit_pct) FROM signals WHERE status = 'WIN'",
                fetch='one'
            )
            avg_profit = avg_profit[0] or 0

            # Get average loss
            avg_loss = self.db_manager.execute_query(
                'signals',
                "SELECT AVG(profit_pct) FROM signals WHERE status = 'LOSS'",
                fetch='one'
            )
            avg_loss = avg_loss[0] or 0

            # Get best and worst trades
            best_trade = self.db_manager.execute_query(
                'signals',
                "SELECT MAX(profit_pct) FROM signals",
                fetch='one'
            )
            best_trade = best_trade[0] or 0

            worst_trade = self.db_manager.execute_query(
                'signals',
                "SELECT MIN(profit_pct) FROM signals WHERE profit_pct IS NOT NULL",
                fetch='one'
            )
            worst_trade = worst_trade[0] or 0

            return {
                'total': total,
                'wins': wins,
                'losses': losses,
                'win_rate': win_rate,
                'avg_profit': avg_profit,
                'avg_loss': avg_loss,
                'best_trade': best_trade,
                'worst_trade': worst_trade
            }
        except Exception as e:
            logger.error(f"Error getting performance stats: {e}")
            return {
                'total': 0,
                'wins': 0,
                'losses': 0,
                'win_rate': 0,
                'avg_profit': 0,
                'avg_loss': 0,
                'best_trade': 0,
                'worst_trade': 0
            }

    def _save_manual_position(self, symbol, direction, entry_price):
        """Save a manually entered position"""
        try:
            if not self.db_manager:
                logger.error("Database manager not available")
                return None

            # Create manual positions table if it doesn't exist
            self.db_manager.execute_query(
                'signals',
                '''
                CREATE TABLE IF NOT EXISTS manual_positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    take_profit REAL,
                    stop_loss REAL,
                    entry_time INTEGER NOT NULL,
                    exit_price REAL,
                    exit_time INTEGER,
                    profit_pct REAL,
                    status TEXT DEFAULT 'ACTIVE'
                )
                '''
            )

            # Generate suggested TP/SL
            suggestion = self._get_exit_suggestion(symbol, direction, entry_price)

            # Insert position
            result = self.db_manager.execute_query(
                'signals',
                '''
                INSERT INTO manual_positions 
                (symbol, direction, entry_price, take_profit, stop_loss, entry_time)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                params=(
                    symbol,
                    direction,
                    entry_price,
                    suggestion['take_profit'],
                    suggestion['stop_loss'],
                    int(time.time())
                ),
                fetch='lastrowid'
            )

            return result
        except Exception as e:
            logger.error(f"Error saving manual position: {e}")
            return None

    def _get_position(self, position_id):
        """Get details of a manual position"""
        try:
            if not self.db_manager:
                logger.error("Database manager not available")
                return None

            result = self.db_manager.execute_query(
                'signals',
                '''
                SELECT id, symbol, direction, entry_price, take_profit, stop_loss, entry_time, status
                FROM manual_positions
                WHERE id = ? AND status = 'ACTIVE'
                ''',
                params=(position_id,),
                fetch='one'
            )

            if not result:
                return None

            return {
                'id': result[0],
                'symbol': result[1],
                'direction': result[2],
                'entry_price': result[3],
                'take_profit': result[4],
                'stop_loss': result[5],
                'entry_time': result[6],
                'status': result[7]
            }
        except Exception as e:
            logger.error(f"Error getting position: {e}")
            return None

    def _close_position(self, position_id, exit_price, profit_pct):
        """Close a manual position"""
        try:
            if not self.db_manager:
                logger.error("Database manager not available")
                return False

            # Determine outcome
            status = 'WIN' if profit_pct > 0 else 'LOSS'

            # Update position
            self.db_manager.execute_query(
                'signals',
                '''
                UPDATE manual_positions
                SET exit_price = ?, exit_time = ?, profit_pct = ?, status = ?
                WHERE id = ?
                ''',
                params=(
                    exit_price,
                    int(time.time()),
                    profit_pct,
                    status,
                    position_id
                )
            )

            return True
        except Exception as e:
            logger.error(f"Error closing position: {e}")
            return False

    def _get_exit_suggestion(self, symbol, direction, entry_price, current_price=None):
        """Get suggested exit levels from the AI model"""
        try:
            # Default suggestions based on simple risk management
            if direction == 'LONG':
                take_profit = entry_price * 1.03  # 3% profit
                stop_loss = entry_price * 0.98    # 2% loss
            else:  # SHORT
                take_profit = entry_price * 0.97  # 3% profit
                stop_loss = entry_price * 1.02    # 2% loss

            # If we have an AI model, try to get better suggestions
            if self.signal_generator and hasattr(self.signal_generator, 'ai_model'):
                # Get technical indicators
                if hasattr(self.signal_generator, 'indicators'):
                    indicators = self.signal_generator.indicators.get_latest_indicators(symbol, '1h')
                    if indicators:
                        # Ask AI model for prediction
                        prediction = self.signal_generator.ai_model.predict(symbol, '1h', indicators)
                        if prediction and prediction['direction'] == direction:
                            take_profit = prediction['take_profit']
                            stop_loss = prediction['stop_loss']

            return {
                'take_profit': take_profit,
                'stop_loss': stop_loss
            }
        except Exception as e:
            logger.error(f"Error getting exit suggestion: {e}")
            # Return default values
            if direction == 'LONG':
                return {'take_profit': entry_price * 1.03, 'stop_loss': entry_price * 0.98}
            else:  # SHORT
                return {'take_profit': entry_price * 0.97, 'stop_loss': entry_price * 1.02}