import logging
import time
import json
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

class SignalGenerator:
    """Signal generator with improved database handling"""

    def __init__(self, db_manager, indicators=None, ai_model=None):
        try:
            # Store components
            self.db_manager = db_manager
            self.indicators = indicators
            self.ai_model = ai_model
            logger.info("Signal generator initialized")
        except Exception as e:
            logger.error(f"Database manager not provided to SignalGenerator: {e}")

    def _init_database(self):
        """Initialize the signal database"""
        if not self.db_manager:
            logger.error("Database manager not provided to SignalGenerator")
            return

        # Create signals table
        self.db_manager.execute_query(
            'signals',
            '''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                direction TEXT,
                entry_price REAL,
                take_profit REAL,
                stop_loss REAL,
                timestamp INTEGER,
                confidence REAL,
                timeframe TEXT,
                model_name TEXT,
                status TEXT DEFAULT 'OPEN',
                exit_price REAL,
                exit_timestamp INTEGER,
                profit_loss REAL
            )
            '''
        )

        # Create signal counts table for limiting signals per symbol
        self.db_manager.execute_query(
            'signals',
            '''
            CREATE TABLE IF NOT EXISTS signal_counts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                count INTEGER DEFAULT 0,
                date TEXT,
                max_count INTEGER DEFAULT 3,
                UNIQUE(symbol, date)
            )
            '''
        )

        # Create indexes for faster queries
        self.db_manager.execute_query(
            'signals',
            'CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals (symbol, status)'
        )

        self.db_manager.execute_query(
            'signals',
            'CREATE INDEX IF NOT EXISTS idx_signal_counts_date ON signal_counts (symbol, date)'
        )

        logger.info("Signal database initialized")

    def generate_signals(self, symbol, timeframes, klines_data):
        """Generate trading signals for a symbol across multiple timeframes"""
        if not self.db_manager or not self.indicators or not self.ai_model:
            logger.error("Required components not available")
            return []

        try:
            # Check if we've hit the signal limit for this symbol today
            if not self._can_generate_signal(symbol):
                return []

            signals = []

            for timeframe in timeframes:
                if timeframe not in klines_data or not klines_data[timeframe]:
                    continue

                # Calculate indicators
                indicators = self.indicators.calculate_indicators(
                    symbol, timeframe, klines_data[timeframe]
                )

                if not indicators:
                    continue

                # Get AI model prediction
                prediction = self.ai_model.predict(symbol, timeframe, indicators)

                if prediction and prediction.get('confidence', 0) >= self.min_confidence:
                    # Create signal object
                    signal = {
                        'symbol': symbol,
                        'direction': prediction.get('direction', 'NONE'),
                        'entry_price': prediction.get('entry_price', 0),
                        'take_profit': prediction.get('take_profit', 0),
                        'stop_loss': prediction.get('stop_loss', 0),
                        'timestamp': int(time.time()),
                        'confidence': prediction.get('confidence', 0),
                        'timeframe': timeframe,
                        'model_name': prediction.get('model_name', 'default'),
                        'status': 'OPEN'
                    }

                    # Save signal to database
                    signal_id = self._save_signal(signal)

                    if signal_id:
                        signal['id'] = signal_id
                        signals.append(signal)

                        # Increment signal count for this symbol
                        self._increment_signal_count(symbol)

                        # Once signal is confirmed, track it in positions
                        if signal['direction'] != 'NONE' and signal['confidence'] > 70:
                            # If position tracking component is available in self.components
                            if hasattr(self, 'components') and 'positions' in self.components:
                                # Check if we're allowed to send more signals for this symbol
                                if self.components['positions'].has_active_position(symbol):
                                    logger.debug("Skipping signal for %s - active position exists", symbol)
                                    continue

                                # Check signal limits
                                if not self.components['positions'].increment_signal_count(symbol):
                                    logger.debug("Skipping signal for %s - daily limit reached", symbol)
                                    continue

                                # Add signal ID
                                signal['id'] = signal_id

                                # Pre-create position (unconfirmed until user responds)
                                position_id = self.components['positions'].add_position(
                                    symbol,
                                    signal['entry_price'],
                                    signal['direction'],
                                    signal['id']
                                )

                                if position_id:
                                    logger.info("Created position for %s with ID: %s", symbol, position_id)
                                else:
                                    logger.warning("Failed to create position for %s", symbol)

            return signals

        except Exception as e:
            logger.error("Error generating signals for %s: %s", symbol, str(e))
            return []

    def _save_signal(self, signal):
        """Save a signal to the database"""
        if not self.db_manager:
            return None

        try:
            result = self.db_manager.execute_query(
                'signals',
                '''
                INSERT INTO signals 
                (symbol, direction, entry_price, take_profit, stop_loss, timestamp, 
                confidence, timeframe, model_name, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                params=(
                    signal['symbol'], signal['direction'], signal['entry_price'],
                    signal['take_profit'], signal['stop_loss'], signal['timestamp'],
                    signal['confidence'], signal['timeframe'], signal['model_name'],
                    signal['status']
                ),
                fetch='lastrowid'
            )

            return result
        except Exception as e:
            logger.error(f"Error saving signal: {e}")
            return None

    def close_signal(self, signal_id, exit_price):
        """Close a signal and record the results"""
        if not self.db_manager:
            return False

        try:
            # Get the signal first
            signal = self.get_signal(signal_id)

            if not signal:
                logger.error(f"Signal {signal_id} not found")
                return False

            # Calculate profit/loss
            entry_price = signal['entry_price']
            direction = signal['direction']

            if direction == 'LONG':
                profit_loss = (exit_price - entry_price) / entry_price * 100
            else:  # SHORT
                profit_loss = (entry_price - exit_price) / entry_price * 100

            # Update signal in database
            self.db_manager.execute_query(
                'signals',
                '''
                UPDATE signals
                SET status = ?, exit_price = ?, exit_timestamp = ?, profit_loss = ?
                WHERE id = ?
                ''',
                params=('CLOSED', exit_price, int(time.time()), profit_loss, signal_id)
            )

            return True
        except Exception as e:
            logger.error(f"Error closing signal {signal_id}: {e}")
            return False

    def get_signal(self, signal_id):
        """Get a signal by ID"""
        if not self.db_manager:
            return None

        try:
            result = self.db_manager.execute_query(
                'signals',
                'SELECT * FROM signals WHERE id = ?',
                params=(signal_id,),
                fetch='one'
            )

            if not result:
                return None

            # Convert row to dictionary
            signal = dict(result)
            return signal
        except Exception as e:
            logger.error(f"Error getting signal {signal_id}: {e}")
            return None

    def get_open_signals(self, symbol=None):
        """Get all open signals, optionally filtered by symbol"""
        if not self.db_manager:
            return []

        try:
            if symbol:
                result = self.db_manager.execute_query(
                    'signals',
                    'SELECT * FROM signals WHERE status = ? AND symbol = ? ORDER BY timestamp DESC',
                    params=('OPEN', symbol),
                    fetch='all'
                )
            else:
                result = self.db_manager.execute_query(
                    'signals',
                    'SELECT * FROM signals WHERE status = ? ORDER BY timestamp DESC',
                    params=('OPEN',),
                    fetch='all'
                )

            # Convert rows to dictionaries
            signals = [dict(row) for row in result]
            return signals
        except Exception as e:
            logger.error(f"Error getting open signals: {e}")
            return []

    def _can_generate_signal(self, symbol):
        """Check if we can generate more signals for this symbol today"""
        if not self.db_manager:
            return False

        try:
            today = datetime.now().strftime("%Y-%m-%d")

            # Get current count
            result = self.db_manager.execute_query(
                'signals',
                'SELECT count, max_count FROM signal_counts WHERE symbol = ? AND date = ?',
                params=(symbol, today),
                fetch='one'
            )

            if not result:
                return True  # No record yet, so we can generate

            count, max_count = result
            return count < max_count
        except Exception as e:
            logger.error(f"Error checking signal count for {symbol}: {e}")
            return False

    def _increment_signal_count(self, symbol):
        """Increment the signal count for a symbol"""
        if not self.db_manager:
            return False

        try:
            today = datetime.now().strftime("%Y-%m-%d")

            # Check if entry exists
            result = self.db_manager.execute_query(
                'signals',
                'SELECT count, max_count FROM signal_counts WHERE symbol = ? AND date = ?',
                params=(symbol, today),
                fetch='one'
            )

            if result:
                # Update existing entry
                count, max_count = result
                max_count = max_count or 3

                if count < max_count:
                    self.db_manager.execute_query(
                        'signals',
                        'UPDATE signal_counts SET count = ? WHERE symbol = ? AND date = ?',
                        params=(count + 1, symbol, today)
                    )
                    return True
                else:
                    return False
            else:
                # Create new entry
                self.db_manager.execute_query(
                    'signals',
                    'INSERT INTO signal_counts (symbol, count, date, max_count) VALUES (?, ?, ?, ?)',
                    params=(symbol, 1, today, 3)
                )
                return True
        except Exception as e:
            logger.error(f"Error incrementing signal count for {symbol}: {e}")
            return False