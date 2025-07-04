import os
import sqlite3
import logging
import time
from datetime import datetime, timedelta  # Add timedelta import here

logger = logging.getLogger(__name__)

class PositionTracker:
    """Tracks trading positions and provides exit recommendations"""

    def __init__(self, db_path='data'):
        self.db_path = db_path
        self.positions_db = os.path.join(db_path, 'positions.db')
        self.active_positions = {}
        self.signal_limits = {}  # Format: {symbol: {"limit": N, "count": M, "last_reset": timestamp}}

        # Ensure database exists and has required tables
        self._initialize_database()

        # Load active positions from database
        self._load_active_positions()

        logger.info("Position tracker initialized")

    def _initialize_database(self):
        """Initialize the positions database if it doesn't exist"""
        try:
            conn = sqlite3.connect(self.positions_db)
            cursor = conn.cursor()

            # Create positions table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                position_type TEXT NOT NULL,
                entry_price REAL NOT NULL,
                entry_time TEXT NOT NULL,
                exit_price REAL,
                exit_time TEXT,
                status TEXT NOT NULL,
                confirmed INTEGER DEFAULT 0,
                profit_loss_percent REAL,
                exit_reason TEXT,
                signal_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            # Create signal limits table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS signal_limits (
                symbol TEXT PRIMARY KEY,
                limit_value INTEGER DEFAULT 5,
                count INTEGER DEFAULT 0,
                last_reset TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            conn.commit()
            conn.close()

            logger.info("Positions database initialized")
        except Exception as e:
            logger.error(f"Error initializing positions database: {e}")
            raise

    def _load_active_positions(self):
        """Load active positions from database"""
        try:
            conn = sqlite3.connect(self.positions_db)
            cursor = conn.cursor()

            # Get active positions
            cursor.execute('''
            SELECT id, symbol, position_type, entry_price, entry_time, signal_id, confirmed
            FROM positions
            WHERE status = 'OPEN'
            ''')

            rows = cursor.fetchall()

            for row in rows:
                position_id, symbol, position_type, entry_price, entry_time, signal_id, confirmed = row

                self.active_positions[symbol] = {
                    'id': position_id,
                    'direction': position_type,
                    'entry_price': entry_price,
                    'entry_time': entry_time,
                    'signal_id': signal_id,
                    'confirmed': confirmed == 1
                }

            # Load signal limits
            cursor.execute('SELECT symbol, limit_value, count, last_reset FROM signal_limits')

            for row in cursor.fetchall():
                symbol, limit_value, count, last_reset = row
                self.signal_limits[symbol] = {
                    "limit": limit_value,
                    "count": count,
                    "last_reset": datetime.strptime(last_reset, '%Y-%m-%d %H:%M:%S').timestamp()
                }

            conn.close()

            logger.info(f"Loaded {len(self.active_positions)} active positions from database")
        except Exception as e:
            logger.error(f"Error loading active positions: {e}")

    def add_position(self, symbol, price, direction, signal_id):
        """Add a new unconfirmed position"""
        try:
            conn = sqlite3.connect(self.positions_db)
            cursor = conn.cursor()

            # Add position to database
            cursor.execute('''
            INSERT INTO positions (
                symbol, position_type, entry_price, entry_time,
                status, signal_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                symbol,
                direction,
                price,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'OPEN',
                signal_id
            ))

            position_id = cursor.lastrowid
            conn.commit()
            conn.close()

            # Add to active positions dictionary
            self.active_positions[symbol] = {
                'id': position_id,
                'direction': direction,
                'entry_price': price,
                'entry_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'signal_id': signal_id,
                'confirmed': False
            }

            logger.info(f"Added new position: {symbol} {direction} at {price}, ID: {position_id}")
            return position_id
        except Exception as e:
            logger.error(f"Error adding position for {symbol}: {e}")
            return None

    def confirm_position(self, position_id):
        """Confirm a position from user response"""
        try:
            # Find position by ID or signal ID
            symbol = None
            for sym, pos in self.active_positions.items():
                if pos['id'] == position_id or pos['signal_id'] == position_id:
                    symbol = sym
                    break

            if not symbol:
                logger.warning(f"Could not find position with ID: {position_id}")
                return False

            # Update database
            conn = sqlite3.connect(self.positions_db)
            cursor = conn.cursor()

            cursor.execute('''
            UPDATE positions
            SET confirmed = 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? OR signal_id = ?
            ''', (position_id, position_id))

            if cursor.rowcount == 0:
                conn.close()
                logger.warning(f"No position found with ID: {position_id}")
                return False

            conn.commit()
            conn.close()

            # Update in memory
            if symbol in self.active_positions:
                self.active_positions[symbol]['confirmed'] = True

            logger.info(f"Confirmed position with ID: {position_id}")
            return True
        except Exception as e:
            logger.error(f"Error confirming position: {e}")
            return False

    def close_position(self, symbol, exit_price, exit_reason="Manual exit"):
        """Close an active position"""
        if symbol not in self.active_positions:
            logger.warning(f"No active position for {symbol}")
            return False

        try:
            position = self.active_positions[symbol]

            # Calculate profit/loss
            if position['direction'] == 'LONG':
                profit_pct = ((exit_price - position['entry_price']) / position['entry_price']) * 100
            else:  # SHORT
                profit_pct = ((position['entry_price'] - exit_price) / position['entry_price']) * 100

            # Update database
            conn = sqlite3.connect(self.positions_db)
            cursor = conn.cursor()

            cursor.execute('''
            UPDATE positions
            SET exit_price = ?, exit_time = ?, status = 'CLOSED',
                profit_loss_percent = ?, exit_reason = ?, 
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''', (
                exit_price,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                profit_pct,
                exit_reason,
                position['id']
            ))

            conn.commit()
            conn.close()

            # Remove from active positions
            del self.active_positions[symbol]

            logger.info(f"Closed position for {symbol} with {profit_pct:.2f}% profit/loss")
            return True
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {e}")
            return False

    def update_signal_limit(self, symbol, limit):
        """Update signal limit for a symbol"""
        try:
            conn = sqlite3.connect(self.positions_db)
            cursor = conn.cursor()

            # Check if symbol exists
            cursor.execute('SELECT symbol FROM signal_limits WHERE symbol = ?', (symbol,))
            exists = cursor.fetchone() is not None

            if exists:
                cursor.execute('''
                UPDATE signal_limits 
                SET limit_value = ?, updated_at = CURRENT_TIMESTAMP
                WHERE symbol = ?
                ''', (limit, symbol))
            else:
                cursor.execute('''
                INSERT INTO signal_limits (symbol, limit_value, count, last_reset)
                VALUES (?, ?, 0, ?)
                ''', (symbol, limit, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            conn.commit()
            conn.close()

            # Update in memory
            if symbol in self.signal_limits:
                self.signal_limits[symbol]['limit'] = limit
            else:
                self.signal_limits[symbol] = {
                    'limit': limit,
                    'count': 0,
                    'last_reset': time.time()
                }

            logger.info(f"Updated signal limit for {symbol} to {limit}")
            return True
        except Exception as e:
            logger.error(f"Error updating signal limit: {e}")
            return False

    def increment_signal_count(self, symbol):
        """Increment signal count for a symbol and check if limit reached"""
        # Reset counts older than 24 hours
        self._reset_old_signal_counts()

        try:
            # Default limit if not set
            if symbol not in self.signal_limits:
                self.signal_limits[symbol] = {
                    'limit': 5,
                    'count': 0,
                    'last_reset': time.time()
                }

            # Increment count
            self.signal_limits[symbol]['count'] += 1
            count = self.signal_limits[symbol]['count']
            limit = self.signal_limits[symbol]['limit']

            # Update database
            conn = sqlite3.connect(self.positions_db)
            cursor = conn.cursor()

            cursor.execute('''
            INSERT INTO signal_limits (symbol, limit_value, count, last_reset)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                count = ?,
                updated_at = CURRENT_TIMESTAMP
            ''', (
                symbol,
                limit,
                count,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                count
            ))

            conn.commit()
            conn.close()

            logger.debug(f"Incremented signal count for {symbol} to {count}/{limit}")

            # Return True if under limit, False if limit reached
            return count <= limit
        except Exception as e:
            logger.error(f"Error incrementing signal count: {e}")
            return False

    def _reset_old_signal_counts(self):
        """Reset signal counts older than 24 hours"""
        current_time = time.time()

        # Check in memory first
        for symbol, data in self.signal_limits.items():
            if current_time - data['last_reset'] >= 86400:  # 24 hours
                data['count'] = 0
                data['last_reset'] = current_time

                # Also update in database
                try:
                    conn = sqlite3.connect(self.positions_db)
                    cursor = conn.cursor()

                    cursor.execute('''
                    UPDATE signal_limits
                    SET count = 0, last_reset = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE symbol = ?
                    ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), symbol))

                    conn.commit()
                    conn.close()

                    logger.debug(f"Reset signal count for {symbol} (24h expired)")
                except Exception as e:
                    logger.error(f"Error resetting signal count: {e}")

    def check_exit_conditions(self, symbol, current_price, indicators=None):
        """Check if position should be closed based on price and indicators"""
        if symbol not in self.active_positions:
            return None

        position = self.active_positions[symbol]

        # Skip unconfirmed positions
        if not position.get('confirmed', False):
            return None

        # Calculate current profit/loss
        if position['direction'] == 'LONG':
            profit_pct = ((current_price - position['entry_price']) / position['entry_price']) * 100
        else:  # SHORT
            profit_pct = ((position['entry_price'] - current_price) / position['entry_price']) * 100

        # Simple exit rules (these can be expanded)
        exit_reason = None

        # Take profit at +10%
        if profit_pct >= 10:
            exit_reason = f"Take profit hit: {profit_pct:.2f}%"

        # Stop loss at -5%
        elif profit_pct <= -5:
            exit_reason = f"Stop loss hit: {profit_pct:.2f}%"

        # Check RSI overbought/oversold if indicators available
        elif indicators and 'rsi' in indicators:
            rsi = indicators['rsi']
            if position['direction'] == 'LONG' and rsi > 75:
                exit_reason = f"RSI overbought: {rsi}"
            elif position['direction'] == 'SHORT' and rsi < 25:
                exit_reason = f"RSI oversold: {rsi}"

        if exit_reason:
            return {
                'position_id': position['id'],
                'symbol': symbol,
                'exit_price': current_price,
                'profit_pct': profit_pct,
                'reason': exit_reason
            }

        return None

    def has_active_position(self, symbol):
        """Check if there's an active position for a symbol"""
        return symbol in self.active_positions

    def get_position_summary(self):
        """Get summary of active positions"""
        return self.active_positions

    def generate_weekly_report(self):
        """Generate a report of position performance for the past week"""
        try:
            conn = sqlite3.connect(self.positions_db)
            cursor = conn.cursor()

            # Get positions closed in the last 7 days
            one_week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute('''
            SELECT 
                symbol, position_type, entry_price, exit_price, 
                profit_loss_percent, exit_reason
            FROM positions
            WHERE status = 'CLOSED' AND exit_time > ?
            ORDER BY exit_time DESC
            ''', (one_week_ago,))

            positions = cursor.fetchall()
            conn.close()

            if not positions:
                return "No closed positions in the last 7 days."

            # Calculate statistics
            total_positions = len(positions)
            profitable = sum(1 for p in positions if p[4] > 0)
            win_rate = (profitable / total_positions) * 100 if total_positions > 0 else 0

            total_profit = sum(p[4] for p in positions if p[4] > 0)
            total_loss = sum(p[4] for p in positions if p[4] <= 0)

            avg_profit = total_profit / profitable if profitable > 0 else 0
            avg_loss = total_loss / (total_positions - profitable) if (total_positions - profitable) > 0 else 0

            # Build report
            report = f"*Weekly Trading Report*\n\n"
            report += f"Period: Last 7 days\n"
            report += f"Total Positions: {total_positions}\n"
            report += f"Win Rate: {win_rate:.2f}%\n"
            report += f"Average Win: {avg_profit:.2f}%\n"
            report += f"Average Loss: {avg_loss:.2f}%\n\n"

            # Show top 5 positions
            report += "*Top Performing Positions*\n"
            top_positions = sorted(positions, key=lambda p: p[4], reverse=True)[:5]

            for i, pos in enumerate(top_positions, 1):
                symbol, position_type, entry, exit, profit, reason = pos
                report += f"{i}. {symbol} {position_type}: {profit:.2f}%\n"

            return report
        except Exception as e:
            logger.error(f"Error generating weekly report: {e}")
            return "Error generating report."