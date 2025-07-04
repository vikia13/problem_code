import sqlite3
import datetime
import threading
import queue
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path="positions.db"):
        self.db_path = db_path
        self.lock = threading.RLock()  # Keep this for compatibility
        self.queue = queue.Queue()
        self.running = True

        # Start worker thread
        self.worker_thread = threading.Thread(target=self._process_queue)
        self.worker_thread.daemon = True
        self.worker_thread.start()

        # Initialize database structure
        self._direct_init_db()
        logger.info(f"Database initialized with queue system: {db_path}")

    def _direct_init_db(self):
        """Initialize database tables directly (only at startup)"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            cursor = conn.cursor()

            # Create tables if they don't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                entry_price REAL NOT NULL,
                trend TEXT NOT NULL,
                entry_time TEXT NOT NULL,
                exit_price REAL,
                exit_time TEXT,
                signal_id INTEGER,
                status TEXT DEFAULT 'OPEN'
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS signal_counts (
                symbol TEXT PRIMARY KEY,
                count INTEGER DEFAULT 0,
                date TEXT NOT NULL,
                max_count INTEGER DEFAULT 3
            )
            ''')

            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise

    def _process_queue(self):
        """Process database operations from the queue"""
        # Use a dedicated connection for the queue processor
        conn = None
        batch = []
        batch_size = 50
        batch_timeout = 0.2  # seconds
        last_process_time = time.time()

        while self.running:
            try:
                if conn is None:
                    conn = sqlite3.connect(self.db_path, timeout=30.0)

                # Get item from queue with timeout
                try:
                    item = self.queue.get(timeout=batch_timeout)
                    batch.append(item)
                    self.queue.task_done()
                except queue.Empty:
                    pass  # No new items, might still process existing batch

                current_time = time.time()
                # Process batch if it's full or timeout occurred
                if len(batch) >= batch_size or (batch and current_time - last_process_time > batch_timeout):
                    with conn:  # Ensures transaction commit/rollback
                        for operation, args, kwargs, result_queue in batch:
                            try:
                                result = operation(conn, *args, **kwargs)
                                if result_queue:
                                    result_queue.put((True, result))
                            except Exception as e:
                                logger.error(f"Database operation error: {e}")
                                if result_queue:
                                    result_queue.put((False, str(e)))

                    batch = []
                    last_process_time = current_time

            except Exception as e:
                logger.error(f"Error in database queue processor: {e}")
                # Close and reopen connection on error
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                    conn = None
                time.sleep(1)  # Delay before retry

        # Cleanup when thread is stopping
        if conn:
            conn.close()

    def _execute_write(self, conn, query, params=None):
        """Execute write operation on the provided connection"""
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        conn.commit()
        return cursor.lastrowid

    def _execute_read(self, conn, query, params=None):
        """Execute read operation on the provided connection"""
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        return cursor.fetchall()

    def _queue_operation(self, operation, args=None, kwargs=None, need_result=False):
        """Queue an operation and wait for result if needed"""
        result_queue = queue.Queue() if need_result else None
        self.queue.put((operation, args or (), kwargs or {}, result_queue))

        if need_result:
            success, result = result_queue.get()
            result_queue.task_done()
            if not success:
                raise Exception(f"Database operation failed: {result}")
            return result
        return None

    # Public API - maintains compatibility with existing code
    def init_db(self):
        """Initialize database (no-op as it's done in __init__)"""
        pass  # Already initialized in __init__

    def add_position(self, symbol, entry_price, trend, signal_id=None):
        """Add a new position"""
        try:
            entry_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            query = "INSERT INTO positions (symbol, entry_price, trend, entry_time, signal_id, status) VALUES (?, ?, ?, ?, ?, ?)"
            params = (symbol, entry_price, trend, entry_time, signal_id, 'OPEN')

            return self._queue_operation(
                self._execute_write,
                args=(query, params),
                need_result=True
            )
        except Exception as e:
            logger.error(f"Error adding position: {e}")
            return None

    def close_position(self, position_id, exit_price):
        """Close an existing position"""
        try:
            exit_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            query = "UPDATE positions SET exit_price = ?, exit_time = ?, status = 'CLOSED' WHERE id = ?"
            params = (exit_price, exit_time, position_id)

            self._queue_operation(
                self._execute_write,
                args=(query, params)
            )
            return True
        except Exception as e:
            logger.error(f"Error closing position: {e}")
            return False

    def get_open_positions(self):
        """Get all open positions"""
        try:
            query = "SELECT * FROM positions WHERE status = 'OPEN'"
            return self._queue_operation(
                self._execute_read,
                args=(query,),
                need_result=True
            )
        except Exception as e:
            logger.error(f"Error getting open positions: {e}")
            return []

    def get_position_by_signal_id(self, signal_id):
        """Get position by signal ID"""
        try:
            query = "SELECT * FROM positions WHERE signal_id = ? AND status = 'OPEN'"
            result = self._queue_operation(
                self._execute_read,
                args=(query, (signal_id,)),
                need_result=True
            )
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting position by signal ID: {e}")
            return None

    def increment_signal_count(self, symbol):
        """Increment the signal count for a symbol"""
        try:
            today = datetime.datetime.now().strftime("%Y-%m-%d")

            def _increment_count(conn):
                cursor = conn.cursor()
                # Check if entry exists
                cursor.execute("SELECT count, max_count FROM signal_counts WHERE symbol = ? AND date = ?",
                               (symbol, today))
                result = cursor.fetchone()

                if result:
                    # Update existing entry
                    current_count, max_count = result
                    max_count = max_count or 3

                    if current_count < max_count:
                        cursor.execute(
                            "UPDATE signal_counts SET count = ? WHERE symbol = ? AND date = ?",
                            (current_count + 1, symbol, today)
                        )
                        return True
                    else:
                        return False
                else:
                    # Create new entry
                    cursor.execute(
                        "INSERT INTO signal_counts (symbol, count, date, max_count) VALUES (?, ?, ?, ?)",
                        (symbol, 1, today, 3)
                    )
                    return True

            return self._queue_operation(_increment_count, need_result=True)
        except Exception as e:
            logger.error(f"Error incrementing signal count: {e}")
            return False

    def get_signal_count(self, symbol):
        """Get the current signal count for a symbol"""
        try:
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            query = "SELECT count FROM signal_counts WHERE symbol = ? AND date = ?"
            result = self._queue_operation(
                self._execute_read,
                args=(query, (symbol, today)),
                need_result=True
            )
            return result[0][0] if result else 0
        except Exception as e:
            logger.error(f"Error getting signal count: {e}")
            return 0

    def set_max_signals(self, symbol, max_count):
        """Set the maximum signals for a symbol"""
        try:
            today = datetime.datetime.now().strftime("%Y-%m-%d")

            def _set_max_signals(conn):
                cursor = conn.cursor()
                # Check if entry exists
                cursor.execute("SELECT count FROM signal_counts WHERE symbol = ? AND date = ?",
                               (symbol, today))
                result = cursor.fetchone()

                if result:
                    cursor.execute(
                        "UPDATE signal_counts SET max_count = ? WHERE symbol = ? AND date = ?",
                        (max_count, symbol, today)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO signal_counts (symbol, count, date, max_count) VALUES (?, ?, ?, ?)",
                        (symbol, 0, today, max_count)
                    )
                return True

            return self._queue_operation(_set_max_signals, need_result=True)
        except Exception as e:
            logger.error(f"Error setting max signals: {e}")
            return False

    def close(self):
        """Close the database connection"""
        try:
            self.running = False
            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=5)
        except Exception as e:
            logger.error(f"Error closing database: {e}")