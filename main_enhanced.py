import os
import logging
import time
import threading
import signal
import sys
import queue
from telegram.ext._utils.webhookhandler import TelegramHandler
from database_manager import DatabaseManager
from websocket_client import BinanceWebSocketClient
from indicators import TechnicalIndicators
from ai_model_enhanced import EnhancedAIModel
from signal_generator import SignalGenerator
import db_setup
db_setup.setup_databases('data')
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_bot.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class TradingBot:
    """Main trading bot application with enhanced components"""

    def __init__(self, telegram_token=None, allowed_users=None, db_path='data'):
        self.telegram_token = telegram_token
        self.allowed_users = allowed_users or []
        self.db_path = db_path
        self.components = {}
        self.running = False
        self.component_lock = threading.RLock()  # Adiciona o lock para acesso aos componentes

        # Inicializar todos os componentes
        self._init_components()

        logger.info("Trading bot initialized with enhanced components")

    def _load_historical_data_if_needed(self, symbol, timeframe="1h", min_candles=50):
        try:
            # Verifica se já tem candles suficientes
            with self.component_lock:
                try:
                    # Obter contagem real de candles do banco de dados ou memória
                    if 'websocket' in self.components:
                        klines = self.components['websocket'].get_klines(symbol, timeframe)
                        count = len(klines)
                    else:
                        count = 0

                    logger.info(
                        f"Carregando dados históricos para {symbol} no timeframe {timeframe}. Atual: {count}/{min_candles}")

                    # Se não tem candles suficientes, carrega mais
                    if count < min_candles:
                        # lógica para carregar mais candles
                        pass

                    return True
                except Exception as e:
                    logger.error(f"Erro ao verificar estrutura do banco: {e}")
                    return False
        except Exception as e:
            logger.error(f"Erro ao verificar/carregar dados históricos para {symbol} {timeframe}: {e}")
            return False

    def _init_components(self):
        """Initialize all trading bot components"""
        try:
            # Inicializar o gerenciador de banco de dados
            self.db_manager = DatabaseManager(self.db_path)
            self.components['database'] = self.db_manager

            # Inicializar WebSocket Client no modo público
            self.components['websocket'] = BinanceWebSocketClient()

            # Inicializar indicadores técnicos
            self.components['indicators'] = TechnicalIndicators(self.db_manager)

            # Inicializar modelo de IA
            self.components['ai_model'] = EnhancedAIModel(db_manager=self.db_manager)

            # Inicializar gerador de sinais (remover parâmetro 'components')
            self.components['signals'] = SignalGenerator(self.db_manager)

            # Inicializar position tracker
            self._init_position_tracker()

            # Inicializar Telegram bot se token fornecido
            if self.telegram_token:
                self._init_telegram()

        except Exception as e:
            logger.error(f"Error initializing components: {e}")
            raise

    def _init_position_tracker(self):
        """Initialize position tracker component"""
        try:
            from position_tracker import PositionTracker
            self.components['positions'] = PositionTracker(self.db_path)
            logger.info("Position tracker initialized successfully")
        except Exception as e:
            logger.error("Error initializing position tracker: %s", e)

    def _init_telegram(self):
        """Initialize Telegram component if token is available"""
        if self.telegram_token and len(self.telegram_token) > 20:
            try:
                from telegram_adapter import TelegramAdapter
                self.components['telegram'] = TelegramAdapter(
                    token=self.telegram_token,
                    allowed_users=self.allowed_users,
                    components=self.components
                )
                logger.info(
                    "Telegram bot initialized with token: {}...{}".format(
                        self.telegram_token[:5], self.telegram_token[-5:]))
            except ImportError as e:
                logger.error("Could not import telegram_adapter: %s", e)
                logger.warning("Telegram notifications will be disabled")
            except Exception as e:
                logger.error("Error initializing Telegram component: %s", e)
                logger.warning("Telegram notifications will be disabled")
        else:
            logger.warning("No valid Telegram token provided, Telegram notifications will be disabled")

    def start(self):
        """Inicia o bot de trading e seus componentes"""
        self.running = True
        logger.info("Starting trading bot")

        # Inicializa filas para comunicação entre threads
        self.signal_queue = queue.Queue()

        # Inicia o cliente WebSocket para obter dados
        if 'websocket' in self.components:
            try:
                # Iniciar o WebSocket
                self.components['websocket'].start()

                # Buscar todos os pares USDT disponíveis
                logger.info("Buscando todos os pares USDT disponíveis na Binance...")
                all_symbols = self.components['websocket'].get_all_usdt_pairs()
                logger.info(f"Total de pares USDT encontrados: {len(all_symbols)}")

                # Filtrar apenas os pares com preço acima de 0.50 USD
                filtered_symbols = []
                for symbol in all_symbols:
                    try:
                        price = self.components['websocket'].get_current_price(symbol)
                        if price >= 0.5:
                            filtered_symbols.append(symbol)
                            logger.info(f"Adicionado {symbol} (preço: {price:.4f} USD)")
                        else:
                            logger.debug(f"Ignorado {symbol} (preço: {price:.4f} USD)")
                    except Exception as e:
                        logger.error(f"Erro ao verificar preço para {symbol}: {e}")

                logger.info(f"Total de pares filtrados (preço >= 0.50 USD): {len(filtered_symbols)}")

                # Carrega dados históricos para os símbolos filtrados
                for symbol in filtered_symbols:
                    self._load_historical_data_if_needed(symbol, "1h")
                    time.sleep(0.5)  # Evita sobrecarga na API

                # Iniciar threads de processamento
                logger.info("Iniciando threads de processamento de sinais")
                self.signal_thread = threading.Thread(target=self._process_signals)
                self.signal_thread.daemon = True
                self.signal_thread.start()

                self.telegram_thread = threading.Thread(target=self._process_telegram_queue)
                self.telegram_thread.daemon = True
                self.telegram_thread.start()
                logger.info("Threads de processamento iniciadas com sucesso")

            except Exception as e:
                logger.error(f"Erro ao iniciar cliente WebSocket: {e}")

    def stop(self):
        """Stop the trading bot and all its components"""
        if not self.running:
            return

        self.running = False
        logger.info("Stopping trading bot")

        try:
            with self.component_lock:
                # Stop Telegram bot if available
                if 'telegram' in self.components:
                    try:
                        logger.info("Stopping Telegram bot")
                        self.components['telegram'].stop()
                        logger.info("Telegram bot stopped successfully")
                    except Exception as e:
                        logger.error(f"Error stopping Telegram bot: {e}")

                # Stop WebSocket client
                if 'websocket' in self.components:
                    self.components['websocket'].stop()

            # Wait for threads to finish
            if hasattr(self, 'signal_thread') and self.signal_thread.is_alive():
                self.signal_thread.join(timeout=5)

            if hasattr(self, 'telegram_thread') and self.telegram_thread.is_alive():
                self.telegram_thread.join(timeout=5)

            # Close all database connections
            if 'database' in self.components:
                self.components['database'].close_all()

            logger.info("Trading bot stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping trading bot: {e}", exc_info=True)

    def _process_telegram_queue(self):
        """Process signals in the queue and send to Telegram"""
        while self.running:
            try:
                # Get signals from queue with timeout
                try:
                    signal = self.signal_queue.get(timeout=5)
                    with self.component_lock:
                        if 'telegram' in self.components:
                            try:
                                logger.info(f"Sending signal to Telegram: {signal}")
                                self.components['telegram'].send_signal_notification(signal)
                            except Exception as e:
                                logger.error(f"Error sending signal to Telegram: {e}")
                    self.signal_queue.task_done()
                except queue.Empty:
                    pass  # No signals in queue, continue
            except Exception as e:
                logger.error(f"Error in Telegram queue processing: {e}")
                time.sleep(5)  # Prevent tight loop on error

    def _get_candle_count(self, symbol, timeframe):
        """Verifica a quantidade de velas disponíveis para um símbolo e timeframe"""
        try:
            with self.component_lock:
                db = self.components.get('database')
                if db:
                    # Corrigido para usar o banco de dados 'kline_data' em vez de 'market_data'
                    result = db.execute_query(
                        'kline_data',  # Nome correto do banco de dados
                        'SELECT COUNT(*) FROM klines WHERE symbol = ? AND timeframe = ?',  # Nome provável da tabela
                        params=(symbol, timeframe),
                        fetch='one'
                    )

                    # Adiciona logs para depuração
                    count = result[0] if result else 0
                    logger.debug(f"Contagem de velas para {symbol} {timeframe}: {count}")
                    return count
                return 0
        except Exception as e:
            logger.error(f"Erro ao contar velas para {symbol} {timeframe}: {e}")

            # Verificação da estrutura do banco de dados para depuração
            try:
                with self.component_lock:
                    db = self.components.get('database')
                    if db:
                        # Verifica quais tabelas existem no banco de dados
                        tables = db.execute_query(
                            'kline_data',
                            "SELECT name FROM sqlite_master WHERE type='table';",
                            fetch='all'
                        )
                        logger.info(f"Tabelas no banco kline_data: {tables}")
            except Exception as inner_e:
                logger.error(f"Erro ao verificar estrutura do banco: {inner_e}")

            return 0

    def _process_signals(self):
        """Process market data and generate signals"""
        timeframes = ["1m", "5m", "15m", "1h", "4h"]

        with self.component_lock:
            # Check if required components are available
            if 'indicators' not in self.components or 'signals' not in self.components:
                logger.error("Required components missing, cannot process signals")
                return

        # Initial delay to allow WebSocket connections to establish
        time.sleep(30)

        check_interval = 30  # Check every 30 seconds
        symbols_per_batch = 10  # Process fewer symbols per batch

        while self.running:
            try:
                # Get active symbols from WebSocket client
                with self.component_lock:
                    if 'websocket' not in self.components:
                        logger.error("WebSocket component not available")
                        time.sleep(30)
                        continue
                    symbols = self.components['websocket'].get_active_symbols()

                if not symbols:
                    logger.warning("No active symbols available")
                    time.sleep(30)
                    continue

                # Process each symbol in batches
                for i in range(0, min(20, len(symbols)), symbols_per_batch):
                    batch_symbols = symbols[i:i + symbols_per_batch]
                    logger.info(f"Processing symbols batch: {batch_symbols}")

                    # Process each symbol in the batch
                    for symbol in batch_symbols:
                        # Verificar dados históricos suficientes
                        if not self._load_historical_data_if_needed(symbol, '1h', 50):
                            continue

                        # Get klines data for each timeframe
                        klines_data = {}
                        for timeframe in timeframes:
                            with self.component_lock:
                                # Usar get_klines em vez de get_historical_klines
                                klines = self.components['websocket'].get_klines(symbol, timeframe)
                                if klines and len(klines) >= 20:
                                    klines_data[timeframe] = klines
                                else:
                                    # Tentar carregar dados históricos se não houver suficientes
                                    logger.info(f"Carregando dados históricos para {symbol} {timeframe}")
                                    hist_klines = self.components['websocket'].get_historical_klines(symbol, timeframe)
                                    if hist_klines and len(hist_klines) >= 20:
                                        klines_data[timeframe] = hist_klines

                        # Resto do código para processar sinais...

                    # Delay entre lotes
                    time.sleep(5)

                # Intervalo entre verificações
                time.sleep(check_interval)

            except Exception as e:
                logger.error(f"Error in signal processing: {e}", exc_info=True)
                time.sleep(30)  # Wait and retry

    def run_forever(self):
        """Run the bot until interrupted"""
        self.start()

        # Set up signal handler for graceful shutdown
        def signal_handler(sig, frame):
            logger.info("Shutdown signal received")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("Trading bot running. Press Ctrl+C to stop.")
        logger.info(f"Active components: {list(self.components.keys())}")

        # Periodically check and report status
        status_interval = 300  # Every 5 minutes
        last_status_time = time.time()

        # Keep the main thread alive
        while self.running:
            # Periodically log status
            current_time = time.time()
            if current_time - last_status_time > status_interval:
                websocket_status = "Running" if hasattr(self.components['websocket'], 'running') and self.components[
                    'websocket'].running else "Not running"
                telegram_status = "Running" if 'telegram' in self.components else "Not running"
                logger.info(f"WebSocket status: {websocket_status}")
                logger.info(f"Telegram bot status: {telegram_status}")
                last_status_time = current_time

            time.sleep(1)


if __name__ == "__main__":
    # Set your Telegram bot token and allowed user IDs here
    TELEGRAM_TOKEN = "7633147170:AAGTGmkeVSnCdBnO8d5Pzxx7v7WzNBfhSNI"  # Your valid token

    ALLOWED_USERS = [5829074137]  # Your user ID

    logger.info(f"Allowed Telegram users: {ALLOWED_USERS}")

    bot = TradingBot(telegram_token=TELEGRAM_TOKEN, allowed_users=ALLOWED_USERS)
    bot.run_forever()