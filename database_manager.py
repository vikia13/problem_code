import os
import sqlite3
import threading
import logging
import time

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Gerenciador de conexões de banco de dados para prevenir problemas de bloqueio do SQLite.
    Gerencia conexões específicas de thread e implementa lógica de retry.
    """

    def __init__(self, db_path):
        """
        Inicializa o gerenciador de banco de dados.

        Args:
            db_path: Caminho para o diretório onde os arquivos de banco de dados são armazenados
        """
        self.db_path = db_path
        self.connection_pool = {}
        self.lock = threading.Lock()

        # Garantir que o diretório do banco de dados exista
        os.makedirs(db_path, exist_ok=True)

        # Inicializar schemas
        self._fix_kline_data_schema()
        self.fix_all_schemas()

    def _fix_kline_data_schema(self):
        """Corrige especificamente o problema na tabela klines"""
        try:
            db_file = os.path.join(self.db_path, "kline_data.db")
            if os.path.exists(db_file):
                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()

                # Verificar se a tabela klines existe
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='klines'")
                if cursor.fetchone():
                    # Verificar se a coluna 'open' existe
                    cursor.execute("PRAGMA table_info(klines)")
                    columns = [col[1] for col in cursor.fetchall()]

                    if 'open' not in columns:
                        logger.info("Recriando tabela klines com estrutura correta")

                        # Renomear tabela antiga
                        cursor.execute("ALTER TABLE klines RENAME TO klines_old")

                        # Criar nova tabela com estrutura correta
                        cursor.execute("""
                        CREATE TABLE klines (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            symbol TEXT,
                            timeframe TEXT,
                            open_time INTEGER,
                            close_time INTEGER,
                            open REAL,
                            high REAL,
                            low REAL,
                            close REAL,
                            volume REAL,
                            trades INTEGER,
                            timestamp INTEGER,
                            UNIQUE(symbol, timeframe, open_time)
                        )
                        """)

                        # Tentar migrar dados da tabela antiga
                        try:
                            cursor.execute("PRAGMA table_info(klines_old)")
                            old_columns = [col[1] for col in cursor.fetchall()]

                            # Colunas comuns entre as duas tabelas
                            common_columns = set(old_columns).intersection(
                                {'symbol', 'timeframe', 'open_time', 'close_time', 'timestamp'})

                            if common_columns:
                                cols = ', '.join(common_columns)
                                cursor.execute(f"INSERT INTO klines ({cols}) SELECT {cols} FROM klines_old")
                                logger.info(f"Migrados dados existentes ({len(common_columns)} colunas)")
                        except Exception as e:
                            logger.warning(f"Não foi possível migrar dados: {e}")

                        # Criar índices
                        cursor.execute("CREATE INDEX IF NOT EXISTS idx_klines_symbol ON klines (symbol)")
                        cursor.execute("CREATE INDEX IF NOT EXISTS idx_klines_timeframe ON klines (timeframe)")
                        cursor.execute("CREATE INDEX IF NOT EXISTS idx_klines_time ON klines (open_time)")

                        conn.commit()
                        logger.info("Tabela klines recriada com sucesso")

                conn.close()
            else:
                logger.info("Banco de dados kline_data.db não encontrado, será criado quando necessário")

        except Exception as e:
            logger.error(f"Erro ao corrigir schema da tabela klines: {e}")

    def fix_all_schemas(self):
        """Corrige schemas em todas as tabelas de banco de dados"""
        try:
            # Corrigir schema na tabela principal market_data
            self.fix_database_schema()

            # Procurar por todos os bancos de dados SQLite no diretório
            for filename in os.listdir(self.db_path):
                if filename.endswith('.db'):
                    db_name = filename[:-3]  # Remover extensão .db

                    # Corrigir kline_data especificamente
                    if db_name == 'kline_data':
                        self._ensure_kline_table(db_name)

                    # Obter todas as tabelas neste banco de dados
                    tables = self.execute_query(db_name,
                                                "SELECT name FROM sqlite_master WHERE type='table'",
                                                fetch='all')

                    if tables:
                        for table in tables:
                            table_name = table['name'] if isinstance(table, dict) else table[0]

                            # Verificar se esta tabela tem dados de preço (procurar por coluna 'open')
                            columns = self.execute_query(db_name,
                                                         f"PRAGMA table_info({table_name})",
                                                         fetch='all')

                            if columns:
                                column_names = [col['name'] if isinstance(col, dict) else col[1] for col in columns]

                                # Se tiver 'open' mas não 'open_price', adicionar a coluna
                                if ('open' in column_names and 'open_price' not in column_names):
                                    try:
                                        self.execute_query(db_name,
                                                           f"ALTER TABLE {table_name} ADD COLUMN open_price REAL")
                                        self.execute_query(db_name,
                                                           f"UPDATE {table_name} SET open_price = open WHERE open_price IS NULL")
                                        logger.info(f"Adicionada coluna open_price a {db_name}.{table_name}")
                                    except Exception as e:
                                        logger.error(f"Falha ao atualizar schema para {db_name}.{table_name}: {e}")

            logger.info("Schemas de banco de dados corrigidos com sucesso")

        except Exception as e:
            logger.error(f"Erro durante correção de schemas: {e}")

    def _ensure_kline_table(self, db_name):
        """Garante que a tabela klines tenha a estrutura correta"""
        try:
            # Verificar se a tabela existe
            if not self.table_exists(db_name, 'klines'):
                # Criar tabela com estrutura correta
                self.execute_query(db_name, """
                CREATE TABLE klines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    timeframe TEXT,
                    open_time INTEGER,
                    close_time INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    trades INTEGER,
                    timestamp INTEGER,
                    UNIQUE(symbol, timeframe, open_time)
                )
                """)

                # Criar índices
                self.execute_query(db_name,
                                   "CREATE INDEX IF NOT EXISTS idx_klines_symbol ON klines (symbol)")
                self.execute_query(db_name,
                                   "CREATE INDEX IF NOT EXISTS idx_klines_timeframe ON klines (timeframe)")
                self.execute_query(db_name,
                                   "CREATE INDEX IF NOT EXISTS idx_klines_time ON klines (open_time)")

                logger.info(f"Tabela klines criada em {db_name}")
            else:
                # Verificar se tem todas as colunas necessárias
                columns = self.execute_query(db_name, "PRAGMA table_info(klines)", fetch='all')
                column_names = [col['name'] if isinstance(col, dict) else col[1] for col in columns]

                required_columns = ['symbol', 'timeframe', 'open_time', 'close_time',
                                    'open', 'high', 'low', 'close', 'volume', 'trades', 'timestamp']

                for column in required_columns:
                    if column not in column_names:
                        col_type = 'INTEGER' if column in ['open_time', 'close_time', 'trades', 'timestamp'] else 'REAL'
                        if column in ['symbol', 'timeframe']:
                            col_type = 'TEXT'

                        self.execute_query(db_name, f"ALTER TABLE klines ADD COLUMN {column} {col_type}")
                        logger.info(f"Adicionada coluna {column} à tabela klines em {db_name}")

        except Exception as e:
            logger.error(f"Erro ao garantir estrutura da tabela klines: {e}")

    def get_connection(self, db_name):
        thread_id = threading.get_ident()

        with self.lock:
            if thread_id not in self.connection_pool:
                self.connection_pool[thread_id] = {}

            if db_name not in self.connection_pool[thread_id]:
                db_file = os.path.join(self.db_path, f"{db_name}.db")
                conn = sqlite3.connect(db_file, timeout=30)
                # Habilitar foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                # Definir modo de journal para WAL para melhor concorrência
                conn.execute("PRAGMA journal_mode = WAL")
                conn.row_factory = sqlite3.Row
                self.connection_pool[thread_id][db_name] = conn
                logger.debug(f"Criada nova conexão para thread {thread_id}, banco de dados {db_name}")

            return self.connection_pool[thread_id][db_name]

    def execute_query(self, db_name, query, params=None, fetch=None, commit=True, max_retries=5):
        """
        Executa consulta SQL com lógica de retry para bloqueios de banco de dados.

        Args:
            db_name: Nome do banco de dados (sem extensão .db)
            query: String de consulta SQL
            params: Parâmetros para a consulta (opcional)
            fetch: Modo de busca - 'one', 'all', 'lastrowid', ou None
            commit: Se deve fazer commit da transação
            max_retries: Número máximo de tentativas para banco de dados bloqueado

        Returns:
            Resultados da consulta com base no parâmetro fetch
        """
        conn = self.get_connection(db_name)
        cursor = conn.cursor()

        for attempt in range(max_retries):
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                if commit:
                    conn.commit()

                if fetch == 'one':
                    return cursor.fetchone()
                elif fetch == 'all':
                    return cursor.fetchall()
                elif fetch == 'lastrowid':
                    return cursor.lastrowid
                else:
                    return True

            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    # Usar backoff exponencial para retries
                    sleep_time = 0.1 * (2 ** attempt)
                    logger.warning(
                        f"Banco de dados bloqueado, tentando novamente em {sleep_time:.2f}s (tentativa {attempt + 1}/{max_retries})")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Erro de banco de dados após {attempt + 1} tentativas: {e}")
                    raise
            except Exception as e:
                logger.error(f"Erro ao executar consulta: {e}")
                raise

    def _column_exists(self, connection, table, column):
        """Verifica se uma coluna já existe na tabela"""
        cursor = connection.cursor()
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [info[1] for info in cursor.fetchall()]
        return column in columns

    def update_column_if_not_exists(self, db_name, table, column_name, column_type):
        """Adiciona uma coluna apenas se ela ainda não existir na tabela"""
        try:
            with self._get_connection(db_name) as conn:
                # Verifica se a coluna já existe
                if not self._column_exists(conn, table, column_name):
                    # Adiciona a coluna apenas se não existir
                    cursor = conn.cursor()
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}")
                    conn.commit()
                    logger.info(f"Coluna {column_name} adicionada à tabela {table}")
                    return True
                else:
                    logger.debug(f"Coluna {column_name} já existe na tabela {table}")
                    return True
        except Exception as e:
            logger.error(f"Erro ao adicionar coluna {column_name} à tabela {table}: {e}")
            return False

    def execute_script(self, db_name, script, max_retries=5):
        """
        Executa script SQL com lógica de retry para bloqueios de banco de dados.

        Args:
            db_name: Nome do banco de dados (sem extensão .db)
            script: String de script SQL
            max_retries: Número máximo de tentativas para banco de dados bloqueado

        Returns:
            True se bem-sucedido
        """
        conn = self.get_connection(db_name)

        for attempt in range(max_retries):
            try:
                conn.executescript(script)
                conn.commit()
                return True
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    sleep_time = 0.1 * (2 ** attempt)
                    logger.warning(
                        f"Banco de dados bloqueado, tentando novamente em {sleep_time:.2f}s (tentativa {attempt + 1}/{max_retries})")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Erro de banco de dados após {attempt + 1} tentativas: {e}")
                    raise
            except Exception as e:
                logger.error(f"Erro ao executar script: {e}")
                raise

    def create_table(self, db_name, table_name, schema):
        """
        Cria uma tabela se ela não existir.

        Args:
            db_name: Nome do banco de dados (sem extensão .db)
            table_name: Nome da tabela a criar
            schema: Definição de schema (especificações de colunas)

        Returns:
            True se bem-sucedido
        """
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
        return self.execute_query(db_name, query)

    def initialize_schema(self):
        """Cria ou atualiza as tabelas de banco de dados com schema apropriado"""
        try:
            # Inicializar tabela market_data com colunas corretas incluindo open_price
            self.execute_query('market_data', '''
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                timeframe TEXT NOT NULL,
                open REAL NOT NULL,
                open_price REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                UNIQUE(symbol, timestamp, timeframe)
            )
            ''')

            # Inicializar tabela klines
            self.execute_query('kline_data', '''
            CREATE TABLE IF NOT EXISTS klines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timeframe TEXT,
                open_time INTEGER,
                close_time INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                trades INTEGER,
                timestamp INTEGER,
                UNIQUE(symbol, timeframe, open_time)
            )
            ''')

            # Criar índices
            self.execute_query('kline_data',
                               "CREATE INDEX IF NOT EXISTS idx_klines_symbol ON klines (symbol)")
            self.execute_query('kline_data',
                               "CREATE INDEX IF NOT EXISTS idx_klines_timeframe ON klines (timeframe)")
            self.execute_query('kline_data',
                               "CREATE INDEX IF NOT EXISTS idx_klines_time ON klines (open_time)")

            logging.info("Schema de dados de mercado inicializado")
        except Exception as e:
            logging.error(f"Erro ao inicializar schema: {e}")

    def table_exists(self, db_name, table_name):
        """
        Verifica se uma tabela existe no banco de dados.

        Args:
            db_name: Nome do banco de dados (sem extensão .db)
            table_name: Nome da tabela a verificar

        Returns:
            True se a tabela existir
        """
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = self.execute_query(db_name, query, (table_name,), fetch='one')
        return result is not None

    def vacuum_database(self, db_name):
        """
        Executa VACUUM no banco de dados para otimizar armazenamento.

        Args:
            db_name: Nome do banco de dados (sem extensão .db)

        Returns:
            True se bem-sucedido
        """
        return self.execute_query(db_name, "VACUUM")

    def close_connection(self, db_name, thread_id=None):
        """
        Fecha uma conexão específica de banco de dados.

        Args:
            db_name: Nome do banco de dados (sem extensão .db)
            thread_id: ID da Thread (opcional, thread atual se None)
        """
        if thread_id is None:
            thread_id = threading.get_ident()

        with self.lock:
            if thread_id in self.connection_pool and db_name in self.connection_pool[thread_id]:
                try:
                    self.connection_pool[thread_id][db_name].close()
                    del self.connection_pool[thread_id][db_name]
                    logger.debug(f"Fechada conexão para thread {thread_id}, banco de dados {db_name}")
                except Exception as e:
                    logger.error(f"Erro ao fechar conexão de banco de dados: {e}")

    def fix_database_schema(self):
        """Corrige problemas de schema de banco de dados para todas as tabelas de dados de mercado"""
        try:
            # Primeiro corrigir a tabela market_data padrão se ela existir
            if self.table_exists('market_data', 'market_data'):
                self._fix_table_schema('market_data', 'market_data')

            # Garantir que a tabela klines tenha a estrutura correta
            self._ensure_kline_table('kline_data')

            # Encontrar todas as tabelas no banco de dados market_data
            tables = self.execute_query('market_data',
                                        "SELECT name FROM sqlite_master WHERE type='table'",
                                        fetch='all')

            if tables:
                for table_row in tables:
                    table_name = table_row['name'] if isinstance(table_row, dict) else table_row[0]
                    if table_name != 'sqlite_sequence':  # Pular tabelas de sistema do SQLite
                        self._fix_table_schema('market_data', table_name)

                logging.info(f"Schema corrigido para tabelas no banco de dados market_data")

        except Exception as e:
            logging.error(f"Erro ao corrigir schema de banco de dados: {str(e)}")

    def _fix_table_schema(self, db_name, table_name):
        """Corrige schema para uma tabela específica"""
        try:
            # Obter colunas atuais na tabela
            columns_result = self.execute_query(db_name,
                                                f"PRAGMA table_info({table_name})",
                                                fetch='all')

            existing_columns = []
            if columns_result:
                for col in columns_result:
                    if isinstance(col, dict) and 'name' in col:
                        existing_columns.append(col['name'])
                    elif isinstance(col, (list, tuple)) and len(col) > 1:
                        existing_columns.append(col[1])

            # Definir colunas para verificar/adicionar e suas colunas de origem
            column_mapping = {
                # Colunas de preço com colunas de origem
                'open_price': 'open',
                'high_price': 'high',
                'low_price': 'low',
                'close_price': 'close',

                # Colunas de tempo com suas potenciais fontes
                'timestamp': 'time'  # Fonte primária de timestamp
            }

            # Adicionar timestamp de fontes alternativas se fontes primárias não existirem
            timestamp_sources = ['time', 'open_time', 'close_time']
            timestamp_source = None

            # Primeiro verificar se existe alguma fonte de timestamp
            for source in timestamp_sources:
                if source in existing_columns:
                    timestamp_source = source
                    break

            # Adicionar cada coluna faltante
            for new_col, default_source in column_mapping.items():
                if new_col not in existing_columns:
                    # Para timestamp, usar a fonte encontrada se disponível
                    source_col = timestamp_source if new_col == 'timestamp' and timestamp_source else default_source

                    # Verificar se a fonte existe
                    has_source = source_col in existing_columns

                    # Adicionar a coluna (INTEGER para timestamp, REAL para preços)
                    col_type = 'INTEGER' if new_col == 'timestamp' else 'REAL'
                    self.execute_query(db_name, f"ALTER TABLE {table_name} ADD COLUMN {new_col} {col_type}")

                    # Copiar dados se a fonte existir
                    if has_source:
                        self.execute_query(db_name, f"UPDATE {table_name} SET {new_col} = {source_col}")
                        logging.info(
                            f"Adicionada coluna {new_col} a {table_name} e dados sincronizados de {source_col}")
                    else:
                        logging.info(
                            f"Adicionada coluna {new_col} a {table_name} mas nenhuma fonte de dados encontrada")

        except Exception as e:
            logging.error(f"Erro ao corrigir schema para tabela {table_name}: {str(e)}")

    def _safe_add_column(self, connection, table, column, type_def):
        """Adiciona coluna somente se ela não existir na tabela"""
        cursor = connection.cursor()
        try:
            # Verificar se a coluna já existe
            cursor.execute(f"PRAGMA table_info({table})")
            existing_columns = [row[1] for row in cursor.fetchall()]

            if column not in existing_columns:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {type_def}")
                logger.info(f"Coluna {column} adicionada à tabela {table}")
                return True
            else:
                logger.debug(f"Coluna {column} já existe na tabela {table}")
                return False
        except Exception as e:
            logger.error(f"Erro ao adicionar coluna {column} à tabela {table}: {e}")
            return False

    def update_schema(self, db_name, table, columns_dict):
        """
        Atualiza o schema de uma tabela adicionando colunas faltantes

        Args:
            db_name: Nome do banco de dados
            table: Nome da tabela
            columns_dict: Dicionário com nome da coluna e tipo de dado
        """
        try:
            connection = self._get_connection(db_name)
            for column, type_def in columns_dict.items():
                self._safe_add_column(connection, table, column, type_def)
            connection.commit()
            return True
        except Exception as e:
            logger.error(f"Erro ao atualizar schema para tabela {table}: {e}")
            return False

    def close_all(self):
        """Fecha todas as conexões de banco de dados no pool"""
        with self.lock:
            for thread_id, connections in list(self.connection_pool.items()):
                for db_name, conn in list(connections.items()):
                    try:
                        conn.close()
                        logger.debug(f"Fechada conexão para thread {thread_id}, banco de dados {db_name}")
                    except Exception as e:
                        logger.error(f"Erro ao fechar conexão com {db_name}: {e}")

            self.connection_pool = {}
            logger.info("Todas as conexões de banco de dados fechadas")