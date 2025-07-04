import os
import sqlite3
import logging
import shutil

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def corrigir_schema_tabela(connection, tabela, colunas):
    cursor = connection.cursor()

    try:
        # Obter informações sobre as colunas existentes
        cursor.execute(f"PRAGMA table_info({tabela})")
        colunas_existentes = [row[1] for row in cursor.fetchall()]

        # Adicionar apenas colunas que não existem
        for coluna, tipo in colunas.items():
            if coluna not in colunas_existentes:
                cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {tipo}")
                logger.info(f"Coluna {coluna} adicionada à tabela {tabela}")
            else:
                logger.debug(f"Coluna {coluna} já existe na tabela {tabela}")

        connection.commit()
        logger.info(f"Schema da tabela {tabela} corrigido com sucesso")
        return True

    except Exception as e:
        connection.rollback()
        logger.error(f"Erro ao corrigir schema para tabela {tabela}: {str(e)}")
        raise

def setup_databases(data_path='data'):
    """Configura todos os bancos de dados necessários com as estruturas corretas"""
    try:
        # Garantir que o diretório de dados existe
        os.makedirs(data_path, exist_ok=True)

        # Configurar kline_data.db
        setup_kline_database(os.path.join(data_path, 'kline_data.db'))

        # Configurar outros bancos de dados necessários
        setup_market_database(os.path.join(data_path, 'market_data.db'))
        setup_position_database(os.path.join(data_path, 'position_data.db'))
        setup_ta_database(os.path.join(data_path, 'technical_analysis.db'))

        logger.info("Todos os bancos de dados configurados com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro ao configurar bancos de dados: {e}")
        return False

def setup_kline_database(db_path):
    """Configura o banco de dados kline_data.db"""
    try:
        # Fazer backup se existir
        if os.path.exists(db_path):
            backup_path = f"{db_path}_backup"
            shutil.copy2(db_path, backup_path)
            logger.info(f"Backup criado: {backup_path}")

        # Conectar ao banco de dados
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Verificar se a tabela klines existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='klines'")
        if cursor.fetchone():
            # Verificar se tem a estrutura correta
            cursor.execute("PRAGMA table_info(klines)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'open' not in columns:
                logger.info("Recriando tabela klines com estrutura correta")

                # Renomear tabela antiga
                cursor.execute("ALTER TABLE klines RENAME TO klines_old")

                # Criar nova tabela
                create_klines_table(cursor)

                # Tentar migrar dados
                try:
                    # Obter colunas da tabela antiga
                    cursor.execute("PRAGMA table_info(klines_old)")
                    old_cols = [col[1] for col in cursor.fetchall()]

                    # Colunas comuns
                    common_cols = []
                    for col in ['symbol', 'timeframe', 'open_time', 'close_time', 'timestamp']:
                        if col in old_cols:
                            common_cols.append(col)

                    if common_cols:
                        cols_str = ', '.join(common_cols)
                        cursor.execute(f"INSERT INTO klines ({cols_str}) SELECT {cols_str} FROM klines_old")
                        logger.info(f"Dados migrados da tabela antiga ({len(common_cols)} colunas)")
                except Exception as e:
                    logger.warning(f"Não foi possível migrar dados: {e}")
            else:
                # Se a tabela existir e tiver a coluna 'open', corrigir o schema se necessário
                # Usar a nova função corrigir_schema_tabela para adicionar colunas que podem estar faltando
                colunas_para_adicionar = {
                    "open_price": "REAL",
                    "high_price": "REAL",
                    "low_price": "REAL",
                    "close_price": "REAL"
                }
                corrigir_schema_tabela(conn, "klines", colunas_para_adicionar)
        else:
            # Criar tabela klines
            create_klines_table(cursor)
            logger.info("Tabela klines criada")

        # Criar índices
        create_klines_indexes(cursor)

        conn.commit()
        conn.close()
        logger.info("Banco de dados kline_data configurado")

    except Exception as e:
        logger.error(f"Erro ao configurar banco de dados kline_data: {e}")
        raise

def create_klines_table(cursor):
    """Cria a tabela klines com estrutura correta"""
    cursor.execute("""
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
    """)

def create_klines_indexes(cursor):
    """Cria índices para a tabela klines"""
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_klines_symbol ON klines (symbol)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_klines_timeframe ON klines (timeframe)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_klines_time ON klines (open_time)")


def setup_market_database(db_path):
    """Configura o banco de dados para dados de mercado"""
    try:
        # Conectar ao banco de dados
        conn = sqlite3.connect(db_path)

        # Corrigir schema para tabela symbols
        colunas_symbols = {
            "open_price": "REAL",
            "price_change": "REAL",
            "price_change_percent": "REAL"
        }
        corrigir_schema_tabela(conn, "symbols", colunas_symbols)

        # Corrigir schema para tabela market_data
        colunas_market = {
            "open_price": "REAL",
            "price_change": "REAL",
            "price_change_percent": "REAL"
        }
        corrigir_schema_tabela(conn, "market_data", colunas_market)

        # Garantir que as tabelas existem
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS symbols (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE,
                price REAL,
                volume REAL,
                timestamp INTEGER,
                open_price REAL,
                price_change REAL,
                price_change_percent REAL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                price REAL,
                volume REAL,
                timestamp INTEGER,
                open_price REAL,
                price_change REAL,
                price_change_percent REAL
            )
        """)

        conn.commit()
        conn.close()
        logger.info("Banco de dados market_data configurado")
        return True

    except Exception as e:
        logger.error(f"Erro ao configurar banco de dados market_data: {e}")
        return False

def setup_position_database(db_path):
    """Configura o banco de dados position_data.db"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            direction TEXT,
            entry_price REAL,
            entry_time INTEGER,
            exit_price REAL,
            exit_time INTEGER,
            size REAL,
            profit_loss REAL,
            status TEXT,
            strategy TEXT
        )
        """)

        conn.commit()
        conn.close()
        logger.info("Banco de dados position_data configurado")

    except Exception as e:
        logger.error(f"Erro ao configurar banco de dados position_data: {e}")
        raise


def setup_ta_database(db_path):
    """Configura o banco de dados technical_analysis.db"""
    try:
        # Verificar se o arquivo existe
        if os.path.exists(db_path):
            backup_path = f"{db_path}_backup"
            shutil.copy2(db_path, backup_path)
            logger.info(f"Backup criado: {backup_path}")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Verificar se a tabela indicators já existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='indicators'")
        if cursor.fetchone():
            # Se existe, verificamos se precisa ser recriada
            try:
                # Tentar consultar a coluna 'indicator' para ver se existe
                cursor.execute("SELECT indicator FROM indicators LIMIT 1")
            except sqlite3.OperationalError:
                logger.info("Recriando tabela indicators com estrutura correta")
                # Renomear tabela antiga
                cursor.execute("ALTER TABLE indicators RENAME TO indicators_old")

                # Criar nova tabela
                cursor.execute("""
                CREATE TABLE indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    timeframe TEXT,
                    timestamp INTEGER,
                    indicator TEXT,
                    value REAL,
                    parameters TEXT
                )
                """)
                conn.commit()

                # Tentar migrar dados se possível
                try:
                    cursor.execute("PRAGMA table_info(indicators_old)")
                    old_columns = [col[1] for col in cursor.fetchall()]
                    common_cols = set(old_columns).intersection(
                        {'symbol', 'timeframe', 'timestamp', 'value', 'parameters'})

                    if common_cols:
                        col_list = ", ".join(common_cols)
                        cursor.execute(f"INSERT INTO indicators ({col_list}) SELECT {col_list} FROM indicators_old")
                        logger.info(f"Dados migrados da tabela antiga (colunas: {col_list})")
                except Exception as e:
                    logger.warning(f"Não foi possível migrar dados: {e}")
        else:
            # Criar tabela indicators
            cursor.execute("""
            CREATE TABLE indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timeframe TEXT,
                timestamp INTEGER,
                indicator TEXT,
                value REAL,
                parameters TEXT
            )
            """)
            conn.commit()

        # Garantir que a tabela foi criada antes de criar o índice
        conn.commit()

        # Criar o índice após garantir que a tabela foi criada
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_indicators_symbol_timeframe
        ON indicators (symbol, timeframe, indicator)
        """)

        conn.commit()
        conn.close()
        logger.info("Banco de dados technical_analysis configurado")

    except Exception as e:
        logger.error(f"Erro ao configurar banco de dados technical_analysis: {e}")
        raise

if __name__ == "__main__":
    setup_databases()