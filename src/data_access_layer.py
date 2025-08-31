import sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime, date, timedelta
import logging
from contextlib import contextmanager
import os
import glob

# Configurando um logger básico para melhor feedback
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataAccessLayer:
    """
    Camada de Acesso a Dados (DAL) para o mercado financeiro.
    
    Responsável por:
    1. Coletar dados de 1 minuto e armazená-los em um arquivo SQLite por dia.
    2. Sincronizar o histórico, garantindo que todos os dias até D-1 estejam salvos.
    3. Buscar dados de um período, mesclando os arquivos diários e compondo (resample)
       no timeframe desejado dinamicamente.
    """
    def __init__(self, data_dir='market_data', ticker='BOVA11.SA'):
        self.data_dir = data_dir
        self.ticker = ticker
        self.table_name = 'tf_1m'
        # Cria o diretório de dados se ele não existir
        os.makedirs(self.data_dir, exist_ok=True)
        logging.info(f"Usando o ativo '{self.ticker}' que possui dados de volume.")

    def _get_db_path_for_date(self, target_date):
        """Gera o caminho do arquivo DB para uma data específica."""
        # Converte para objeto date se for datetime
        if isinstance(target_date, datetime):
            target_date = target_date.date()
        return os.path.join(self.data_dir, f"{target_date.strftime('%Y-%m-%d')}.sqlite3")

    @contextmanager
    def _get_connection(self, db_path):
        """Gerenciador de contexto para a conexão com um arquivo DB específico."""
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            yield conn
        finally:
            if conn:
                conn.commit()
                conn.close()

    def _ensure_table_exists(self, db_path):
        """Garante que a tabela de 1 minuto exista em um arquivo DB específico."""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            Datetime TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Volume INTEGER
        );
        """
        with self._get_connection(db_path) as conn:
            conn.cursor().execute(query)

    def _get_ultima_data_db(self):
        """Encontra a data mais recente escaneando os nomes dos arquivos no diretório."""
        # Lista todos os arquivos .sqlite3 no diretório
        db_files = glob.glob(os.path.join(self.data_dir, '*.sqlite3'))
        if not db_files:
            return None
        
        # Extrai as datas dos nomes dos arquivos e encontra a mais recente
        datas = [os.path.basename(f).replace('.sqlite3', '') for f in db_files]
        try:
            ultima_data = max([datetime.strptime(d, '%Y-%m-%d').date() for d in datas])
            # Retorna como datetime para consistência com o resto do código
            return datetime.combine(ultima_data, datetime.min.time())
        except ValueError:
            return None

    def sincronizar_dados_historicos(self):
        """Sincroniza os dados, baixando um dia de cada vez e salvando em seu próprio arquivo."""
        logging.info("--- INICIANDO SINCRONIZAÇÃO HISTÓRICA DE 1 MINUTO (UM DIA POR VEZ) ---")
        
        ultima_data_db = self._get_ultima_data_db()
        
        if ultima_data_db is None:
            # Se não há dados, busca os últimos 6 dias completos
            data_inicial = date.today() - timedelta(days=6)
            logging.info(f"Nenhum dado local. Buscando a partir de {data_inicial}.")
        else:
            # Se há dados, começa a partir do dia seguinte ao último salvo
            data_inicial = ultima_data_db.date() + timedelta(days=1)
        
        data_final = date.today()

        if data_inicial >= data_final:
            logging.info("Dados históricos já estão sincronizados até D-1.")
            return

        # Itera dia a dia para baixar e salvar
        current_date = data_inicial
        while current_date < data_final:
            # CORREÇÃO: Verifica se é um dia da semana (Segunda=0, Domingo=6)
            if current_date.weekday() < 5:
                start_dt = datetime.combine(current_date, datetime.min.time())
                end_dt = start_dt + timedelta(days=1)
                
                logging.info(f"Baixando dados para o dia: {current_date.strftime('%Y-%m-%d')}")
                
                # yfinance lida com erros internamente e os imprime, então não precisamos de try/except aqui.
                novos_dados = yf.download(self.ticker, start=start_dt, end=end_dt, interval='1m', auto_adjust=True)

                if not novos_dados.empty:
                    db_path = self._get_db_path_for_date(current_date)
                    self._ensure_table_exists(db_path)

                    if isinstance(novos_dados.columns, pd.MultiIndex):
                        novos_dados.columns = novos_dados.columns.droplevel(1)

                    novos_dados.index.name = 'Datetime'
                    if novos_dados.index.tz is not None:
                        novos_dados.index = novos_dados.index.tz_localize(None)

                    with self._get_connection(db_path) as conn:
                        novos_dados.to_sql(self.table_name, conn, if_exists='replace', index=True)
                    logging.info(f"{len(novos_dados)} registros salvos em '{db_path}'.")
            else:
                logging.info(f"Pulando data {current_date.strftime('%Y-%m-%d')} (fim de semana).")
            
            current_date += timedelta(days=1)


    def buscar_dados(self, timeframe, data_inicio, data_fim):
        """Busca dados de 1m de múltiplos arquivos diários e os compõe dinamicamente."""
        logging.info(f"Buscando dados de '{data_inicio}' a '{data_fim}' para compor o timeframe de '{timeframe}'...")
        
        start = pd.to_datetime(data_inicio).date()
        end = pd.to_datetime(data_fim).date()
        
        lista_dfs = []
        current_date = start
        while current_date <= end:
            db_path = self._get_db_path_for_date(current_date)
            if os.path.exists(db_path):
                try:
                    with self._get_connection(db_path) as conn:
                        query = f"SELECT * FROM {self.table_name}"
                        df_dia = pd.read_sql_query(query, conn, index_col='Datetime', parse_dates=['Datetime'])
                        lista_dfs.append(df_dia)
                except Exception as e:
                    logging.error(f"Falha ao ler o arquivo {db_path}: {e}")
            current_date += timedelta(days=1)

        if not lista_dfs:
            logging.warning("Nenhum dado de 1 minuto encontrado no período solicitado.")
            return pd.DataFrame()

        dados_1m = pd.concat(lista_dfs)
        dados_1m = dados_1m[~dados_1m.index.duplicated(keep='first')] # Remove duplicatas caso haja sobreposição
        dados_1m.sort_index(inplace=True)
        
        regras_resample = {
            'Open': 'first', 'High': 'max', 'Low': 'min',
            'Close': 'last', 'Volume': 'sum'
        }
        
        dados_compostos = dados_1m.resample(rule=timeframe).agg(regras_resample)
        dados_compostos.dropna(inplace=True)
        
        logging.info(f"Dados de 1m ({len(dados_1m)} regs) compostos em {len(dados_compostos)} candles de {timeframe}.")
        
        return dados_compostos

