from src.data_access_layer import DataAccessLayer
from datetime import date, timedelta


# 1. Instanciar a camada de acesso a dados.
#    O arquivo 'mercado_1m.db' será criado/usado.
dal = DataAccessLayer()

# 2. Sincronizar a base de dados.
#    Na primeira vez, vai baixar os últimos 7 dias de dados de 1 minuto.
#    Nas próximas, apenas o que falta para completar até ontem.
dal.sincronizar_dados_historicos()

# 3. Agora, buscar dados dinamicamente em qualquer timeframe.

# Exemplo A: Buscar dados de 5 minutos dos últimos 3 dias
print("\n--- Exemplo A: Timeframe de 5 minutos ---")
dados_5m = dal.buscar_dados(
    timeframe='5min',
    data_inicio=(date.today() - timedelta(days=3)).strftime('%Y-%m-%d'),
    data_fim=date.today().strftime('%Y-%m-%d')
)
if not dados_5m.empty:
    print(dados_5m.head())
    print(dados_5m.tail())

# Exemplo B: Buscar dados de 30 minutos dos últimos 3 dias
print("\n--- Exemplo B: Timeframe de 30 minutos ---")
dados_30m = dal.buscar_dados(
    timeframe='30min',
    data_inicio=(date.today() - timedelta(days=3)).strftime('%Y-%m-%d'),
    data_fim=date.today().strftime('%Y-%m-%d')
)
if not dados_30m.empty:
    print(dados_30m.head())
    print(dados_30m.tail())

# Exemplo C: Buscar dados diários
print("\n--- Exemplo C: Timeframe Diário ('D') ---")
dados_1d = dal.buscar_dados(
    timeframe='D',
    data_inicio=(date.today() - timedelta(days=3)).strftime('%Y-%m-%d'),
    data_fim=date.today().strftime('%Y-%m-%d')
)
if not dados_1d.empty:
    print(dados_1d)