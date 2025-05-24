#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MT5 Order Executor - Módulo de execução de ordens para MetaTrader 5
com gerenciamento de risco e integração com banco de dados

Este módulo conecta-se ao MT5, gerencia ordens e posições,
calcula tamanho de lote com base no risco e registra operações.
Versão 3.0: Implementa leitura e processamento de sinais do arquivo JSON.

Autor: Manus
Data: 23/05/2025
Versão: 3.0 - Leitura de sinais do arquivo JSON
"""

import MetaTrader5 as mt5
import time
import threading
import queue
from datetime import datetime, timedelta
import os
import logging
import json
import sqlite3
import sys
import traceback

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("mt5_order_executor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("MT5OrderExecutor")

class MT5OrderExecutor:
    """
    Classe principal para execução de ordens no MetaTrader 5
    
    Arquitetura:
    - Thread principal: Gerencia a conexão e status
    - Thread de processamento de ordens: Executa ordens da fila
    - Thread de processamento de sinais: Lê sinais do arquivo JSON e os coloca na fila de ordens
    - Thread de monitoramento: Acompanha posições abertas
    """
    
    def __init__(self, config_file="mt5_config.json"):
        """
        Inicializa o executor de ordens com configurações do arquivo JSON
        
        Args:
            config_file (str): Caminho para o arquivo de configuração JSON
        """
        logger.info(f"Inicializando MT5 Order Executor v3.0 com arquivo de configuração: {config_file}")
        self.config_file = config_file
        self.config = self._load_config(config_file)
        self.mt5_initialized = False
        self.account_info = None
        self.stop_event = threading.Event()
        self.order_queue = queue.Queue()
        self.signal_queue = queue.Queue() # Mantido para compatibilidade, mas não usado diretamente
        self.threads = []
        self.db_conn = None
        self.daily_stats = {"trades": 0, "pnl": 0, "wins": 0, "losses": 0, "last_reset": datetime.now().strftime("%Y-%m-%d")}
        self.processed_signal_timestamps = set() # Para evitar processar o mesmo sinal múltiplas vezes
        self.last_signal_file_mtime = 0 # Timestamp da última modificação do arquivo de sinais
    
    def _load_config(self, config_file):
        """
        Carrega configurações do arquivo JSON
        
        Args:
            config_file (str): Caminho para o arquivo de configuração
            
        Returns:
            dict: Configurações carregadas ou configurações padrão em caso de erro
        """
        default_config = {
            "mt5": {
                "login": 12345678,  # Substituir pelo seu login
                "password": "your_password",  # Substituir pela sua senha
                "server": "YourBroker-Server",  # Substituir pelo servidor da sua corretora
                "path": "C:\\Program Files\\MetaTrader 5\\terminal64.exe" # Caminho para o terminal MT5
            },
            "trading": {
                "allowed_symbols": ["WINFUT", "WDOFUT"], # Ativos permitidos
                "risk_per_trade_pct": 1.0,  # Risco máximo por operação em % do saldo
                "max_daily_loss_pct": 3.0,  # Perda máxima diária em % do saldo inicial
                "max_open_trades": 5,  # Número máximo de operações abertas simultaneamente
                "slippage": 3,  # Slippage permitido em pontos
                "magic_number": 234567, # Número mágico para identificar ordens
                "trading_hours": {
                    "start": "09:00",
                    "end": "17:55"
                }
            },
            "database": {
                "db_path": "market_data.db",
                "orders_table": "executed_orders",
                "stats_table": "daily_stats"
            },
            "signals": {
                "signal_file": "trading_signals.json",
                "polling_interval": 5.0 # Intervalo para verificar novos sinais (segundos)
            }
        }
        
        try:
            if os.path.exists(config_file):
                logger.info(f"Carregando configurações do arquivo: {config_file}")
                with open(config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                logger.info(f"Arquivo de configuração não encontrado. Criando com valores padrão: {config_file}")
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, indent=4)
                logger.info(f"Arquivo de configuração {config_file} criado com valores padrão. Por favor, edite-o com suas informações.")
                return default_config
        except Exception as e:
            logger.error(f"Erro ao carregar configurações: {e}")
            logger.error(traceback.format_exc())
            logger.info("Usando configurações padrão devido ao erro")
            return default_config
    
    def setup_database_connection(self):
        """
        Configura a conexão com o banco de dados SQLite e cria tabelas se necessário
        
        Returns:
            bool: True se a conexão foi estabelecida com sucesso, False caso contrário
        """
        try:
            db_config = self.config["database"]
            db_path = db_config["db_path"]
            self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
            self.db_conn.row_factory = sqlite3.Row
            cursor = self.db_conn.cursor()
            
            # Cria tabela de ordens executadas se não existir
            orders_table = db_config["orders_table"]
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {orders_table} (
                order_id INTEGER PRIMARY KEY,
                mt5_ticket INTEGER UNIQUE,
                asset TEXT NOT NULL,
                type TEXT NOT NULL, -- BUY, SELL
                volume REAL NOT NULL,
                price REAL NOT NULL,
                sl REAL,
                tp REAL,
                status TEXT NOT NULL, -- PENDING, OPEN, CLOSED, FAILED
                open_time TEXT,
                close_time TEXT,
                profit REAL,
                reason TEXT, -- Motivo da entrada/saída
                signal_timestamp TEXT, -- Timestamp do sinal original
                strategy_profile TEXT -- Perfil de estratégia que gerou o sinal
            )
            """)
            
            # Cria tabela de estatísticas diárias se não existir
            stats_table = db_config["stats_table"]
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {stats_table} (
                date TEXT PRIMARY KEY,
                trades INTEGER DEFAULT 0,
                pnl REAL DEFAULT 0.0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0
            )
            """)
            
            self.db_conn.commit()
            logger.info(f"Banco de dados configurado: {db_path}")
            
            # Carrega estatísticas do dia atual
            self.load_daily_stats()
            
            return True
        except Exception as e:
            logger.error(f"Erro ao configurar banco de dados: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def load_daily_stats(self):
        """
        Carrega as estatísticas de trading do dia atual do banco de dados
        """
        try:
            today_str = datetime.now().strftime("%Y-%m-%d")
            stats_table = self.config["database"]["stats_table"]
            cursor = self.db_conn.cursor()
            
            cursor.execute(f"SELECT * FROM {stats_table} WHERE date = ?", (today_str,))
            stats = cursor.fetchone()
            
            if stats:
                self.daily_stats = dict(stats)
                logger.info(f"Estatísticas diárias carregadas: {self.daily_stats}")
            else:
                # Se não houver estatísticas para hoje, reseta
                self.daily_stats = {"date": today_str, "trades": 0, "pnl": 0, "wins": 0, "losses": 0}
                cursor.execute(f"INSERT OR REPLACE INTO {stats_table} (date, trades, pnl, wins, losses) VALUES (?, ?, ?, ?, ?)",
                               (today_str, 0, 0.0, 0, 0))
                self.db_conn.commit()
                logger.info("Estatísticas diárias resetadas para hoje")
            
            # Garante que last_reset está presente
            self.daily_stats["last_reset"] = self.daily_stats.get("date", today_str)
            
        except Exception as e:
            logger.error(f"Erro ao carregar estatísticas diárias: {e}")
            logger.error(traceback.format_exc())
            # Usa valores padrão em caso de erro
            today_str = datetime.now().strftime("%Y-%m-%d")
            self.daily_stats = {"date": today_str, "trades": 0, "pnl": 0, "wins": 0, "losses": 0, "last_reset": today_str}
    
    def save_daily_stats(self):
        """
        Salva as estatísticas de trading do dia atual no banco de dados
        """
        try:
            stats_table = self.config["database"]["stats_table"]
            cursor = self.db_conn.cursor()
            
            cursor.execute(f"""
            INSERT OR REPLACE INTO {stats_table} (date, trades, pnl, wins, losses)
            VALUES (?, ?, ?, ?, ?)
            """, (self.daily_stats["date"], self.daily_stats["trades"], self.daily_stats["pnl"],
                  self.daily_stats["wins"], self.daily_stats["losses"]))
            
            self.db_conn.commit()
            logger.debug(f"Estatísticas diárias salvas: {self.daily_stats}")
        except Exception as e:
            logger.error(f"Erro ao salvar estatísticas diárias: {e}")
            logger.error(traceback.format_exc())
    
    def update_daily_stats(self, profit):
        """
        Atualiza as estatísticas diárias após fechar uma operação
        
        Args:
            profit (float): Lucro ou prejuízo da operação fechada
        """
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        # Reseta estatísticas se for um novo dia
        if self.daily_stats["date"] != today_str:
            self.load_daily_stats()
        
        self.daily_stats["trades"] += 1
        self.daily_stats["pnl"] += profit
        if profit > 0:
            self.daily_stats["wins"] += 1
        else:
            self.daily_stats["losses"] += 1
        
        self.save_daily_stats()
    
    def connect_mt5(self):
        """
        Conecta ao terminal MetaTrader 5
        
        Returns:
            bool: True se conectado com sucesso, False caso contrário
        """
        try:
            mt5_config = self.config["mt5"]
            if not mt5.initialize(login=mt5_config["login"],
                                  password=mt5_config["password"],
                                  server=mt5_config["server"],
                                  path=mt5_config["path"]):
                logger.error(f"Falha ao inicializar MT5: {mt5.last_error()}")
                self.mt5_initialized = False
                return False
            
            self.account_info = mt5.account_info()
            if self.account_info is None:
                logger.error(f"Falha ao obter informações da conta: {mt5.last_error()}")
                mt5.shutdown()
                self.mt5_initialized = False
                return False
            
            logger.info(f"Conectado ao MetaTrader 5. Conta: {self.account_info.login}, Servidor: {self.account_info.server}")
            logger.info(f"Saldo: {self.account_info.balance}, Margem Livre: {self.account_info.margin_free}")
            self.mt5_initialized = True
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar ao MT5: {e}")
            logger.error(traceback.format_exc())
            self.mt5_initialized = False
            return False
    
    def disconnect_mt5(self):
        """
        Desconecta do terminal MetaTrader 5
        """
        if self.mt5_initialized:
            mt5.shutdown()
            self.mt5_initialized = False
            logger.info("Desconectado do MetaTrader 5")
    
    def check_trading_allowed(self):
        """
        Verifica se o trading é permitido com base no horário e perda diária
        
        Returns:
            bool: True se o trading é permitido, False caso contrário
        """
        try:
            # Verifica horário de trading
            trading_hours = self.config["trading"]["trading_hours"]
            now = datetime.now().time()
            start_time = datetime.strptime(trading_hours["start"], "%H:%M").time()
            end_time = datetime.strptime(trading_hours["end"], "%H:%M").time()
            
            if not (start_time <= now <= end_time):
                logger.debug(f"Fora do horário de trading: {now} (permitido: {start_time}-{end_time})")
                return False
            
            # Verifica perda máxima diária
            max_daily_loss_pct = self.config["trading"]["max_daily_loss_pct"]
            initial_balance = self.account_info.balance - self.daily_stats["pnl"] # Aproximação do saldo inicial do dia
            max_loss_value = (max_daily_loss_pct / 100.0) * initial_balance
            
            if self.daily_stats["pnl"] < -max_loss_value:
                logger.warning(f"Perda máxima diária atingida: PNL={self.daily_stats['pnl']:.2f}, Limite={-max_loss_value:.2f}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Erro ao verificar permissão de trading: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def calculate_lot_size(self, asset, stop_loss_price):
        """
        Calcula o tamanho do lote com base no risco por operação
        
        Args:
            asset (str): Símbolo do ativo
            stop_loss_price (float): Preço do stop loss
            
        Returns:
            float: Tamanho do lote calculado ou 0.0 em caso de erro
        """
        try:
            if not self.mt5_initialized:
                logger.error("MT5 não inicializado para calcular lote")
                return 0.0
            
            # Obtém informações do símbolo
            symbol_info = mt5.symbol_info(asset)
            if symbol_info is None:
                logger.error(f"Falha ao obter informações do símbolo {asset}: {mt5.last_error()}")
                return 0.0
            
            # Obtém preço atual
            tick = mt5.symbol_info_tick(asset)
            if tick is None:
                logger.error(f"Falha ao obter tick do símbolo {asset}: {mt5.last_error()}")
                return 0.0
            current_price = tick.ask if stop_loss_price < tick.bid else tick.bid # Usa ask para compra, bid para venda
            
            # Calcula risco em pontos
            risk_points = abs(current_price - stop_loss_price)
            if risk_points == 0:
                logger.warning(f"Risco em pontos é zero para {asset}, não é possível calcular lote")
                return 0.0
            
            # Calcula valor do ponto na moeda da conta
            point_value = symbol_info.trade_tick_value / symbol_info.trade_tick_size * symbol_info.point
            if point_value == 0:
                # Tenta obter do config se não disponível no MT5
                point_value = self.config["assets"].get(asset, {}).get("point_value", 0)
                if point_value == 0:
                    logger.error(f"Valor do ponto não encontrado para {asset}")
                    return 0.0
            
            # Calcula risco por contrato
            risk_per_contract = risk_points / symbol_info.point * point_value
            if risk_per_contract <= 0:
                logger.error(f"Risco por contrato inválido para {asset}: {risk_per_contract}")
                return 0.0
            
            # Calcula valor do risco em dinheiro
            risk_per_trade_pct = self.config["trading"]["risk_per_trade_pct"]
            account_balance = self.account_info.balance
            risk_amount = (risk_per_trade_pct / 100.0) * account_balance
            
            # Calcula tamanho do lote
            lot_size = risk_amount / risk_per_contract
            
            # Ajusta para o volume mínimo e máximo permitido
            lot_size = max(symbol_info.volume_min, lot_size)
            lot_size = min(symbol_info.volume_max, lot_size)
            
            # Ajusta para o passo de volume
            if symbol_info.volume_step != 0:
                lot_size = round(lot_size / symbol_info.volume_step) * symbol_info.volume_step
            
            # Arredonda para precisão de volume
            lot_size = round(lot_size, symbol_info.volume_digits if hasattr(symbol_info, 'volume_digits') else 2)
            
            logger.info(f"Cálculo de lote para {asset}: Risco={risk_amount:.2f}, Risco/Contrato={risk_per_contract:.2f}, Lote={lot_size}")
            
            return lot_size
        except Exception as e:
            logger.error(f"Erro ao calcular tamanho do lote para {asset}: {e}")
            logger.error(traceback.format_exc())
            return 0.0
    
    def send_market_order(self, asset, order_type, volume, stop_loss, take_profit, signal_details):
        """
        Envia uma ordem a mercado para o MT5
        
        Args:
            asset (str): Símbolo do ativo
            order_type (str): "BUY" ou "SELL"
            volume (float): Tamanho do lote
            stop_loss (float): Preço do stop loss
            take_profit (float): Preço do take profit
            signal_details (dict): Detalhes do sinal original
            
        Returns:
            int: Ticket da ordem MT5 ou None em caso de falha
        """
        try:
            if not self.mt5_initialized:
                logger.error("MT5 não inicializado para enviar ordem")
                return None
            
            # Verifica se o símbolo está disponível
            symbol_info = mt5.symbol_info(asset)
            if symbol_info is None:
                logger.error(f"Símbolo {asset} não encontrado no MT5")
                return None
            if not symbol_info.visible:
                logger.warning(f"Símbolo {asset} não está visível, tentando selecioná-lo...")
                if not mt5.symbol_select(asset, True):
                    logger.error(f"Falha ao selecionar símbolo {asset}: {mt5.last_error()}")
                    return None
                time.sleep(0.1) # Pequena pausa após selecionar
                symbol_info = mt5.symbol_info(asset)
            
            # Define o tipo de ordem MT5
            mt5_order_type = mt5.ORDER_TYPE_BUY if order_type == "BUY" else mt5.ORDER_TYPE_SELL
            
            # Obtém o preço atual para preencher a ordem
            price = mt5.symbol_info_tick(asset).ask if order_type == "BUY" else mt5.symbol_info_tick(asset).bid
            
            # Prepara a requisição da ordem
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": asset,
                "volume": volume,
                "type": mt5_order_type,
                "price": price,
                "sl": stop_loss,
                "tp": take_profit,
                "deviation": self.config["trading"]["slippage"],
                "magic": self.config["trading"]["magic_number"],
                "comment": f"Signal {signal_details.get('strategy_profile', 'N/A')}",
                "type_time": mt5.ORDER_TIME_GTC, # Good till cancelled
                "type_filling": mt5.ORDER_FILLING_IOC, # Immediate or Cancel
            }
            
            # Envia a ordem
            logger.info(f"Enviando ordem {order_type} para {asset} - Vol: {volume}, SL: {stop_loss}, TP: {take_profit}")
            result = mt5.order_send(request)
            
            # Verifica o resultado
            if result is None:
                logger.error(f"Falha ao enviar ordem para {asset}: {mt5.last_error()}")
                return None
            
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                logger.error(f"Ordem para {asset} não executada: {result.retcode} - {result.comment}")
                # Loga detalhes da requisição e resultado para debug
                logger.debug(f"Detalhes da requisição: {request}")
                logger.debug(f"Detalhes do resultado: {result}")
                return None
            
            logger.info(f"Ordem {order_type} para {asset} executada com sucesso. Ticket: {result.order}")
            return result.order
        except Exception as e:
            logger.error(f"Erro ao enviar ordem a mercado para {asset}: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def close_position(self, position_ticket, reason="Fechamento manual"):
        """
        Fecha uma posição aberta específica
        
        Args:
            position_ticket (int): Ticket da posição a ser fechada
            reason (str): Motivo do fechamento
            
        Returns:
            bool: True se a posição foi fechada com sucesso, False caso contrário
        """
        try:
            if not self.mt5_initialized:
                logger.error("MT5 não inicializado para fechar posição")
                return False
            
            # Obtém informações da posição
            position = mt5.positions_get(ticket=position_ticket)
            if not position or len(position) == 0:
                logger.warning(f"Posição com ticket {position_ticket} não encontrada")
                return False
            
            position = position[0] # Pega a primeira (e única) posição com o ticket
            asset = position.symbol
            volume = position.volume
            order_type = position.type # 0 = BUY, 1 = SELL
            
            # Define o tipo de ordem oposta para fechar
            close_order_type = mt5.ORDER_TYPE_SELL if order_type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
            
            # Obtém o preço atual para fechar
            price = mt5.symbol_info_tick(asset).bid if order_type == mt5.ORDER_TYPE_BUY else mt5.symbol_info_tick(asset).ask
            
            # Prepara a requisição de fechamento
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": asset,
                "volume": volume,
                "type": close_order_type,
                "position": position_ticket,
                "price": price,
                "deviation": self.config["trading"]["slippage"],
                "magic": self.config["trading"]["magic_number"],
                "comment": reason,
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            # Envia a ordem de fechamento
            logger.info(f"Enviando ordem de fechamento para posição {position_ticket} ({asset})")
            result = mt5.order_send(request)
            
            # Verifica o resultado
            if result is None:
                logger.error(f"Falha ao enviar ordem de fechamento para {position_ticket}: {mt5.last_error()}")
                return False
            
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                logger.error(f"Ordem de fechamento para {position_ticket} não executada: {result.retcode} - {result.comment}")
                logger.debug(f"Detalhes da requisição de fechamento: {request}")
                logger.debug(f"Detalhes do resultado do fechamento: {result}")
                return False
            
            logger.info(f"Posição {position_ticket} ({asset}) fechada com sucesso. Ticket da ordem de fechamento: {result.order}")
            
            # Atualiza estatísticas diárias (requer obter o lucro da operação fechada)
            # O lucro pode ser obtido do histórico de ordens/deals
            # Esta parte pode ser complexa e requer consulta ao histórico
            # self.update_daily_stats(profit) # Implementar obtenção do profit
            
            return True
        except Exception as e:
            logger.error(f"Erro ao fechar posição {position_ticket}: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def close_all_positions(self, reason="Fechamento de todas as posições"):
        """
        Fecha todas as posições abertas gerenciadas por este executor
        """
        try:
            if not self.mt5_initialized:
                logger.error("MT5 não inicializado para fechar todas as posições")
                return
            
            positions = mt5.positions_get(magic=self.config["trading"]["magic_number"])
            if positions is None:
                logger.error(f"Erro ao obter posições: {mt5.last_error()}")
                return
            
            if len(positions) == 0:
                logger.info("Nenhuma posição aberta para fechar")
                return
            
            logger.info(f"Fechando {len(positions)} posições abertas...")
            closed_count = 0
            for position in positions:
                if self.close_position(position.ticket, reason):
                    closed_count += 1
            logger.info(f"{closed_count} posições fechadas com sucesso")
        except Exception as e:
            logger.error(f"Erro ao fechar todas as posições: {e}")
            logger.error(traceback.format_exc())
    
    def get_open_positions(self):
        """
        Obtém informações sobre as posições abertas gerenciadas por este executor
        
        Returns:
            list: Lista de dicionários com informações das posições abertas
        """
        try:
            if not self.mt5_initialized:
                return []
            
            positions = mt5.positions_get(magic=self.config["trading"]["magic_number"])
            if positions is None:
                logger.error(f"Erro ao obter posições abertas: {mt5.last_error()}")
                return []
            
            open_positions = []
            for pos in positions:
                open_positions.append({
                    "ticket": pos.ticket,
                    "symbol": pos.symbol,
                    "type": "BUY" if pos.type == mt5.ORDER_TYPE_BUY else "SELL",
                    "volume": pos.volume,
                    "open_price": pos.price_open,
                    "current_price": pos.price_current,
                    "sl": pos.sl,
                    "tp": pos.tp,
                    "profit": pos.profit,
                    "open_time": datetime.fromtimestamp(pos.time).isoformat()
                })
            return open_positions
        except Exception as e:
            logger.error(f"Erro ao obter posições abertas: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def add_order_to_queue(self, order_details):
        """
        Adiciona uma ordem à fila de processamento
        
        Args:
            order_details (dict): Dicionário com detalhes da ordem
                                  (asset, type, stop_loss, take_profit, signal_details)
        """
        try:
            # Validação básica da ordem
            required_keys = ["asset", "type", "stop_loss", "take_profit", "signal_details"]
            if not all(key in order_details for key in required_keys):
                logger.error(f"Detalhes da ordem inválidos: {order_details}")
                return
            
            # Adiciona à fila
            self.order_queue.put(order_details)
            logger.info(f"Ordem adicionada à fila: {order_details['type']} {order_details['asset']}")
        except Exception as e:
            logger.error(f"Erro ao adicionar ordem à fila: {e}")
            logger.error(traceback.format_exc())
    
    def order_processor_thread(self):
        """
        Thread dedicada para processar ordens da fila
        """
        logger.info("Thread de processamento de ordens iniciada")
        while not self.stop_event.is_set():
            try:
                # Obtém ordem da fila (bloqueia por 1 segundo)
                order = self.order_queue.get(timeout=1.0)
                
                logger.info(f"Processando ordem da fila: {order['type']} {order['asset']}")
                
                # Verifica se o trading é permitido
                if not self.check_trading_allowed():
                    logger.warning(f"Trading não permitido, ordem ignorada: {order}")
                    self.order_queue.task_done()
                    continue
                
                # Verifica se o número máximo de trades abertos foi atingido
                open_positions = self.get_open_positions()
                if len(open_positions) >= self.config["trading"]["max_open_trades"]:
                    logger.warning(f"Número máximo de trades abertos atingido ({len(open_positions)}), ordem ignorada: {order}")
                    self.order_queue.task_done()
                    continue
                
                # Calcula o tamanho do lote
                lot_size = self.calculate_lot_size(order["asset"], order["stop_loss"])
                
                if lot_size <= 0:
                    logger.error(f"Tamanho do lote inválido ({lot_size}), ordem ignorada: {order}")
                    self.order_queue.task_done()
                    continue
                
                # Envia a ordem a mercado
                mt5_ticket = self.send_market_order(
                    asset=order["asset"],
                    order_type=order["type"],
                    volume=lot_size,
                    stop_loss=order["stop_loss"],
                    take_profit=order["take_profit"],
                    signal_details=order["signal_details"]
                )
                
                # Registra a ordem no banco de dados
                status = "OPEN" if mt5_ticket else "FAILED"
                signal_timestamp = order["signal_details"].get("timestamp")
                strategy_profile = order["signal_details"].get("strategy_profile")
                reasons = ", ".join(order["signal_details"].get("reasons", []))
                
                try:
                    cursor = self.db_conn.cursor()
                    orders_table = self.config["database"]["orders_table"]
                    cursor.execute(f"""
                    INSERT INTO {orders_table} (mt5_ticket, asset, type, volume, price, sl, tp, status, open_time, signal_timestamp, strategy_profile, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (mt5_ticket, order["asset"], order["type"], lot_size, order["signal_details"].get("entry_price", 0), 
                          order["stop_loss"], order["take_profit"], status, datetime.now().isoformat(), 
                          signal_timestamp, strategy_profile, reasons))
                    self.db_conn.commit()
                    logger.info(f"Ordem registrada no banco de dados (Status: {status})")
                except Exception as db_err:
                    logger.error(f"Erro ao registrar ordem no banco de dados: {db_err}")
                    logger.error(traceback.format_exc())
                
                # Marca a tarefa como concluída
                self.order_queue.task_done()
                
            except queue.Empty:
                # Fila vazia, continua esperando
                continue
            except Exception as e:
                logger.error(f"Erro na thread de processamento de ordens: {e}")
                logger.error(traceback.format_exc())
                time.sleep(5) # Pausa em caso de erro
        
        logger.info("Thread de processamento de ordens finalizada")
    
    def signal_processor_thread(self):
        """
        Thread dedicada para ler sinais do arquivo JSON e adicioná-los à fila de ordens
        """
        logger.info("Thread de processamento de sinais iniciada")
        signal_file = self.config["signals"]["signal_file"]
        polling_interval = self.config["signals"]["polling_interval"]
        
        while not self.stop_event.is_set():
            try:
                # Verifica se o arquivo de sinais existe
                if not os.path.exists(signal_file):
                    logger.debug(f"Arquivo de sinais não encontrado: {signal_file}")
                    time.sleep(polling_interval)
                    continue
                
                # Verifica se o arquivo foi modificado desde a última leitura
                current_mtime = os.path.getmtime(signal_file)
                if current_mtime <= self.last_signal_file_mtime:
                    #logger.debug("Arquivo de sinais não modificado")
                    time.sleep(polling_interval)
                    continue
                
                logger.info(f"Arquivo de sinais modificado, lendo novos sinais...")
                self.last_signal_file_mtime = current_mtime
                
                # Lê o arquivo JSON
                with open(signal_file, 'r', encoding='utf-8') as f:
                    try:
                        signals = json.load(f)
                        if not isinstance(signals, list):
                            logger.error("Formato inválido no arquivo de sinais, esperado uma lista JSON")
                            signals = []
                    except json.JSONDecodeError as json_err:
                        logger.error(f"Erro ao decodificar JSON do arquivo de sinais: {json_err}")
                        signals = []
                
                # Processa cada sinal
                new_signals_count = 0
                for signal in signals:
                    try:
                        # Usa o timestamp do sinal como identificador único
                        signal_timestamp = signal.get("timestamp")
                        if not signal_timestamp:
                            logger.warning(f"Sinal sem timestamp ignorado: {signal}")
                            continue
                        
                        # Verifica se o sinal já foi processado
                        if signal_timestamp in self.processed_signal_timestamps:
                            #logger.debug(f"Sinal já processado ignorado: {signal_timestamp}")
                            continue
                        
                        # Validação básica do sinal
                        required_keys = ["asset", "type", "stop_loss", "take_profit", "timestamp"]
                        if not all(key in signal for key in required_keys):
                            logger.warning(f"Sinal inválido ignorado: {signal}")
                            self.processed_signal_timestamps.add(signal_timestamp) # Marca como processado mesmo se inválido
                            continue
                        
                        # Adiciona à fila de ordens
                        order_details = {
                            "asset": signal["asset"],
                            "type": signal["type"],
                            "stop_loss": signal["stop_loss"],
                            "take_profit": signal["take_profit"],
                            "signal_details": signal # Passa todos os detalhes do sinal
                        }
                        self.add_order_to_queue(order_details)
                        self.processed_signal_timestamps.add(signal_timestamp)
                        new_signals_count += 1
                        
                    except Exception as signal_err:
                        logger.error(f"Erro ao processar sinal individual: {signal_err}")
                        logger.error(f"Sinal com erro: {signal}")
                        logger.error(traceback.format_exc())
                        # Marca como processado para não tentar novamente
                        if signal_timestamp:
                            self.processed_signal_timestamps.add(signal_timestamp)
                
                if new_signals_count > 0:
                    logger.info(f"{new_signals_count} novos sinais processados do arquivo {signal_file}")
                
            except FileNotFoundError:
                logger.debug(f"Arquivo de sinais não encontrado: {signal_file}")
            except Exception as e:
                logger.error(f"Erro na thread de processamento de sinais: {e}")
                logger.error(traceback.format_exc())
            
            # Aguarda o intervalo de polling
            time.sleep(polling_interval)
        
        logger.info("Thread de processamento de sinais finalizada")
    
    def position_monitor_thread(self):
        """
        Thread dedicada para monitorar posições abertas
        (Pode implementar lógicas como trailing stop, fechamento por tempo, etc.)
        """
        logger.info("Thread de monitoramento de posições iniciada")
        monitor_interval = 30.0 # Intervalo de monitoramento em segundos
        
        while not self.stop_event.is_set():
            try:
                if not self.mt5_initialized:
                    time.sleep(monitor_interval)
                    continue
                
                open_positions = self.get_open_positions()
                
                if open_positions:
                    logger.info(f"Monitorando {len(open_positions)} posições abertas:")
                    total_profit = 0
                    for pos in open_positions:
                        logger.info(f"  - Ticket: {pos['ticket']}, Ativo: {pos['symbol']}, Tipo: {pos['type']}, Vol: {pos['volume']}, Lucro: {pos['profit']:.2f}")
                        total_profit += pos['profit']
                    logger.info(f"Lucro total das posições abertas: {total_profit:.2f}")
                else:
                    logger.debug("Nenhuma posição aberta para monitorar")
                
                # Implementar lógicas de monitoramento aqui (ex: trailing stop)
                # Exemplo: verificar se SL/TP foram atingidos (embora o MT5 faça isso)
                # Exemplo: fechar posições no final do dia
                
                # Verifica se precisa fechar posições no final do dia
                trading_hours = self.config["trading"]["trading_hours"]
                now = datetime.now().time()
                end_time = datetime.strptime(trading_hours["end"], "%H:%M").time()
                # Fecha X minutos antes do fim do pregão
                close_time_threshold = (datetime.combine(datetime.today(), end_time) - timedelta(minutes=2)).time()
                
                if now >= close_time_threshold and open_positions:
                    logger.warning(f"Horário de fechamento próximo ({close_time_threshold}). Fechando todas as posições...")
                    self.close_all_positions(reason="Fim do pregão")
                
            except Exception as e:
                logger.error(f"Erro na thread de monitoramento de posições: {e}")
                logger.error(traceback.format_exc())
            
            # Aguarda o intervalo de monitoramento
            time.sleep(monitor_interval)
        
        logger.info("Thread de monitoramento de posições finalizada")
    
    def start(self):
        """
        Inicia o executor de ordens e suas threads
        
        Returns:
            bool: True se iniciado com sucesso, False caso contrário
        """
        logger.info("Iniciando MT5 Order Executor v3.0...")
        
        # Conecta ao MT5
        if not self.connect_mt5():
            logger.error("Falha ao conectar ao MT5. Abortando.")
            return False
        
        # Configura banco de dados
        if not self.setup_database_connection():
            logger.error("Falha ao configurar banco de dados. Abortando.")
            self.disconnect_mt5()
            return False
        
        # Reseta o evento de parada
        self.stop_event.clear()
        
        # Inicia as threads
        self.threads = []
        
        order_thread = threading.Thread(target=self.order_processor_thread, name="OrderProcessor")
        self.threads.append(order_thread)
        
        signal_thread = threading.Thread(target=self.signal_processor_thread, name="SignalProcessor")
        self.threads.append(signal_thread)
        
        monitor_thread = threading.Thread(target=self.position_monitor_thread, name="PositionMonitor")
        self.threads.append(monitor_thread)
        
        for thread in self.threads:
            thread.start()
        
        logger.info("MT5 Order Executor iniciado com sucesso")
        return True
    
    def stop(self):
        """
        Para o executor de ordens e suas threads
        """
        logger.info("Parando MT5 Order Executor...")
        
        # Sinaliza para as threads pararem
        self.stop_event.set()
        
        # Aguarda as threads terminarem
        for thread in self.threads:
            thread.join(timeout=5.0)
        
        # Fecha a conexão com o banco de dados
        if self.db_conn:
            self.db_conn.close()
            logger.info("Conexão com banco de dados fechada")
        
        # Desconecta do MT5
        self.disconnect_mt5()
        
        logger.info("MT5 Order Executor parado com sucesso")
    
    def get_status(self):
        """
        Retorna o status atual do executor
        
        Returns:
            dict: Dicionário com informações de status
        """
        open_positions_info = self.get_open_positions()
        total_profit = sum(p["profit"] for p in open_positions_info)
        
        account_details = {}
        if self.account_info:
            account_details = {
                "login": self.account_info.login,
                "server": self.account_info.server,
                "balance": self.account_info.balance,
                "equity": self.account_info.equity,
                "margin": self.account_info.margin,
                "free_margin": self.account_info.margin_free
            }
        
        return {
            "connected": self.mt5_initialized,
            "trading_allowed": self.check_trading_allowed(),
            "order_queue_size": self.order_queue.qsize(),
            "signal_queue_size": self.signal_queue.qsize(), # Mantido para compatibilidade
            "positions": {
                "count": len(open_positions_info),
                "total_profit": total_profit,
                "positions": open_positions_info
            },
            "daily_stats": self.daily_stats,
            "running": not self.stop_event.is_set(),
            "last_update": datetime.now().isoformat(),
            "account": account_details
        }

def main():
    """
    Função principal para execução do script
    """
    executor = MT5OrderExecutor()
    
    if not executor.start():
        logger.error("Falha ao iniciar o MT5 Order Executor")
        return
    
    try:
        # Mantém a thread principal viva para exibir status ou receber comandos
        while not executor.stop_event.is_set():
            status = executor.get_status()
            logger.info(f"Status: {status}")
            time.sleep(60) # Exibe status a cada minuto
            
    except KeyboardInterrupt:
        logger.info("Interrupção pelo usuário detectada")
    finally:
        executor.stop()

if __name__ == "__main__":
    main()

