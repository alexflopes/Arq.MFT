#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Profit RTD Reader (Win32COM Version) - Script para leitura direta do Excel via Win32COM

Este script implementa a leitura de dados de fluxo de ordens do Profit Chart
exportados para Excel via RTD (Real-Time Data), usando Win32COM para acessar
diretamente as células do Excel, sem dependências externas como pandas ou xlwings.

Autor: Manus
Data: 19/05/2025
Versão: 7.0 - Abordagem via Win32COM puro
"""

import win32com.client
import pythoncom
import time
import threading
import queue
from datetime import datetime
import sqlite3
import os
import logging
import json
import sys
import traceback

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("profit_rtd_reader.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ProfitRTDReader")

class ProfitRTDReader:
    """
    Classe principal para leitura de dados do Excel via Win32COM
    
    Arquitetura:
    - Thread principal: responsável pela leitura do Excel (evita problemas de marshalling COM)
    - Thread de processamento: responsável pelo armazenamento e análise dos dados
    """
    
    def __init__(self, config_file="config.json"):
        """
        Inicializa o leitor RTD com configurações do arquivo JSON
        
        Args:
            config_file (str): Caminho para o arquivo de configuração JSON
        """
        self.config = self._load_config(config_file)
        self.excel_app = None
        self.workbook = None
        self.db_conn = None
        self.stop_event = threading.Event()
        self.data_queue = queue.Queue()
        self.signal_queue = queue.Queue()
        self.last_data = {}  # Cache para comparação e detecção de mudanças
        self.threads = []
        self.cell_errors = {}  # Rastreia células com erros para evitar log excessivo
        
    def _load_config(self, config_file):
        """
        Carrega configurações do arquivo JSON
        
        Args:
            config_file (str): Caminho para o arquivo de configuração
            
        Returns:
            dict: Configurações carregadas ou configurações padrão em caso de erro
        """
        default_config = {
            "excel": {
                "workbook_path": "C:\\MFT_Trading\\RTD_Profit.xlsx",
                "polling_interval": 0.5  # segundos
            },
            "database": {
                "db_path": "market_data.db",
                "table_name": "rtd_data"
            },
            "cell_ranges": {
                "winfut": {
                    "data": "RTD!B17",
                    "hora": "RTD!C17",
                    "ultimo": "RTD!D17",
                    "abertura": "RTD!E17",
                    "maximo": "RTD!F17",
                    "minimo": "RTD!G17",
                    "variacao": "RTD!J17",
                    "agressao_compra": "RTD!AO17",
                    "agressao_venda": "RTD!AQ17",
                    "agressao_saldo": "RTD!AP17"
                },
                "wdofut": {
                    "data": "RTD!B15",
                    "hora": "RTD!C15",
                    "ultimo": "RTD!D15",
                    "abertura": "RTD!E15",
                    "maximo": "RTD!F15",
                    "minimo": "RTD!G15",
                    "variacao": "RTD!J15",
                    "agressao_compra": "RTD!AO15",
                    "agressao_venda": "RTD!AQ15",
                    "agressao_saldo": "RTD!AP15"
                },
                "winfut_replay": {
                    "ultimo": "RTD!D22",
                    "maximo": "RTD!F22",
                    "minimo": "RTD!G22",
                    "agressao_compra": "RTD!AO22",
                    "agressao_venda": "RTD!AQ22",
                    "agressao_saldo": "RTD!AP22"
                },
                "wdofut_replay": {
                    "ultimo": "RTD!D22",
                    "maximo": "RTD!F22",
                    "minimo": "RTD!G22",
                    "agressao_compra": "RTD!AO22",
                    "agressao_venda": "RTD!AQ22",
                    "agressao_saldo": "RTD!AP22"
                }
            }
        }
        
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # Se o arquivo não existir, cria com as configurações padrão
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, indent=4)
                logger.info(f"Arquivo de configuração {config_file} criado com valores padrão")
                return default_config
        except Exception as e:
            logger.error(f"Erro ao carregar configurações: {e}")
            logger.error(traceback.format_exc())
            return default_config
    
    def setup_excel_connection(self):
        """
        Estabelece conexão com o Excel e abre o arquivo de dados
        
        Returns:
            bool: True se a conexão foi estabelecida com sucesso, False caso contrário
        """
        try:
            # Inicializa COM para esta thread (thread principal)
            pythoncom.CoInitialize()
            
            excel_config = self.config["excel"]
            workbook_path = excel_config["workbook_path"]
            
            # Verifica se o arquivo existe
            if not os.path.exists(workbook_path):
                logger.error(f"Arquivo Excel não encontrado: {workbook_path}")
                return False
            
            # Tenta obter uma instância do Excel já aberta
            try:
                logger.info("Tentando conectar ao Excel já aberto...")
                self.excel_app = win32com.client.GetObject("Excel.Application")
                logger.info("Conectado ao Excel já aberto")
            except:
                # Se não conseguir, cria uma nova instância
                logger.info("Criando nova instância do Excel...")
                self.excel_app = win32com.client.Dispatch("Excel.Application")
                logger.info("Nova instância do Excel criada")
            
            # Torna o Excel visível (opcional, útil para debug)
            self.excel_app.Visible = True
            
            # Verifica se o arquivo já está aberto
            workbook_found = False
            for wb in self.excel_app.Workbooks:
                if wb.FullName.lower() == workbook_path.lower():
                    self.workbook = wb
                    workbook_found = True
                    logger.info(f"Arquivo já aberto: {workbook_path}")
                    break
            
            # Se o arquivo não estiver aberto, abre-o
            if not workbook_found:
                logger.info(f"Abrindo arquivo: {workbook_path}")
                self.workbook = self.excel_app.Workbooks.Open(workbook_path)
            
            logger.info(f"Conectado ao arquivo Excel: {workbook_path}")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar com Excel: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def setup_database(self):
        """
        Configura o banco de dados SQLite para armazenamento dos dados
        Cria tabelas dinamicamente com base nas configurações
        
        Returns:
            bool: True se o banco foi configurado com sucesso, False caso contrário
        """
        try:
            db_config = self.config["database"]
            db_path = db_config["db_path"]
            table_name = db_config["table_name"]
            
            # Conecta ao banco de dados (cria se não existir)
            self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
            
            # Para cada ativo configurado, cria uma tabela com colunas dinâmicas
            for asset, cell_ranges in self.config["cell_ranges"].items():
                # Primeiro, remove a tabela se já existir para garantir compatibilidade
                cursor = self.db_conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}_{asset}")
                
                # Cria a definição de colunas com base nas chaves do cell_ranges
                columns = ["timestamp TEXT PRIMARY KEY"]
                for column_name in cell_ranges.keys():
                    # Todas as colunas são TEXT para simplificar (podemos converter depois)
                    columns.append(f"{column_name} TEXT")
                
                # Cria a tabela com as colunas dinâmicas
                create_table_sql = f"CREATE TABLE {table_name}_{asset} ({', '.join(columns)})"
                cursor.execute(create_table_sql)
                
                # Cria índice para timestamp
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{asset}_timestamp ON {table_name}_{asset} (timestamp)")
                
                logger.info(f"Tabela {table_name}_{asset} criada com colunas: {', '.join(cell_ranges.keys())}")
            
            self.db_conn.commit()
            logger.info(f"Banco de dados configurado: {db_path}")
            return True
        except Exception as e:
            logger.error(f"Erro ao configurar banco de dados: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def _parse_cell_reference(self, cell_ref):
        """
        Converte uma referência de célula no formato 'Sheet!A1' para componentes
        
        Args:
            cell_ref (str): Referência da célula
            
        Returns:
            tuple: (sheet_name, cell) ou (None, None) em caso de erro
        """
        try:
            # Separa o nome da planilha e a referência da célula
            if '!' in cell_ref:
                sheet_name, cell = cell_ref.split('!')
                # Remove aspas simples do nome da planilha, se houver
                sheet_name = sheet_name.replace("'", "")
            else:
                sheet_name = None
                cell = cell_ref
            
            return sheet_name, cell
        except Exception as e:
            logger.error(f"Erro ao analisar referência de célula '{cell_ref}': {e}")
            return None, None
    
    def _safe_read_cell(self, cell_ref):
        """
        Lê uma célula de forma segura, com tratamento de erros
        
        Args:
            cell_ref (str): Referência da célula no formato 'Sheet!A1'
            
        Returns:
            tuple: (valor, None) ou (None, erro)
        """
        try:
            # Analisa a referência da célula
            sheet_name, cell = self._parse_cell_reference(cell_ref)
            
            if not sheet_name or not cell:
                return None, f"Referência de célula inválida: {cell_ref}"
            
            # Obtém a planilha
            try:
                sheet = self.workbook.Sheets(sheet_name)
            except:
                return None, f"Planilha '{sheet_name}' não encontrada"
            
            # Lê o valor da célula
            value = sheet.Range(cell).Value
            
            return value, None
        except Exception as e:
            return None, str(e)
    
    def read_rtd_data(self, asset):
        """
        Lê dados RTD específicos do Excel para um ativo
        IMPORTANTE: Esta função deve ser chamada apenas pela thread principal
        
        Args:
            asset (str): Nome do ativo ('winfut' ou 'wdofut')
            
        Returns:
            tuple: (dados lidos, timestamp) ou (None, None) em caso de erro
        """
        if asset not in self.config["cell_ranges"]:
            logger.error(f"Ativo {asset} não encontrado nas configurações")
            return None, None
        
        cell_ranges = self.config["cell_ranges"][asset]
        data = {}
        error_count = 0
        
        # Lê cada célula configurada
        for name, cell_ref in cell_ranges.items():
            # Verifica se já tivemos erro nesta célula recentemente
            cell_error_key = f"{asset}_{name}"
            if cell_error_key in self.cell_errors:
                last_error_time, error_count = self.cell_errors[cell_error_key]
                # Se o último erro foi há menos de 5 minutos e já tivemos mais de 3 erros,
                # pulamos esta célula para evitar log excessivo
                if (datetime.now() - last_error_time).total_seconds() < 300 and error_count > 3:
                    continue
            
            value, error = self._safe_read_cell(cell_ref)
            
            if error:
                # Registra o erro e atualiza o contador
                if cell_error_key not in self.cell_errors:
                    self.cell_errors[cell_error_key] = (datetime.now(), 1)
                else:
                    _, count = self.cell_errors[cell_error_key]
                    self.cell_errors[cell_error_key] = (datetime.now(), count + 1)
                
                logger.error(f"Erro ao ler {asset}.{name} ({cell_ref}): {error}")
                error_count += 1
                # Usa None para esta célula
                data[name] = None
            else:
                # Limpa o registro de erro se a leitura foi bem-sucedida
                if cell_error_key in self.cell_errors:
                    del self.cell_errors[cell_error_key]
                
                # Armazena o valor
                data[name] = str(value) if value is not None else None
        
        # Se tivermos erros em todas as células, retorna None
        if error_count == len(cell_ranges):
            logger.error(f"Falha ao ler dados para {asset}: todas as células resultaram em erro")
            return None, None
        
        timestamp = datetime.now().isoformat()
        return data, timestamp
    
    def store_data_in_db(self, asset, data, timestamp):
        """
        Armazena os dados lidos no banco de dados
        
        Args:
            asset (str): Nome do ativo ('winfut' ou 'wdofut')
            data (dict): Dados lidos do Excel
            timestamp (str): Timestamp ISO da leitura
            
        Returns:
            bool: True se os dados foram armazenados com sucesso, False caso contrário
        """
        if not data:
            return False
        
        try:
            table_name = f"{self.config['database']['table_name']}_{asset}"
            
            # Prepara a query de inserção
            columns = ["timestamp"] + list(data.keys())
            placeholders = ", ".join(["?"] * len(columns))
            
            query = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Prepara os valores para inserção
            values = [timestamp] + [data[key] for key in data.keys()]
            
            # Executa a inserção
            cursor = self.db_conn.cursor()
            cursor.execute(query, values)
            self.db_conn.commit()
            
            return True
        except Exception as e:
            logger.error(f"Erro ao armazenar dados no banco para {asset}: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def data_processor_thread(self):
        """
        Thread dedicada para processamento dos dados lidos e armazenamento no banco
        Esta thread não acessa o Excel diretamente, apenas processa dados da fila
        """
        logger.info("Thread de processamento de dados iniciada")
        
        while not self.stop_event.is_set():
            try:
                if not self.data_queue.empty():
                    # Obtém os próximos dados da fila
                    asset, data, timestamp = self.data_queue.get()
                    
                    # Armazena no banco de dados
                    success = self.store_data_in_db(asset, data, timestamp)
                    
                    if success:
                        logger.debug(f"Dados de {asset} armazenados com sucesso")
                    
                    # Aqui seria implementada a lógica de análise e geração de sinais
                    # Por enquanto, apenas registramos os dados recebidos
                    logger.debug(f"Dados processados para {asset}: {data}")
                    
                    # Marca a tarefa como concluída
                    self.data_queue.task_done()
                else:
                    # Se não há dados para processar, aguarda um pouco
                    time.sleep(0.01)
            except Exception as e:
                logger.error(f"Erro na thread de processamento: {e}")
                logger.error(traceback.format_exc())
                time.sleep(0.1)
    
    def start(self):
        """
        Inicia o leitor RTD, estabelecendo conexões e iniciando threads
        
        Returns:
            bool: True se iniciado com sucesso, False caso contrário
        """
        logger.info("Iniciando Profit RTD Reader (Win32COM Version)...")
        
        # Conecta ao Excel
        if not self.setup_excel_connection():
            logger.error("Falha ao conectar com Excel. Abortando.")
            return False
        
        # Configura o banco de dados
        if not self.setup_database():
            logger.error("Falha ao configurar banco de dados. Abortando.")
            return False
        
        # Reseta o evento de parada
        self.stop_event.clear()
        
        # Inicia apenas a thread de processamento
        # A leitura do Excel será feita na thread principal
        processor_thread = threading.Thread(target=self.data_processor_thread)
        processor_thread.daemon = True
        processor_thread.start()
        self.threads.append(processor_thread)
        
        logger.info("Profit RTD Reader iniciado com sucesso")
        return True
    
    def stop(self):
        """
        Para o leitor RTD, encerrando threads e conexões
        """
        logger.info("Parando Profit RTD Reader...")
        
        # Sinaliza para as threads pararem
        self.stop_event.set()
        
        # Aguarda as threads terminarem
        for thread in self.threads:
            thread.join(timeout=5.0)
        
        # Fecha a conexão com o banco de dados
        if self.db_conn:
            self.db_conn.close()
            logger.info("Conexão com banco de dados fechada")
        
        # Não fecha o Excel, pois pode estar sendo usado pelo usuário
        # Apenas libera as referências
        self.workbook = None
        self.excel_app = None
        
        # Finaliza COM
        pythoncom.CoUninitialize()
        
        logger.info("Profit RTD Reader parado com sucesso")
    
    def get_status(self):
        """
        Retorna o status atual do leitor
        
        Returns:
            dict: Status atual com informações sobre conexões e filas
        """
        return {
            "excel_connected": self.excel_app is not None and self.workbook is not None,
            "db_connected": self.db_conn is not None,
            "data_queue_size": self.data_queue.qsize(),
            "signal_queue_size": self.signal_queue.qsize(),
            "running": not self.stop_event.is_set() and all(t.is_alive() for t in self.threads),
            "last_update": datetime.now().isoformat(),
            "error_cells": len(self.cell_errors)
        }
    
    def run_main_loop(self):
        """
        Loop principal para leitura do Excel na thread principal
        Esta função deve ser chamada pela thread principal após iniciar o leitor
        """
        polling_interval = self.config["excel"]["polling_interval"]
        
        try:
            while not self.stop_event.is_set():
                try:
                    # Lê dados para cada ativo configurado
                    for asset in self.config["cell_ranges"].keys():
                        try:
                            asset_data, asset_timestamp = self.read_rtd_data(asset)
                            if asset_data:
                                # Verifica se os dados mudaram desde a última leitura
                                if asset not in self.last_data or asset_data != self.last_data[asset]:
                                    self.data_queue.put((asset, asset_data, asset_timestamp))
                                    self.last_data[asset] = asset_data.copy()
                                    logger.info(f"Novos dados detectados para {asset}")
                        except Exception as e:
                            logger.error(f"Erro ao processar dados para {asset}: {e}")
                            logger.error(traceback.format_exc())
                    
                    # A cada 60 segundos, exibe o status
                    current_time = time.time()
                    if not hasattr(self, "_last_status_time") or current_time - self._last_status_time >= 60:
                        status = self.get_status()
                        logger.info(f"Status: {status}")
                        self._last_status_time = current_time
                    
                    # Aguarda o intervalo de polling
                    time.sleep(polling_interval)
                except Exception as e:
                    logger.error(f"Erro no loop principal: {e}")
                    logger.error(traceback.format_exc())
                    time.sleep(1)  # Pausa maior em caso de erro
        except KeyboardInterrupt:
            logger.info("Interrupção pelo usuário detectada")
        finally:
            self.stop()


def main():
    """
    Função principal para execução do script
    """
    # Inicializa COM para a thread principal
    pythoncom.CoInitialize()
    
    try:
        reader = ProfitRTDReader()
        
        if not reader.start():
            logger.error("Falha ao iniciar o Profit RTD Reader")
            pythoncom.CoUninitialize()
            return
        
        # Executa o loop principal na thread principal
        reader.run_main_loop()
    except Exception as e:
        logger.error(f"Erro na função principal: {e}")
        logger.error(traceback.format_exc())
    finally:
        # Finaliza COM para a thread principal
        pythoncom.CoUninitialize()


if __name__ == "__main__":
    main()
