#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Market Analyzer Robust - Módulo de análise e geração de sinais para o sistema MFT
com validação rigorosa de dados e tratamento de inconsistências

Este módulo implementa a análise de dados de fluxo de ordens e geração de sinais
de trading baseados em princípios de Wyckoff, análise quantitativa e estatística.
Executa múltiplos perfis de estratégia simultaneamente com verificação robusta de dados.

Autor: Manus
Data: 23/05/2025
Versão: 4.0 - Implementação de validação rigorosa de dados e tratamento de inconsistências
"""

import sqlite3
import pandas as pd
import numpy as np
import time
import threading
import queue
from datetime import datetime, timedelta
import os
import logging
import json
import sys
import traceback
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib
matplotlib.use('Agg')  # Modo não-interativo para salvar gráficos

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("market_analyzer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("MarketAnalyzer")

class MarketAnalyzer:
    """
    Classe principal para análise de mercado e geração de sinais
    com múltiplos perfis de estratégia simultâneos e validação rigorosa de dados
    
    Arquitetura:
    - Thread principal: responsável pela análise e geração de sinais
    - Thread de monitoramento: verifica novos dados no banco
    - Thread de comunicação: envia sinais para o executor de ordens
    """
    
    def __init__(self, config_file="analyzer_config.json"):
        """
        Inicializa o analisador de mercado com configurações do arquivo JSON
        
        Args:
            config_file (str): Caminho para o arquivo de configuração JSON
        """
        logger.info(f"Inicializando Market Analyzer Robust v4.0 com arquivo de configuração: {config_file}")
        self.config_file = config_file
        self.config = self._load_config(config_file)
        self.db_conn = None
        self.stop_event = threading.Event()
        self.signal_queue = queue.Queue()
        self.threads = []
        self.last_analyzed_timestamp = {}
        self.market_data = {}
        self.signals_history = {}  # Dicionário por perfil de estratégia
        self.charts_dir = os.path.join(os.getcwd(), "charts")
        self.last_chart_time = {}  # Controle de tempo para limitar geração de gráficos
        self.last_signal_time = {}  # Controle de tempo para limitar sinais
        self.data_quality_issues = {}  # Registro de problemas de qualidade de dados
        
        # Inicializa histórico de sinais para cada perfil
        for profile in self.config.get("strategy_profiles", {}).keys():
            self.signals_history[profile] = []
        
        # Cria diretório para gráficos se não existir
        if not os.path.exists(self.charts_dir):
            os.makedirs(self.charts_dir)
            logger.info(f"Diretório para gráficos criado: {self.charts_dir}")
    
    def _load_config(self, config_file):
        """
        Carrega configurações do arquivo JSON
        
        Args:
            config_file (str): Caminho para o arquivo de configuração
            
        Returns:
            dict: Configurações carregadas ou configurações padrão em caso de erro
        """
        default_config = {
            "database": {
                "db_path": "market_data.db",
                "table_prefix": "rtd_data"
            },
            "analysis": {
                "polling_interval": 10.0,  # segundos (10 segundos)
                "lookback_periods": 100,  # número de períodos para análise
                "chart_interval": 300,  # segundos entre geração de gráficos (5 minutos)
                "signal_interval": 300,  # segundos entre sinais para o mesmo ativo (5 minutos)
                "wyckoff": {
                    "enabled": True,
                    "accumulation_threshold": 0.6,
                    "distribution_threshold": 0.6
                },
                "order_flow": {
                    "enabled": True,
                    "aggression_threshold": 1.5,
                    "absorption_threshold": 2.0,
                    "exhaustion_threshold": 3.0
                },
                "momentum": {
                    "enabled": True,
                    "fast_period": 5,
                    "slow_period": 20,
                    "signal_threshold": 0.5
                }
            },
            "signals": {
                "min_confidence": 0.6,  # confiança mínima para gerar sinal
                "confirmation_required": True,  # requer confirmação de múltiplas estratégias
                "risk_reward_min": 1.2  # relação risco/recompensa mínima
            },
            "assets": {
                "winfut": {
                    "enabled": True,
                    "point_value": 0.2,  # valor do ponto
                    "tick_size": 5,  # tamanho do tick
                    "trading_hours": {
                        "start": "09:00",
                        "end": "17:55"
                    }
                },
                "wdofut": {
                    "enabled": True,
                    "point_value": 10.0,  # valor do ponto
                    "tick_size": 0.5,  # tamanho do tick
                    "trading_hours": {
                        "start": "09:00",
                        "end": "17:55"
                    }
                }
            },
            "mt5_executor": {
                "enabled": True,
                "signal_file": "trading_signals.json"
            },
            "strategy_profiles": {
                "conservador": {
                    "enabled": True,
                    "signals": {
                        "min_confidence": 0.75,
                        "confirmation_required": True,
                        "risk_reward_min": 1.8
                    },
                    "analysis": {
                        "wyckoff": {
                            "accumulation_threshold": 0.7,
                            "distribution_threshold": 0.7
                        },
                        "order_flow": {
                            "aggression_threshold": 2.0,
                            "absorption_threshold": 2.5,
                            "exhaustion_threshold": 3.5
                        },
                        "momentum": {
                            "signal_threshold": 0.7
                        }
                    }
                },
                "moderado": {
                    "enabled": True,
                    "signals": {
                        "min_confidence": 0.6,
                        "confirmation_required": True,
                        "risk_reward_min": 1.5
                    },
                    "analysis": {
                        "wyckoff": {
                            "accumulation_threshold": 0.6,
                            "distribution_threshold": 0.6
                        },
                        "order_flow": {
                            "aggression_threshold": 1.5,
                            "absorption_threshold": 2.0,
                            "exhaustion_threshold": 3.0
                        },
                        "momentum": {
                            "signal_threshold": 0.5
                        }
                    }
                },
                "agressivo": {
                    "enabled": True,
                    "signals": {
                        "min_confidence": 0.5,
                        "confirmation_required": False,
                        "risk_reward_min": 1.2
                    },
                    "analysis": {
                        "wyckoff": {
                            "accumulation_threshold": 0.5,
                            "distribution_threshold": 0.5
                        },
                        "order_flow": {
                            "aggression_threshold": 1.2,
                            "absorption_threshold": 1.5,
                            "exhaustion_threshold": 2.5
                        },
                        "momentum": {
                            "signal_threshold": 0.4
                        }
                    }
                }
            },
            "data_validation": {
                "required_columns": {
                    "basic": ["timestamp", "ultimo", "maximo", "minimo"],
                    "wyckoff": ["volume"],
                    "order_flow": ["agressao_compra", "agressao_venda", "agressao_saldo"],
                    "momentum": []
                },
                "fallback_values": {
                    "volume": 0,
                    "agressao_compra": 0,
                    "agressao_venda": 0,
                    "agressao_saldo": 0
                },
                "min_data_points": 10,
                "max_missing_ratio": 0.3  # máximo de 30% de dados ausentes permitidos
            }
        }
        
        try:
            if os.path.exists(config_file):
                logger.info(f"Carregando configurações do arquivo: {config_file}")
                with open(config_file, 'r', encoding='utf-8') as f:
                    loaded_config = json.load(f)
                    
                    # Verifica se a configuração carregada tem a estrutura esperada
                    if "strategy_profiles" not in loaded_config:
                        logger.warning("Configuração carregada não contém 'strategy_profiles'. Usando configuração padrão.")
                        
                        # Se o arquivo existe mas não tem a estrutura esperada, renomeia e cria um novo
                        backup_file = f"{config_file}.bak"
                        logger.info(f"Renomeando arquivo de configuração existente para {backup_file}")
                        os.rename(config_file, backup_file)
                        
                        # Cria um novo arquivo com as configurações padrão
                        with open(config_file, 'w', encoding='utf-8') as f_new:
                            json.dump(default_config, f_new, indent=4)
                        
                        logger.info(f"Novo arquivo de configuração criado com valores padrão: {config_file}")
                        return default_config
                    
                    return loaded_config
            else:
                # Se o arquivo não existir, cria com as configurações padrão
                logger.info(f"Arquivo de configuração não encontrado. Criando com valores padrão: {config_file}")
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, indent=4)
                logger.info(f"Arquivo de configuração {config_file} criado com valores padrão")
                return default_config
        except Exception as e:
            logger.error(f"Erro ao carregar configurações: {e}")
            logger.error(traceback.format_exc())
            logger.info("Usando configurações padrão devido ao erro")
            return default_config
    
    def _get_profile_config(self, profile_name):
        """
        Obtém configuração específica para um perfil de estratégia
        
        Args:
            profile_name (str): Nome do perfil de estratégia
            
        Returns:
            dict: Configuração combinada (base + perfil específico)
        """
        if profile_name not in self.config.get("strategy_profiles", {}):
            logger.warning(f"Perfil de estratégia '{profile_name}' não encontrado. Usando configurações padrão.")
            return self.config
        
        # Cria uma cópia profunda da configuração base
        profile_config = json.loads(json.dumps(self.config))
        
        # Aplica configurações específicas do perfil
        profile = self.config["strategy_profiles"][profile_name]
        
        # Aplica configurações de sinais
        if "signals" in profile:
            for key, value in profile["signals"].items():
                profile_config["signals"][key] = value
        
        # Aplica configurações de análise
        if "analysis" in profile:
            for category, settings in profile["analysis"].items():
                if category in profile_config["analysis"]:
                    for key, value in settings.items():
                        profile_config["analysis"][category][key] = value
        
        return profile_config
    
    def setup_database_connection(self):
        """
        Configura a conexão com o banco de dados SQLite
        
        Returns:
            bool: True se a conexão foi estabelecida com sucesso, False caso contrário
        """
        try:
            db_config = self.config["database"]
            db_path = db_config["db_path"]
            
            # Verifica se o arquivo existe
            if not os.path.exists(db_path):
                logger.error(f"Banco de dados não encontrado: {db_path}")
                return False
            
            # Conecta ao banco de dados
            self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
            
            # Configura para retornar resultados como dicionários
            self.db_conn.row_factory = sqlite3.Row
            
            logger.info(f"Conectado ao banco de dados: {db_path}")
            
            # Verifica as tabelas disponíveis
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"Tabelas disponíveis no banco de dados: {tables}")
            
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def validate_data_columns(self, df, asset, analysis_type):
        """
        Valida se o DataFrame contém as colunas necessárias para um tipo de análise
        
        Args:
            df (pd.DataFrame): DataFrame a ser validado
            asset (str): Nome do ativo
            analysis_type (str): Tipo de análise ('basic', 'wyckoff', 'order_flow', 'momentum')
            
        Returns:
            tuple: (bool, list) - Indica se a validação passou e lista de colunas ausentes
        """
        if df is None or df.empty:
            logger.warning(f"DataFrame vazio ou None para {asset} na validação de {analysis_type}")
            return False, ["DataFrame vazio"]
        
        # Obtém a lista de colunas necessárias para o tipo de análise
        required_columns = self.config.get("data_validation", {}).get("required_columns", {}).get(analysis_type, [])
        
        # Verifica quais colunas estão ausentes
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        # Se houver colunas ausentes, registra o problema
        if missing_columns:
            issue_key = f"{asset}_{analysis_type}"
            if issue_key not in self.data_quality_issues:
                logger.warning(f"Colunas ausentes para {asset} na análise de {analysis_type}: {missing_columns}")
                self.data_quality_issues[issue_key] = {
                    "asset": asset,
                    "analysis_type": analysis_type,
                    "missing_columns": missing_columns,
                    "first_detected": datetime.now().isoformat(),
                    "count": 1
                }
            else:
                self.data_quality_issues[issue_key]["count"] += 1
                # Loga apenas a cada 10 ocorrências para evitar spam no log
                if self.data_quality_issues[issue_key]["count"] % 10 == 0:
                    logger.warning(f"Colunas ausentes para {asset} na análise de {analysis_type}: {missing_columns} (ocorrência {self.data_quality_issues[issue_key]['count']})")
            
            return False, missing_columns
        
        return True, []
    
    def prepare_dataframe(self, df, asset, analysis_type):
        """
        Prepara o DataFrame para análise, adicionando colunas ausentes com valores padrão
        
        Args:
            df (pd.DataFrame): DataFrame original
            asset (str): Nome do ativo
            analysis_type (str): Tipo de análise ('basic', 'wyckoff', 'order_flow', 'momentum')
            
        Returns:
            pd.DataFrame: DataFrame preparado para análise
        """
        if df is None or df.empty:
            logger.warning(f"DataFrame vazio ou None para {asset} na preparação para {analysis_type}")
            return None
        
        # Cria uma cópia do DataFrame para não modificar o original
        prepared_df = df.copy()
        
        # Obtém a lista de colunas necessárias para o tipo de análise
        required_columns = self.config.get("data_validation", {}).get("required_columns", {}).get(analysis_type, [])
        
        # Obtém valores padrão para colunas ausentes
        fallback_values = self.config.get("data_validation", {}).get("fallback_values", {})
        
        # Adiciona colunas ausentes com valores padrão
        for col in required_columns:
            if col not in prepared_df.columns:
                default_value = fallback_values.get(col, 0)
                logger.debug(f"Adicionando coluna ausente {col} para {asset} com valor padrão {default_value}")
                prepared_df[col] = default_value
        
        return prepared_df
    
    def get_latest_data(self, asset, limit=None):
        """
        Obtém os dados mais recentes do banco de dados para um ativo
        
        Args:
            asset (str): Nome do ativo ('winfut' ou 'wdofut')
            limit (int, optional): Limite de registros a retornar. Se None, usa lookback_periods da configuração.
            
        Returns:
            pd.DataFrame: DataFrame com os dados mais recentes ou None em caso de erro
        """
        try:
            if not self.db_conn:
                logger.error("Conexão com banco de dados não estabelecida")
                return None
            
            table_name = f"{self.config['database']['table_prefix']}_{asset}"
            
            # Verifica se a tabela existe
            cursor = self.db_conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            if not cursor.fetchone():
                logger.error(f"Tabela {table_name} não encontrada no banco de dados")
                return None
            
            # Define o limite de registros
            if limit is None:
                limit = self.config["analysis"]["lookback_periods"]
            
            # Obtém o timestamp do último registro analisado
            last_timestamp = self.last_analyzed_timestamp.get(asset)
            
            # Prepara a query
            query = f"SELECT * FROM {table_name} ORDER BY timestamp DESC LIMIT {limit}"
            
            # Executa a query
            cursor = self.db_conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning(f"Nenhum dado encontrado para {asset}")
                return None
            
            # Converte para DataFrame
            df = pd.DataFrame([dict(row) for row in rows])
            
            # Verifica se o DataFrame tem as colunas básicas necessárias
            valid, missing = self.validate_data_columns(df, asset, "basic")
            if not valid:
                logger.error(f"Dados para {asset} não contêm colunas básicas necessárias: {missing}")
                return None
            
            # Converte timestamp para datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Ordena por timestamp (mais antigo primeiro)
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # Converte colunas numéricas
            numeric_columns = ['ultimo', 'abertura', 'maximo', 'minimo', 'variacao', 
                              'agressao_compra', 'agressao_venda', 'agressao_saldo', 'volume']
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Preenche valores NaN com 0 para evitar erros
            df = df.fillna(0)
            
            # Atualiza o timestamp do último registro analisado
            if not df.empty:
                self.last_analyzed_timestamp[asset] = df['timestamp'].max().isoformat()
            
            # Registra estatísticas sobre os dados
            logger.debug(f"Dados obtidos para {asset}: {len(df)} registros, período de {df['timestamp'].min()} a {df['timestamp'].max()}")
            
            return df
        except Exception as e:
            logger.error(f"Erro ao obter dados para {asset}: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def analyze_wyckoff(self, df, asset, profile_config):
        """
        Implementa análise baseada nos princípios de Wyckoff
        
        Args:
            df (pd.DataFrame): DataFrame com os dados do ativo
            asset (str): Nome do ativo
            profile_config (dict): Configuração específica do perfil
            
        Returns:
            dict: Resultados da análise de Wyckoff
        """
        try:
            # Validação básica de dados
            if df is None or df.empty:
                return {"valid": False, "message": "Dados insuficientes para análise"}
            
            # Configurações
            wyckoff_config = profile_config["analysis"]["wyckoff"]
            if not wyckoff_config["enabled"]:
                return {"valid": False, "message": "Análise de Wyckoff desativada"}
            
            # Valida e prepara o DataFrame para análise de Wyckoff
            valid, missing = self.validate_data_columns(df, asset, "wyckoff")
            if not valid:
                # Prepara o DataFrame com colunas padrão para continuar a análise
                df = self.prepare_dataframe(df, asset, "wyckoff")
                logger.debug(f"Usando valores padrão para colunas ausentes na análise de Wyckoff: {missing}")
            
            # Resultados
            results = {
                "valid": True,
                "phase": None,  # Acumulação, Distribuição, Uptrend, Downtrend
                "signals": [],
                "confidence": 0.0,
                "support_level": None,
                "resistance_level": None,
                "volume_analysis": {},
                "price_analysis": {}
            }
            
            # 1. Análise de Volume e Preço (Lei de Esforço vs. Resultado)
            # Calcula a média móvel de volume e preço
            if 'volume' in df.columns:
                # Verifica se há dados suficientes para calcular médias móveis
                if len(df) >= 5:
                    df['volume_ma'] = df['volume'].rolling(window=5).mean()
                    
                    # Divergência de volume e preço
                    df['price_change'] = df['ultimo'].pct_change()
                    df['volume_change'] = df['volume'].pct_change()
                    
                    # Preenche valores NaN com 0
                    df['price_change'] = df['price_change'].fillna(0)
                    df['volume_change'] = df['volume_change'].fillna(0)
                    
                    # Verifica divergência: preço sobe mas volume cai (possível distribuição)
                    price_up_volume_down = ((df['price_change'] > 0) & (df['volume_change'] < 0)).sum()
                    
                    # Verifica divergência: preço desce mas volume sobe (possível acumulação)
                    price_down_volume_up = ((df['price_change'] < 0) & (df['volume_change'] > 0)).sum()
                    
                    results["volume_analysis"] = {
                        "price_up_volume_down": price_up_volume_down,
                        "price_down_volume_up": price_down_volume_up,
                        "effort_result_ratio": price_up_volume_down / (price_down_volume_up + 1)  # Evita divisão por zero
                    }
                else:
                    logger.debug(f"Dados insuficientes para análise de volume para {asset} (len={len(df)})")
            
            # 2. Análise de Suporte e Resistência (Lei de Oferta e Demanda)
            # Identifica níveis de suporte e resistência usando mínimos e máximos recentes
            if len(df) >= 10:
                # Pega os últimos N períodos para análise
                recent_df = df.tail(20)
                
                # Encontra mínimos e máximos locais
                min_prices = recent_df['minimo'].nsmallest(3).tolist()
                max_prices = recent_df['maximo'].nlargest(3).tolist()
                
                # Define suporte como a média dos mínimos recentes
                results["support_level"] = sum(min_prices) / len(min_prices)
                
                # Define resistência como a média dos máximos recentes
                results["resistance_level"] = sum(max_prices) / len(max_prices)
                
                # Calcula a largura do range
                range_width = results["resistance_level"] - results["support_level"]
                
                # Posição atual do preço no range
                last_price = df['ultimo'].iloc[-1]
                if range_width > 0:
                    position_in_range = (last_price - results["support_level"]) / range_width
                    results["price_analysis"]["position_in_range"] = position_in_range
                    
                    # Determina a fase com base na posição no range e no volume
                    if position_in_range < 0.3:
                        # Próximo ao suporte
                        if 'volume_analysis' in results and results["volume_analysis"].get("price_down_volume_up", 0) > 5:
                            results["phase"] = "Acumulação"
                            results["signals"].append({"type": "Compra", "reason": "Acumulação próxima ao suporte"})
                            results["confidence"] = 0.7
                    elif position_in_range > 0.7:
                        # Próximo à resistência
                        if 'volume_analysis' in results and results["volume_analysis"].get("price_up_volume_down", 0) > 5:
                            results["phase"] = "Distribuição"
                            results["signals"].append({"type": "Venda", "reason": "Distribuição próxima à resistência"})
                            results["confidence"] = 0.7
            
            # 3. Análise de Causa e Efeito (Potencial de movimento)
            # Estima o potencial de movimento com base no tamanho da fase de acumulação/distribuição
            if results["phase"] == "Acumulação" and "support_level" in results and "resistance_level" in results:
                potential_target = results["resistance_level"] + (results["resistance_level"] - results["support_level"])
                results["price_analysis"]["potential_target"] = potential_target
                results["price_analysis"]["risk_reward"] = (potential_target - df['ultimo'].iloc[-1]) / (df['ultimo'].iloc[-1] - results["support_level"])
            elif results["phase"] == "Distribuição" and "support_level" in results and "resistance_level" in results:
                potential_target = results["support_level"] - (results["resistance_level"] - results["support_level"])
                results["price_analysis"]["potential_target"] = potential_target
                results["price_analysis"]["risk_reward"] = (df['ultimo'].iloc[-1] - potential_target) / (results["resistance_level"] - df['ultimo'].iloc[-1])
            
            return results
        except Exception as e:
            logger.error(f"Erro na análise de Wyckoff para {asset}: {e}")
            logger.error(traceback.format_exc())
            return {"valid": False, "message": f"Erro na análise: {str(e)}"}
    
    def analyze_order_flow(self, df, asset, profile_config):
        """
        Implementa análise de fluxo de ordens (agressão, absorção, exaustão)
        
        Args:
            df (pd.DataFrame): DataFrame com os dados do ativo
            asset (str): Nome do ativo
            profile_config (dict): Configuração específica do perfil
            
        Returns:
            dict: Resultados da análise de fluxo de ordens
        """
        try:
            # Validação básica de dados
            if df is None or df.empty:
                return {"valid": False, "message": "Dados insuficientes para análise"}
            
            # Configurações
            order_flow_config = profile_config["analysis"]["order_flow"]
            if not order_flow_config["enabled"]:
                return {"valid": False, "message": "Análise de fluxo de ordens desativada"}
            
            # Valida e prepara o DataFrame para análise de fluxo de ordens
            valid, missing = self.validate_data_columns(df, asset, "order_flow")
            if not valid:
                # Prepara o DataFrame com colunas padrão para continuar a análise
                df = self.prepare_dataframe(df, asset, "order_flow")
                logger.debug(f"Usando valores padrão para colunas ausentes na análise de fluxo de ordens: {missing}")
            
            # Resultados
            results = {
                "valid": True,
                "signals": [],
                "confidence": 0.0,
                "aggression_analysis": {},
                "absorption_analysis": {},
                "exhaustion_analysis": {}
            }
            
            # Verifica se temos as colunas necessárias após a preparação
            required_columns = ['agressao_compra', 'agressao_venda', 'agressao_saldo']
            if not all(col in df.columns for col in required_columns):
                logger.error(f"Colunas necessárias ainda ausentes após preparação para {asset}: {[col for col in required_columns if col not in df.columns]}")
                return {"valid": False, "message": "Dados de fluxo de ordens não disponíveis mesmo após preparação"}
            
            # 1. Análise de Agressão
            # Calcula o saldo de agressão e sua média móvel
            if len(df) >= 5:
                df['agressao_saldo_ma5'] = df['agressao_saldo'].rolling(window=5).mean().fillna(0)
            else:
                df['agressao_saldo_ma5'] = df['agressao_saldo']
            
            if len(df) >= 20:
                df['agressao_saldo_ma20'] = df['agressao_saldo'].rolling(window=20).mean().fillna(0)
            else:
                df['agressao_saldo_ma20'] = df['agressao_saldo']
            
            # Pega os últimos valores
            last_saldo = df['agressao_saldo'].iloc[-1] if not df.empty else 0
            last_saldo_ma5 = df['agressao_saldo_ma5'].iloc[-1] if not df.empty else 0
            last_saldo_ma20 = df['agressao_saldo_ma20'].iloc[-1] if not df.empty else 0
            
            # Calcula a força da agressão
            aggression_strength = last_saldo / max(1, abs(last_saldo_ma20))
            
            results["aggression_analysis"] = {
                "current_balance": last_saldo,
                "short_term_ma": last_saldo_ma5,
                "long_term_ma": last_saldo_ma20,
                "strength": aggression_strength
            }
            
            # Gera sinais com base na agressão
            if aggression_strength > order_flow_config["aggression_threshold"]:
                if last_saldo > 0:
                    results["signals"].append({
                        "type": "Compra", 
                        "reason": "Forte agressão compradora", 
                        "strength": aggression_strength
                    })
                    results["confidence"] = min(0.9, 0.5 + aggression_strength / 10)
                elif last_saldo < 0:
                    results["signals"].append({
                        "type": "Venda", 
                        "reason": "Forte agressão vendedora", 
                        "strength": -aggression_strength
                    })
                    results["confidence"] = min(0.9, 0.5 + abs(aggression_strength) / 10)
            
            # 2. Análise de Absorção
            # Verifica se o preço não se move significativamente apesar de forte agressão
            if len(df) >= 5:
                # Calcula a variação de preço nos últimos períodos
                recent_price_change = (df['ultimo'].iloc[-1] - df['ultimo'].iloc[-5]) / max(0.01, df['ultimo'].iloc[-5])
                
                # Calcula o volume de agressão acumulado nos últimos períodos
                recent_aggression_volume = df['agressao_compra'].iloc[-5:].sum() + df['agressao_venda'].iloc[-5:].sum()
                
                # Calcula o saldo de agressão acumulado
                recent_aggression_balance = df['agressao_saldo'].iloc[-5:].sum()
                
                # Determina se há absorção (alto volume de agressão com pouca variação de preço)
                absorption_ratio = recent_aggression_volume / max(0.001, abs(recent_price_change))
                
                results["absorption_analysis"] = {
                    "recent_price_change": recent_price_change,
                    "recent_aggression_volume": recent_aggression_volume,
                    "recent_aggression_balance": recent_aggression_balance,
                    "absorption_ratio": absorption_ratio
                }
                
                # Gera sinais com base na absorção
                if absorption_ratio > order_flow_config["absorption_threshold"]:
                    if recent_aggression_balance > 0 and recent_price_change <= 0:
                        # Absorção de venda (agressão compradora não move o preço para cima)
                        results["signals"].append({
                            "type": "Venda", 
                            "reason": "Absorção de compra detectada", 
                            "strength": absorption_ratio / order_flow_config["absorption_threshold"]
                        })
                        results["confidence"] = max(results["confidence"], min(0.8, 0.4 + absorption_ratio / 20))
                    elif recent_aggression_balance < 0 and recent_price_change >= 0:
                        # Absorção de compra (agressão vendedora não move o preço para baixo)
                        results["signals"].append({
                            "type": "Compra", 
                            "reason": "Absorção de venda detectada", 
                            "strength": absorption_ratio / order_flow_config["absorption_threshold"]
                        })
                        results["confidence"] = max(results["confidence"], min(0.8, 0.4 + absorption_ratio / 20))
            
            # 3. Análise de Exaustão
            # Verifica se há sinais de exaustão do movimento (agressão forte seguida de reversão)
            if len(df) >= 10:
                # Calcula a média de agressão nos períodos anteriores
                previous_aggression = df['agressao_saldo'].iloc[-10:-5].mean()
                recent_aggression = df['agressao_saldo'].iloc[-5:].mean()
                
                # Verifica se houve mudança significativa na direção da agressão
                aggression_direction_change = previous_aggression * recent_aggression
                
                # Calcula a variação de preço
                previous_price_change = (df['ultimo'].iloc[-5] - df['ultimo'].iloc[-10]) / max(0.01, df['ultimo'].iloc[-10])
                recent_price_change = (df['ultimo'].iloc[-1] - df['ultimo'].iloc[-5]) / max(0.01, df['ultimo'].iloc[-5])
                
                # Verifica se houve mudança na direção do preço
                price_direction_change = previous_price_change * recent_price_change
                
                # Determina se há exaustão (mudança na direção da agressão e/ou preço)
                exhaustion_detected = (aggression_direction_change < 0 or price_direction_change < 0)
                
                results["exhaustion_analysis"] = {
                    "previous_aggression": previous_aggression,
                    "recent_aggression": recent_aggression,
                    "aggression_direction_change": aggression_direction_change,
                    "previous_price_change": previous_price_change,
                    "recent_price_change": recent_price_change,
                    "price_direction_change": price_direction_change,
                    "exhaustion_detected": exhaustion_detected
                }
                
                # Gera sinais com base na exaustão
                if exhaustion_detected:
                    if previous_aggression > 0 and recent_aggression < 0:
                        # Exaustão de compra
                        results["signals"].append({
                            "type": "Venda", 
                            "reason": "Exaustão de compra detectada", 
                            "strength": abs(recent_aggression / max(0.01, previous_aggression))
                        })
                        results["confidence"] = max(results["confidence"], 0.75)
                    elif previous_aggression < 0 and recent_aggression > 0:
                        # Exaustão de venda
                        results["signals"].append({
                            "type": "Compra", 
                            "reason": "Exaustão de venda detectada", 
                            "strength": abs(recent_aggression / max(0.01, previous_aggression))
                        })
                        results["confidence"] = max(results["confidence"], 0.75)
            
            return results
        except Exception as e:
            logger.error(f"Erro na análise de fluxo de ordens para {asset}: {e}")
            logger.error(traceback.format_exc())
            return {"valid": False, "message": f"Erro na análise: {str(e)}"}
    
    def analyze_momentum(self, df, asset, profile_config):
        """
        Implementa análise de momentum e tendência
        
        Args:
            df (pd.DataFrame): DataFrame com os dados do ativo
            asset (str): Nome do ativo
            profile_config (dict): Configuração específica do perfil
            
        Returns:
            dict: Resultados da análise de momentum
        """
        try:
            # Validação básica de dados
            if df is None or df.empty:
                return {"valid": False, "message": "Dados insuficientes para análise"}
            
            # Configurações
            momentum_config = profile_config["analysis"]["momentum"]
            if not momentum_config["enabled"]:
                return {"valid": False, "message": "Análise de momentum desativada"}
            
            # Valida e prepara o DataFrame para análise de momentum
            valid, missing = self.validate_data_columns(df, asset, "momentum")
            if not valid:
                # Prepara o DataFrame com colunas padrão para continuar a análise
                df = self.prepare_dataframe(df, asset, "momentum")
                logger.debug(f"Usando valores padrão para colunas ausentes na análise de momentum: {missing}")
            
            # Resultados
            results = {
                "valid": True,
                "signals": [],
                "confidence": 0.0,
                "trend": None,
                "momentum_strength": 0.0,
                "indicators": {}
            }
            
            # Verifica se temos dados suficientes
            fast_period = momentum_config["fast_period"]
            slow_period = momentum_config["slow_period"]
            
            if len(df) < slow_period:
                logger.debug(f"Dados insuficientes para cálculo de médias móveis para {asset} (len={len(df)}, required={slow_period})")
                return {"valid": False, "message": f"Dados insuficientes para cálculo de médias móveis (len={len(df)}, required={slow_period})"}
            
            # 1. Médias Móveis
            # Calcula médias móveis rápida e lenta
            df['ma_fast'] = df['ultimo'].rolling(window=fast_period).mean().fillna(df['ultimo'])
            df['ma_slow'] = df['ultimo'].rolling(window=slow_period).mean().fillna(df['ultimo'])
            
            # Calcula a diferença entre as médias móveis
            df['ma_diff'] = df['ma_fast'] - df['ma_slow']
            df['ma_diff_pct'] = df['ma_diff'] / df['ma_slow'].replace(0, 1)  # Evita divisão por zero
            
            # Pega os últimos valores
            last_price = df['ultimo'].iloc[-1]
            last_ma_fast = df['ma_fast'].iloc[-1]
            last_ma_slow = df['ma_slow'].iloc[-1]
            last_ma_diff = df['ma_diff'].iloc[-1]
            last_ma_diff_pct = df['ma_diff_pct'].iloc[-1]
            
            # Determina a tendência
            if last_ma_diff > 0:
                results["trend"] = "Alta"
                results["momentum_strength"] = last_ma_diff_pct
            else:
                results["trend"] = "Baixa"
                results["momentum_strength"] = -last_ma_diff_pct
            
            results["indicators"]["moving_averages"] = {
                "fast": last_ma_fast,
                "slow": last_ma_slow,
                "diff": last_ma_diff,
                "diff_pct": last_ma_diff_pct
            }
            
            # 2. Cruzamentos de Médias Móveis
            # Verifica se houve cruzamento recente
            if len(df) >= 3:
                prev_ma_diff = df['ma_diff'].iloc[-2]
                
                # Cruzamento para cima (Golden Cross)
                if prev_ma_diff <= 0 and last_ma_diff > 0:
                    results["signals"].append({
                        "type": "Compra", 
                        "reason": "Cruzamento de médias para cima (Golden Cross)", 
                        "strength": abs(last_ma_diff_pct) * 10
                    })
                    results["confidence"] = min(0.85, 0.6 + abs(last_ma_diff_pct) * 5)
                
                # Cruzamento para baixo (Death Cross)
                elif prev_ma_diff >= 0 and last_ma_diff < 0:
                    results["signals"].append({
                        "type": "Venda", 
                        "reason": "Cruzamento de médias para baixo (Death Cross)", 
                        "strength": abs(last_ma_diff_pct) * 10
                    })
                    results["confidence"] = min(0.85, 0.6 + abs(last_ma_diff_pct) * 5)
            
            # 3. Momentum (Rate of Change)
            # Calcula a taxa de variação do preço
            if len(df) >= 5:
                df['roc_5'] = df['ultimo'].pct_change(periods=5) * 100
                last_roc_5 = df['roc_5'].iloc[-1] if not pd.isna(df['roc_5'].iloc[-1]) else 0
            else:
                last_roc_5 = 0
            
            if len(df) >= 10:
                df['roc_10'] = df['ultimo'].pct_change(periods=10) * 100
                last_roc_10 = df['roc_10'].iloc[-1] if not pd.isna(df['roc_10'].iloc[-1]) else 0
            else:
                last_roc_10 = 0
            
            results["indicators"]["rate_of_change"] = {
                "roc_5": last_roc_5,
                "roc_10": last_roc_10
            }
            
            # Gera sinais com base no momentum
            if abs(last_roc_5) > momentum_config["signal_threshold"]:
                if last_roc_5 > 0:
                    # Momentum positivo
                    if results["trend"] == "Alta":
                        # Confirmação de tendência
                        results["signals"].append({
                            "type": "Compra", 
                            "reason": "Momentum positivo em tendência de alta", 
                            "strength": last_roc_5 / momentum_config["signal_threshold"]
                        })
                        results["confidence"] = max(results["confidence"], min(0.8, 0.5 + last_roc_5 / 10))
                else:
                    # Momentum negativo
                    if results["trend"] == "Baixa":
                        # Confirmação de tendência
                        results["signals"].append({
                            "type": "Venda", 
                            "reason": "Momentum negativo em tendência de baixa", 
                            "strength": abs(last_roc_5) / momentum_config["signal_threshold"]
                        })
                        results["confidence"] = max(results["confidence"], min(0.8, 0.5 + abs(last_roc_5) / 10))
            
            return results
        except Exception as e:
            logger.error(f"Erro na análise de momentum para {asset}: {e}")
            logger.error(traceback.format_exc())
            return {"valid": False, "message": f"Erro na análise: {str(e)}"}
    
    def generate_trading_signal(self, asset, analysis_results, profile_name, profile_config):
        """
        Gera sinais de trading com base nos resultados das análises
        
        Args:
            asset (str): Nome do ativo
            analysis_results (dict): Resultados das análises
            profile_name (str): Nome do perfil de estratégia
            profile_config (dict): Configuração específica do perfil
            
        Returns:
            dict: Sinal de trading ou None se não houver sinal
        """
        try:
            # Configurações
            signal_config = profile_config["signals"]
            asset_config = profile_config["assets"].get(asset, {})
            
            # Verifica se o ativo está habilitado
            if not asset_config.get("enabled", False):
                return None
            
            # Verifica se já enviamos um sinal recentemente para este ativo e perfil
            now = datetime.now()
            signal_key = f"{asset}_{profile_name}"
            last_signal_time = self.last_signal_time.get(signal_key)
            signal_interval = profile_config["analysis"]["signal_interval"]
            
            if last_signal_time and (now - last_signal_time).total_seconds() < signal_interval:
                logger.debug(f"Sinal para {asset} (perfil {profile_name}) ignorado: intervalo mínimo não atingido")
                return None
            
            # Extrai sinais de cada análise
            wyckoff_signals = analysis_results.get("wyckoff", {}).get("signals", [])
            order_flow_signals = analysis_results.get("order_flow", {}).get("signals", [])
            momentum_signals = analysis_results.get("momentum", {}).get("signals", [])
            
            # Combina todos os sinais
            all_signals = wyckoff_signals + order_flow_signals + momentum_signals
            
            if not all_signals:
                return None
            
            # Conta sinais de compra e venda
            buy_signals = [s for s in all_signals if s.get("type") == "Compra"]
            sell_signals = [s for s in all_signals if s.get("type") == "Venda"]
            
            # Determina o tipo de sinal predominante
            signal_type = "Compra" if len(buy_signals) > len(sell_signals) else "Venda"
            
            # Filtra sinais do tipo predominante
            filtered_signals = buy_signals if signal_type == "Compra" else sell_signals
            
            if not filtered_signals:
                return None
            
            # Calcula a confiança média dos sinais
            confidence = sum(s.get("confidence", 0) for s in filtered_signals) / len(filtered_signals) if filtered_signals else 0
            
            # Considera também a confiança das análises individuais
            if "wyckoff" in analysis_results and analysis_results["wyckoff"].get("valid", False):
                confidence = max(confidence, analysis_results["wyckoff"].get("confidence", 0))
            if "order_flow" in analysis_results and analysis_results["order_flow"].get("valid", False):
                confidence = max(confidence, analysis_results["order_flow"].get("confidence", 0))
            if "momentum" in analysis_results and analysis_results["momentum"].get("valid", False):
                confidence = max(confidence, analysis_results["momentum"].get("confidence", 0))
            
            # Verifica se a confiança é suficiente
            if confidence < signal_config["min_confidence"]:
                logger.debug(f"Sinal para {asset} (perfil {profile_name}) ignorado: confiança insuficiente ({confidence:.2f} < {signal_config['min_confidence']:.2f})")
                return None
            
            # Verifica se requer confirmação de múltiplas estratégias
            if signal_config["confirmation_required"]:
                # Conta quantas estratégias diferentes geraram sinais do mesmo tipo
                strategies_with_signals = 0
                if any(s.get("type") == signal_type for s in wyckoff_signals):
                    strategies_with_signals += 1
                if any(s.get("type") == signal_type for s in order_flow_signals):
                    strategies_with_signals += 1
                if any(s.get("type") == signal_type for s in momentum_signals):
                    strategies_with_signals += 1
                
                # Requer pelo menos 2 estratégias diferentes
                if strategies_with_signals < 2:
                    logger.debug(f"Sinal para {asset} (perfil {profile_name}) ignorado: confirmação insuficiente ({strategies_with_signals} < 2)")
                    return None
            
            # Determina níveis de entrada, stop e alvo
            last_price = self.market_data.get(asset, {}).get("ultimo")
            if not last_price:
                # Tenta obter o último preço dos resultados da análise
                if "wyckoff" in analysis_results and analysis_results["wyckoff"].get("valid", False):
                    support_level = analysis_results["wyckoff"].get("support_level")
                    resistance_level = analysis_results["wyckoff"].get("resistance_level")
                else:
                    # Valores padrão se não houver análise de Wyckoff
                    support_level = None
                    resistance_level = None
                
                # Tenta obter o último preço do DataFrame original
                df = self.get_latest_data(asset, limit=1)
                if df is not None and not df.empty:
                    last_price = df['ultimo'].iloc[-1]
                else:
                    logger.error(f"Não foi possível determinar o último preço para {asset}")
                    return None
            else:
                # Obtém níveis de suporte e resistência da análise de Wyckoff
                support_level = analysis_results.get("wyckoff", {}).get("support_level")
                resistance_level = analysis_results.get("wyckoff", {}).get("resistance_level")
            
            # Define níveis com base no tipo de sinal
            if signal_type == "Compra":
                entry_price = last_price
                stop_loss = support_level if support_level else last_price * 0.99
                take_profit = resistance_level if resistance_level else last_price * 1.02
            else:  # Venda
                entry_price = last_price
                stop_loss = resistance_level if resistance_level else last_price * 1.01
                take_profit = support_level if support_level else last_price * 0.98
            
            # Calcula relação risco/recompensa
            risk = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            risk_reward_ratio = reward / risk if risk > 0 else 0
            
            # Verifica se a relação risco/recompensa é suficiente
            if risk_reward_ratio < signal_config["risk_reward_min"]:
                logger.debug(f"Sinal para {asset} (perfil {profile_name}) ignorado: relação risco/recompensa insuficiente ({risk_reward_ratio:.2f} < {signal_config['risk_reward_min']:.2f})")
                return None
            
            # Atualiza o timestamp do último sinal
            self.last_signal_time[signal_key] = now
            
            # Cria o sinal de trading
            trading_signal = {
                "asset": asset,
                "type": signal_type,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "confidence": confidence,
                "risk_reward_ratio": risk_reward_ratio,
                "timestamp": now.isoformat(),
                "reasons": [s.get("reason") for s in filtered_signals if "reason" in s],
                "analysis": {
                    "wyckoff": analysis_results.get("wyckoff", {}).get("valid", False),
                    "order_flow": analysis_results.get("order_flow", {}).get("valid", False),
                    "momentum": analysis_results.get("momentum", {}).get("valid", False)
                },
                "strategy_profile": profile_name
            }
            
            logger.info(f"Sinal de trading gerado para {asset} (perfil {profile_name}): {signal_type} @ {entry_price:.2f}")
            logger.info(f"Stop Loss: {stop_loss:.2f}, Take Profit: {take_profit:.2f}")
            logger.info(f"Confiança: {confidence:.2f}, Risco/Recompensa: {risk_reward_ratio:.2f}")
            
            return trading_signal
        except Exception as e:
            logger.error(f"Erro ao gerar sinal de trading para {asset} (perfil {profile_name}): {e}")
            logger.error(traceback.format_exc())
            return None
    
    def send_signal_to_executor(self, signal):
        """
        Envia sinal de trading para o executor de ordens MT5
        
        Args:
            signal (dict): Sinal de trading
            
        Returns:
            bool: True se o sinal foi enviado com sucesso, False caso contrário
        """
        try:
            if not signal:
                return False
            
            # Verifica se o executor MT5 está habilitado
            if not self.config["mt5_executor"]["enabled"]:
                logger.info(f"Executor MT5 desabilitado, sinal não enviado: {signal}")
                return False
            
            # Adiciona o sinal ao histórico do perfil correspondente
            profile_name = signal.get("strategy_profile", "default")
            if profile_name in self.signals_history:
                self.signals_history[profile_name].append(signal)
            
            # Salva o sinal em arquivo JSON
            signal_file = self.config["mt5_executor"]["signal_file"]
            
            # Lê sinais existentes, se houver
            existing_signals = []
            if os.path.exists(signal_file):
                try:
                    with open(signal_file, 'r', encoding='utf-8') as f:
                        existing_signals = json.load(f)
                except Exception as e:
                    logger.error(f"Erro ao ler arquivo de sinais existente: {e}")
                    existing_signals = []
            
            # Adiciona o novo sinal
            existing_signals.append(signal)
            
            # Salva o arquivo atualizado
            with open(signal_file, 'w', encoding='utf-8') as f:
                json.dump(existing_signals, f, indent=4)
            
            logger.info(f"Sinal de trading enviado para o executor MT5: {signal}")
            return True
        except Exception as e:
            logger.error(f"Erro ao enviar sinal para o executor MT5: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def should_generate_chart(self, asset, profile_name):
        """
        Verifica se deve gerar um gráfico para o ativo e perfil
        
        Args:
            asset (str): Nome do ativo
            profile_name (str): Nome do perfil de estratégia
            
        Returns:
            bool: True se deve gerar gráfico, False caso contrário
        """
        now = datetime.now()
        chart_key = f"{asset}_{profile_name}"
        last_chart_time = self.last_chart_time.get(chart_key)
        chart_interval = self.config["analysis"]["chart_interval"]
        
        if not last_chart_time or (now - last_chart_time).total_seconds() >= chart_interval:
            self.last_chart_time[chart_key] = now
            return True
        
        return False
    
    def generate_analysis_chart(self, df, asset, analysis_results, profile_name):
        """
        Gera gráfico de análise para o ativo
        
        Args:
            df (pd.DataFrame): DataFrame com os dados do ativo
            asset (str): Nome do ativo
            analysis_results (dict): Resultados das análises
            profile_name (str): Nome do perfil de estratégia
            
        Returns:
            str: Caminho do arquivo de gráfico gerado ou None em caso de erro
        """
        try:
            if df is None or df.empty:
                logger.warning(f"DataFrame vazio ou None para geração de gráfico de {asset} (perfil {profile_name})")
                return None
            
            # Verifica se deve gerar gráfico
            if not self.should_generate_chart(asset, profile_name):
                return None
            
            # Cria figura e eixos
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), gridspec_kw={'height_ratios': [3, 1, 1]})
            
            # Configura o formato da data
            date_format = DateFormatter('%H:%M')
            
            # Gráfico de preço
            ax1.plot(df['timestamp'], df['ultimo'], label='Preço', color='black')
            
            # Adiciona médias móveis se disponíveis
            if 'ma_fast' in df.columns and 'ma_slow' in df.columns:
                ax1.plot(df['timestamp'], df['ma_fast'], label=f'MM Rápida (5)', color='blue', alpha=0.7)
                ax1.plot(df['timestamp'], df['ma_slow'], label=f'MM Lenta (20)', color='red', alpha=0.7)
            
            # Adiciona níveis de suporte e resistência se disponíveis
            wyckoff_results = analysis_results.get("wyckoff", {})
            if wyckoff_results.get("valid", False):
                support_level = wyckoff_results.get("support_level")
                resistance_level = wyckoff_results.get("resistance_level")
                
                if support_level:
                    ax1.axhline(y=support_level, color='green', linestyle='--', alpha=0.7, label='Suporte')
                
                if resistance_level:
                    ax1.axhline(y=resistance_level, color='red', linestyle='--', alpha=0.7, label='Resistência')
            
            # Adiciona sinais de trading
            trading_signal = analysis_results.get("trading_signal")
            if trading_signal:
                signal_time = df['timestamp'].iloc[-1]
                
                if trading_signal["type"] == "Compra":
                    ax1.scatter(signal_time, trading_signal["entry_price"], marker='^', color='green', s=100, label='Sinal de Compra')
                else:
                    ax1.scatter(signal_time, trading_signal["entry_price"], marker='v', color='red', s=100, label='Sinal de Venda')
                
                # Adiciona níveis de stop e alvo
                ax1.axhline(y=trading_signal["stop_loss"], color='red', linestyle=':', alpha=0.7, label='Stop Loss')
                ax1.axhline(y=trading_signal["take_profit"], color='green', linestyle=':', alpha=0.7, label='Take Profit')
            
            # Configura o gráfico de preço
            ax1.set_title(f'Análise de {asset.upper()} - {datetime.now().strftime("%d/%m/%Y %H:%M:%S")} - Perfil: {profile_name}')
            ax1.set_ylabel('Preço')
            ax1.legend(loc='upper left')
            ax1.grid(True, alpha=0.3)
            ax1.xaxis.set_major_formatter(date_format)
            
            # Gráfico de volume se disponível
            if 'volume' in df.columns:
                ax2.bar(df['timestamp'], df['volume'], color='blue', alpha=0.5, label='Volume')
                ax2.set_ylabel('Volume')
                ax2.grid(True, alpha=0.3)
                ax2.xaxis.set_major_formatter(date_format)
                ax2.legend(loc='upper left')
            else:
                ax2.text(0.5, 0.5, 'Dados de volume não disponíveis', horizontalalignment='center', verticalalignment='center', transform=ax2.transAxes)
                ax2.set_ylabel('Volume')
                ax2.grid(False)
            
            # Gráfico de agressão se disponível
            if 'agressao_saldo' in df.columns:
                # Cria barras coloridas com base no sinal (positivo/negativo)
                colors = ['green' if x > 0 else 'red' for x in df['agressao_saldo']]
                ax3.bar(df['timestamp'], df['agressao_saldo'], color=colors, alpha=0.7, label='Saldo de Agressão')
                
                # Adiciona linha de média móvel do saldo de agressão
                if 'agressao_saldo_ma5' in df.columns:
                    ax3.plot(df['timestamp'], df['agressao_saldo_ma5'], color='blue', label='MM Agressão (5)')
                
                ax3.set_ylabel('Agressão')
                ax3.grid(True, alpha=0.3)
                ax3.xaxis.set_major_formatter(date_format)
                ax3.legend(loc='upper left')
            else:
                ax3.text(0.5, 0.5, 'Dados de agressão não disponíveis', horizontalalignment='center', verticalalignment='center', transform=ax3.transAxes)
                ax3.set_ylabel('Agressão')
                ax3.grid(False)
            
            # Ajusta o layout
            plt.tight_layout()
            
            # Salva o gráfico
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            chart_file = os.path.join(self.charts_dir, f"{asset}_{profile_name}_{timestamp}.png")
            plt.savefig(chart_file, dpi=150)
            plt.close(fig)
            
            logger.info(f"Gráfico de análise gerado: {chart_file}")
            return chart_file
        except Exception as e:
            logger.error(f"Erro ao gerar gráfico de análise para {asset} (perfil {profile_name}): {e}")
            logger.error(traceback.format_exc())
            return None
    
    def analyze_asset_with_profile(self, asset, profile_name):
        """
        Realiza análise completa de um ativo com um perfil específico
        
        Args:
            asset (str): Nome do ativo
            profile_name (str): Nome do perfil de estratégia
            
        Returns:
            dict: Resultados da análise
        """
        try:
            # Obtém configuração específica do perfil
            profile_config = self._get_profile_config(profile_name)
            
            # Verifica se o perfil está habilitado
            if not profile_config["strategy_profiles"].get(profile_name, {}).get("enabled", False):
                logger.debug(f"Perfil {profile_name} desabilitado para {asset}")
                return {"valid": False, "message": f"Perfil {profile_name} desabilitado"}
            
            # Obtém os dados mais recentes
            df = self.get_latest_data(asset)
            if df is None or df.empty:
                logger.warning(f"Sem dados para análise de {asset} (perfil {profile_name})")
                return {"valid": False, "message": "Sem dados para análise"}
            
            # Verifica se há dados suficientes para análise
            min_data_points = self.config.get("data_validation", {}).get("min_data_points", 10)
            if len(df) < min_data_points:
                logger.warning(f"Dados insuficientes para análise de {asset} (perfil {profile_name}): {len(df)} < {min_data_points}")
                return {"valid": False, "message": f"Dados insuficientes para análise: {len(df)} < {min_data_points}"}
            
            # Armazena os últimos dados para referência
            if not df.empty:
                self.market_data[asset] = {
                    "ultimo": df['ultimo'].iloc[-1],
                    "timestamp": df['timestamp'].iloc[-1].isoformat()
                }
            
            # Realiza as análises
            wyckoff_results = self.analyze_wyckoff(df, asset, profile_config)
            order_flow_results = self.analyze_order_flow(df, asset, profile_config)
            momentum_results = self.analyze_momentum(df, asset, profile_config)
            
            # Combina os resultados
            analysis_results = {
                "asset": asset,
                "profile": profile_name,
                "timestamp": datetime.now().isoformat(),
                "wyckoff": wyckoff_results,
                "order_flow": order_flow_results,
                "momentum": momentum_results
            }
            
            # Gera sinal de trading
            trading_signal = self.generate_trading_signal(asset, analysis_results, profile_name, profile_config)
            analysis_results["trading_signal"] = trading_signal
            
            # Gera gráfico de análise apenas se houver sinal ou periodicamente
            if trading_signal or self.should_generate_chart(asset, profile_name):
                chart_file = self.generate_analysis_chart(df, asset, analysis_results, profile_name)
                analysis_results["chart_file"] = chart_file
            
            # Envia sinal para o executor, se houver
            if trading_signal:
                self.send_signal_to_executor(trading_signal)
            
            return analysis_results
        except Exception as e:
            logger.error(f"Erro na análise de {asset} com perfil {profile_name}: {e}")
            logger.error(traceback.format_exc())
            return {"valid": False, "message": f"Erro na análise: {str(e)}"}
    
    def start(self):
        """
        Inicia o analisador de mercado
        
        Returns:
            bool: True se iniciado com sucesso, False caso contrário
        """
        logger.info("Iniciando Market Analyzer Robust v4.0 com múltiplos perfis de estratégia...")
        
        # Conecta ao banco de dados
        if not self.setup_database_connection():
            logger.error("Falha ao conectar ao banco de dados. Abortando.")
            return False
        
        # Reseta o evento de parada
        self.stop_event.clear()
        
        # Lista os perfis habilitados
        enabled_profiles = [
            profile for profile, config in self.config.get("strategy_profiles", {}).items()
            if config.get("enabled", False)
        ]
        
        if not enabled_profiles:
            logger.warning("Nenhum perfil de estratégia habilitado. Usando perfil padrão 'moderado'.")
            enabled_profiles = ["moderado"]
        
        logger.info(f"Perfis de estratégia habilitados: {', '.join(enabled_profiles)}")
        
        # Lista os ativos habilitados
        enabled_assets = [
            asset for asset, config in self.config["assets"].items()
            if config.get("enabled", False)
        ]
        
        if not enabled_assets:
            logger.warning("Nenhum ativo habilitado. Usando ativos padrão 'winfut' e 'wdofut'.")
            enabled_assets = ["winfut", "wdofut"]
        
        logger.info(f"Ativos habilitados: {', '.join(enabled_assets)}")
        
        # Verifica se as tabelas necessárias existem no banco de dados
        if self.db_conn:
            cursor = self.db_conn.cursor()
            for asset in enabled_assets:
                table_name = f"{self.config['database']['table_prefix']}_{asset}"
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
                if not cursor.fetchone():
                    logger.warning(f"Tabela {table_name} não encontrada no banco de dados. O ativo {asset} pode não funcionar corretamente.")
        
        logger.info("Market Analyzer iniciado com sucesso")
        return True
    
    def stop(self):
        """
        Para o analisador de mercado
        """
        logger.info("Parando Market Analyzer...")
        
        # Sinaliza para as threads pararem
        self.stop_event.set()
        
        # Aguarda as threads terminarem
        for thread in self.threads:
            thread.join(timeout=5.0)
        
        # Fecha a conexão com o banco de dados
        if self.db_conn:
            self.db_conn.close()
            logger.info("Conexão com banco de dados fechada")
        
        # Registra estatísticas de problemas de qualidade de dados
        if self.data_quality_issues:
            logger.info("Resumo de problemas de qualidade de dados:")
            for issue_key, issue_data in self.data_quality_issues.items():
                logger.info(f"  {issue_key}: {issue_data['count']} ocorrências, primeira detecção em {issue_data['first_detected']}")
        
        logger.info("Market Analyzer parado com sucesso")
    
    def run_main_loop(self):
        """
        Loop principal para análise e geração de sinais
        Esta função deve ser chamada pela thread principal após iniciar o analisador
        """
        polling_interval = self.config["analysis"]["polling_interval"]
        
        try:
            logger.info(f"Iniciando loop principal com intervalo de polling de {polling_interval} segundos")
            
            while not self.stop_event.is_set():
                try:
                    # Obtém a lista de perfis habilitados
                    enabled_profiles = [
                        profile for profile, config in self.config.get("strategy_profiles", {}).items()
                        if config.get("enabled", False)
                    ]
                    
                    if not enabled_profiles:
                        enabled_profiles = ["moderado"]
                        logger.warning("Nenhum perfil habilitado encontrado, usando perfil padrão 'moderado'")
                    
                    # Analisa cada ativo habilitado com cada perfil habilitado
                    for asset, asset_config in self.config["assets"].items():
                        if asset_config.get("enabled", False):
                            # Verifica se está dentro do horário de trading
                            now = datetime.now().time()
                            trading_hours = asset_config.get("trading_hours", {})
                            start_time = datetime.strptime(trading_hours.get("start", "00:00"), "%H:%M").time()
                            end_time = datetime.strptime(trading_hours.get("end", "23:59"), "%H:%M").time()
                            
                            if start_time <= now <= end_time:
                                # Realiza a análise com cada perfil
                                for profile_name in enabled_profiles:
                                    analysis_results = self.analyze_asset_with_profile(asset, profile_name)
                                    
                                    if analysis_results.get("valid", False):
                                        logger.debug(f"Análise de {asset} com perfil {profile_name} concluída com sucesso")
                                    elif "message" in analysis_results:
                                        logger.debug(f"Análise de {asset} com perfil {profile_name} falhou: {analysis_results['message']}")
                            else:
                                logger.debug(f"Fora do horário de trading para {asset}: {now} (horário: {start_time}-{end_time})")
                    
                    # A cada 5 minutos, exibe um resumo do status
                    current_time = time.time()
                    if not hasattr(self, "_last_status_time") or current_time - self._last_status_time >= 300:
                        # Conta sinais por perfil
                        signals_count = {profile: len(signals) for profile, signals in self.signals_history.items()}
                        logger.info(f"Status: Analisador em execução, sinais gerados por perfil: {signals_count}")
                        
                        # Registra estatísticas de problemas de qualidade de dados
                        if self.data_quality_issues:
                            logger.info("Problemas de qualidade de dados detectados:")
                            for issue_key, issue_data in list(self.data_quality_issues.items())[:5]:  # Limita a 5 para não sobrecarregar o log
                                logger.info(f"  {issue_key}: {issue_data['count']} ocorrências")
                            if len(self.data_quality_issues) > 5:
                                logger.info(f"  ... e mais {len(self.data_quality_issues) - 5} problemas")
                        
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
    try:
        # Verifica se o arquivo de configuração existe e está correto
        config_file = "analyzer_config.json"
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                if "strategy_profiles" not in config:
                    logger.warning(f"Arquivo de configuração {config_file} não contém 'strategy_profiles'. Renomeando para backup.")
                    os.rename(config_file, f"{config_file}.bak")
            except Exception as e:
                logger.warning(f"Erro ao ler arquivo de configuração {config_file}: {e}. Renomeando para backup.")
                if os.path.exists(config_file):
                    os.rename(config_file, f"{config_file}.bak")
        
        analyzer = MarketAnalyzer()
        
        if not analyzer.start():
            logger.error("Falha ao iniciar o Market Analyzer")
            return
        
        # Executa o loop principal na thread principal
        analyzer.run_main_loop()
    except Exception as e:
        logger.error(f"Erro na função principal: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
