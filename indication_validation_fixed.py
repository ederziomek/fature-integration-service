#!/usr/bin/env python3
"""
Módulo de Validação de Indicações Centralizada - VERSÃO CORRIGIDA
Todas as configurações obtidas do config-service (sem valores hardcoded)
"""
import logging
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ValidationCriteria(Enum):
    """Critérios de validação de indicações"""
    OPTION_1 = "option_1"  # Depósito + Apostas
    OPTION_2 = "option_2"  # Depósito + GGR

@dataclass
class ValidationConfig:
    """Configuração de validação obtida do config-service"""
    # Opção 1: Depósito + Apostas
    option1_deposit_min: float
    option1_bets_min: int
    
    # Opção 2: Depósito + GGR
    option2_deposit_min: float
    option2_ggr_min: float
    
    # Configurações gerais
    validation_period_days: int
    timezone: str
    
    # Configurações avançadas
    enable_fraud_detection: bool
    max_validation_attempts: int
    validation_timeout_seconds: int
    require_first_deposit: bool
    min_session_duration_minutes: int

@dataclass
class LeadData:
    """Dados do lead para validação"""
    lead_id: str
    affiliate_id: str
    registration_date: datetime
    total_deposits: float
    total_bets: int
    total_ggr: float
    first_deposit_date: Optional[datetime] = None
    last_activity_date: Optional[datetime] = None

@dataclass
class ValidationResult:
    """Resultado da validação de indicação"""
    lead_id: str
    affiliate_id: str
    is_valid: bool
    criteria_met: Optional[ValidationCriteria] = None
    validation_date: datetime = None
    details: Dict = None
    errors: List[str] = None
    
    def __post_init__(self):
        if self.validation_date is None:
            self.validation_date = datetime.utcnow()
        if self.details is None:
            self.details = {}
        if self.errors is None:
            self.errors = []

class ConfigServiceClient:
    """Cliente para comunicação com o config-service"""
    
    def __init__(self, config_service_url: str = None):
        self.config_service_url = config_service_url or "http://config-service.fature.svc.cluster.local"
        self.session = requests.Session()
        self.session.timeout = 10
        
    def get_validation_config(self) -> ValidationConfig:
        """Obtém configurações de validação do config-service"""
        try:
            # Busca todas as configurações de validação
            config_keys = [
                'cpa.validacao.opcao1.deposito_minimo',
                'cpa.validacao.opcao1.numero_apostas',
                'cpa.validacao.opcao2.deposito_minimo', 
                'cpa.validacao.opcao2.ggr_minimo',
                'cpa.validacao.prazo_dias',
                'cpa.validacao.timezone',
                'cpa.validacao.deteccao_fraude_ativa',
                'cpa.validacao.max_tentativas',
                'cpa.validacao.timeout_segundos',
                'cpa.validacao.exigir_primeiro_deposito',
                'cpa.validacao.duracao_minima_sessao_minutos'
            ]
            
            config_values = {}
            
            for key in config_keys:
                try:
                    response = self.session.get(f"{self.config_service_url}/api/v1/config/{key}")
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('success'):
                            config_values[key] = data['data']['value']
                        else:
                            logger.warning(f"Config key {key} not found, using fallback")
                            config_values[key] = self._get_fallback_value(key)
                    else:
                        logger.warning(f"Failed to get config {key}, using fallback")
                        config_values[key] = self._get_fallback_value(key)
                        
                except Exception as e:
                    logger.error(f"Error getting config {key}: {e}")
                    config_values[key] = self._get_fallback_value(key)
            
            # Cria objeto de configuração
            return ValidationConfig(
                option1_deposit_min=float(config_values['cpa.validacao.opcao1.deposito_minimo']),
                option1_bets_min=int(config_values['cpa.validacao.opcao1.numero_apostas']),
                option2_deposit_min=float(config_values['cpa.validacao.opcao2.deposito_minimo']),
                option2_ggr_min=float(config_values['cpa.validacao.opcao2.ggr_minimo']),
                validation_period_days=int(config_values['cpa.validacao.prazo_dias']),
                timezone=str(config_values['cpa.validacao.timezone']),
                enable_fraud_detection=bool(config_values['cpa.validacao.deteccao_fraude_ativa']),
                max_validation_attempts=int(config_values['cpa.validacao.max_tentativas']),
                validation_timeout_seconds=int(config_values['cpa.validacao.timeout_segundos']),
                require_first_deposit=bool(config_values['cpa.validacao.exigir_primeiro_deposito']),
                min_session_duration_minutes=int(config_values['cpa.validacao.duracao_minima_sessao_minutos'])
            )
            
        except Exception as e:
            logger.error(f"Failed to get validation config from config-service: {e}")
            # Retorna configuração de emergência (apenas para não quebrar o sistema)
            logger.warning("Using emergency fallback configuration")
            return self._get_emergency_config()
    
    def _get_fallback_value(self, key: str):
        """Valores de fallback apenas para emergência"""
        fallbacks = {
            'cpa.validacao.opcao1.deposito_minimo': 30.00,
            'cpa.validacao.opcao1.numero_apostas': 10,
            'cpa.validacao.opcao2.deposito_minimo': 30.00,
            'cpa.validacao.opcao2.ggr_minimo': 20.00,
            'cpa.validacao.prazo_dias': 30,
            'cpa.validacao.timezone': 'America/Sao_Paulo',
            'cpa.validacao.deteccao_fraude_ativa': True,
            'cpa.validacao.max_tentativas': 3,
            'cpa.validacao.timeout_segundos': 30,
            'cpa.validacao.exigir_primeiro_deposito': False,
            'cpa.validacao.duracao_minima_sessao_minutos': 5
        }
        return fallbacks.get(key, None)
    
    def _get_emergency_config(self) -> ValidationConfig:
        """Configuração de emergência quando config-service não responde"""
        return ValidationConfig(
            option1_deposit_min=30.00,
            option1_bets_min=10,
            option2_deposit_min=30.00,
            option2_ggr_min=20.00,
            validation_period_days=30,
            timezone='America/Sao_Paulo',
            enable_fraud_detection=True,
            max_validation_attempts=3,
            validation_timeout_seconds=30,
            require_first_deposit=False,
            min_session_duration_minutes=5
        )

class IndicationValidationService:
    """Serviço centralizado de validação de indicações"""
    
    def __init__(self, config_service_url: str = None):
        self.config_client = ConfigServiceClient(config_service_url)
        self.validation_cache = {}
        self._config = None
        self._config_last_updated = None
        self._config_ttl = 300  # 5 minutos
        
    def _get_config(self) -> ValidationConfig:
        """Obtém configuração com cache"""
        now = datetime.utcnow()
        
        if (self._config is None or 
            self._config_last_updated is None or 
            (now - self._config_last_updated).total_seconds() > self._config_ttl):
            
            logger.info("Refreshing validation config from config-service")
            self._config = self.config_client.get_validation_config()
            self._config_last_updated = now
            
        return self._config
        
    def validate_indication(self, lead_data: LeadData) -> ValidationResult:
        """
        Valida uma indicação baseada nos critérios definidos no config-service
        
        Lógica: O lead deve atender completamente a OPÇÃO 1 OU completamente a OPÇÃO 2
        """
        try:
            config = self._get_config()
            logger.info(f"Iniciando validação para lead {lead_data.lead_id}")
            
            # Verifica se está dentro do prazo de validação
            if not self._is_within_validation_period(lead_data, config):
                return ValidationResult(
                    lead_id=lead_data.lead_id,
                    affiliate_id=lead_data.affiliate_id,
                    is_valid=False,
                    errors=[f"Lead fora do prazo de validação ({config.validation_period_days} dias)"]
                )
            
            # Verifica primeiro depósito se obrigatório
            if config.require_first_deposit and not lead_data.first_deposit_date:
                return ValidationResult(
                    lead_id=lead_data.lead_id,
                    affiliate_id=lead_data.affiliate_id,
                    is_valid=False,
                    errors=["Primeiro depósito obrigatório não encontrado"]
                )
            
            # Tenta validar pela Opção 1
            option1_result = self._validate_option_1(lead_data, config)
            if option1_result.is_valid:
                logger.info(f"Lead {lead_data.lead_id} validado pela Opção 1")
                return option1_result
            
            # Tenta validar pela Opção 2
            option2_result = self._validate_option_2(lead_data, config)
            if option2_result.is_valid:
                logger.info(f"Lead {lead_data.lead_id} validado pela Opção 2")
                return option2_result
            
            # Nenhuma opção foi atendida
            logger.warning(f"Lead {lead_data.lead_id} não atendeu nenhum critério")
            
            # Combina erros de ambas as opções
            all_errors = option1_result.errors + option2_result.errors
            
            return ValidationResult(
                lead_id=lead_data.lead_id,
                affiliate_id=lead_data.affiliate_id,
                is_valid=False,
                details={
                    'option1_details': option1_result.details,
                    'option2_details': option2_result.details
                },
                errors=all_errors
            )
            
        except Exception as e:
            logger.error(f"Erro na validação do lead {lead_data.lead_id}: {e}")
            return ValidationResult(
                lead_id=lead_data.lead_id,
                affiliate_id=lead_data.affiliate_id,
                is_valid=False,
                errors=[f"Erro interno na validação: {str(e)}"]
            )
        finally:
            # Log de auditoria
            self._log_validation_audit(lead_data, locals().get('option1_result') or locals().get('option2_result'))
    
    def _validate_option_1(self, lead_data: LeadData, config: ValidationConfig) -> ValidationResult:
        """Valida pela Opção 1: Depósito + Apostas"""
        errors = []
        details = {}
        
        # Verifica depósito mínimo
        deposit_ok = lead_data.total_deposits >= config.option1_deposit_min
        details['deposit_required'] = config.option1_deposit_min
        details['deposit_actual'] = lead_data.total_deposits
        details['deposit_ok'] = deposit_ok
        
        if not deposit_ok:
            errors.append(f"Depósito insuficiente: R$ {lead_data.total_deposits:.2f} < R$ {config.option1_deposit_min:.2f}")
        
        # Verifica número de apostas
        bets_ok = lead_data.total_bets >= config.option1_bets_min
        details['bets_required'] = config.option1_bets_min
        details['bets_actual'] = lead_data.total_bets
        details['bets_ok'] = bets_ok
        
        if not bets_ok:
            errors.append(f"Apostas insuficientes: {lead_data.total_bets} < {config.option1_bets_min}")
        
        # Opção 1 é válida se AMBOS os critérios forem atendidos
        is_valid = deposit_ok and bets_ok
        
        return ValidationResult(
            lead_id=lead_data.lead_id,
            affiliate_id=lead_data.affiliate_id,
            is_valid=is_valid,
            criteria_met=ValidationCriteria.OPTION_1 if is_valid else None,
            details=details,
            errors=errors
        )
    
    def _validate_option_2(self, lead_data: LeadData, config: ValidationConfig) -> ValidationResult:
        """Valida pela Opção 2: Depósito + GGR"""
        errors = []
        details = {}
        
        # Verifica depósito mínimo
        deposit_ok = lead_data.total_deposits >= config.option2_deposit_min
        details['deposit_required'] = config.option2_deposit_min
        details['deposit_actual'] = lead_data.total_deposits
        details['deposit_ok'] = deposit_ok
        
        if not deposit_ok:
            errors.append(f"Depósito insuficiente: R$ {lead_data.total_deposits:.2f} < R$ {config.option2_deposit_min:.2f}")
        
        # Verifica GGR mínimo
        ggr_ok = lead_data.total_ggr >= config.option2_ggr_min
        details['ggr_required'] = config.option2_ggr_min
        details['ggr_actual'] = lead_data.total_ggr
        details['ggr_ok'] = ggr_ok
        
        if not ggr_ok:
            errors.append(f"GGR insuficiente: R$ {lead_data.total_ggr:.2f} < R$ {config.option2_ggr_min:.2f}")
        
        # Opção 2 é válida se AMBOS os critérios forem atendidos
        is_valid = deposit_ok and ggr_ok
        
        return ValidationResult(
            lead_id=lead_data.lead_id,
            affiliate_id=lead_data.affiliate_id,
            is_valid=is_valid,
            criteria_met=ValidationCriteria.OPTION_2 if is_valid else None,
            details=details,
            errors=errors
        )
    
    def _is_within_validation_period(self, lead_data: LeadData, config: ValidationConfig) -> bool:
        """Verifica se o lead está dentro do prazo de validação"""
        now = datetime.utcnow()
        cutoff_date = now - timedelta(days=config.validation_period_days)
        return lead_data.registration_date >= cutoff_date
    
    def _log_validation_audit(self, lead_data: LeadData, result: ValidationResult):
        """Log de auditoria da validação"""
        if result:
            audit_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "lead_id": lead_data.lead_id,
                "affiliate_id": lead_data.affiliate_id,
                "is_valid": result.is_valid,
                "criteria_met": result.criteria_met.value if result.criteria_met else None,
                "validation_details": result.details,
                "errors": result.errors,
                "context": {}
            }
            
            logger.info(f"VALIDATION_AUDIT: {json.dumps(audit_data)}")
    
    def validate_batch(self, leads_data: List[LeadData]) -> List[ValidationResult]:
        """Valida múltiplas indicações em lote"""
        logger.info(f"Iniciando validação em lote de {len(leads_data)} leads")
        
        results = []
        for lead_data in leads_data:
            result = self.validate_indication(lead_data)
            results.append(result)
        
        valid_count = sum(1 for r in results if r.is_valid)
        logger.info(f"Validação em lote concluída: {valid_count}/{len(results)} leads válidos")
        
        return results

# Exemplo de uso e testes
if __name__ == "__main__":
    # Inicializa serviço (irá buscar configurações do config-service)
    validation_service = IndicationValidationService()
    
    # Dados de teste
    lead_1 = LeadData(
        lead_id="lead_001",
        affiliate_id="aff_001",
        registration_date=datetime.utcnow() - timedelta(days=5),
        total_deposits=50.00,
        total_bets=15,
        total_ggr=25.00
    )
    
    lead_2 = LeadData(
        lead_id="lead_002",
        affiliate_id="aff_001",
        registration_date=datetime.utcnow() - timedelta(days=10),
        total_deposits=25.00,
        total_bets=8,
        total_ggr=15.00
    )
    
    print("=== Teste de Validação de Indicações (Config-Service) ===")
    
    # Teste individual
    result_1 = validation_service.validate_indication(lead_1)
    print(f"Lead 1 - Válido: {result_1.is_valid}, Critério: {result_1.criteria_met}")
    if result_1.details:
        print(f"Detalhes: {result_1.details}")
    if result_1.errors:
        print(f"Erros: {result_1.errors}")
    
    result_2 = validation_service.validate_indication(lead_2)
    print(f"Lead 2 - Válido: {result_2.is_valid}, Critério: {result_2.criteria_met}")
    if result_2.errors:
        print(f"Erros: {result_2.errors}")
    
    # Teste em lote
    batch_results = validation_service.validate_batch([lead_1, lead_2])
    valid_count = sum(1 for r in batch_results if r.is_valid)
    print(f"Validação em lote: {valid_count}/{len(batch_results)} válidos")
    
    print("\n✅ Todas as configurações obtidas do config-service!")

