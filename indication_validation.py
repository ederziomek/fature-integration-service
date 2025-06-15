#!/usr/bin/env python3
"""
Módulo de Validação de Indicações Centralizada
Para ser integrado ao integration-service do Sistema Fature
"""
import logging
import json
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
    option1_deposit_min: float = 30.00
    option1_bets_min: int = 10
    
    # Opção 2: Depósito + GGR
    option2_deposit_min: float = 30.00
    option2_ggr_min: float = 20.00
    
    # Configurações gerais
    validation_period_days: int = 30
    timezone: str = "America/Sao_Paulo"

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

class IndicationValidationService:
    """Serviço centralizado de validação de indicações"""
    
    def __init__(self, config: ValidationConfig, db_connection=None):
        self.config = config
        self.db_connection = db_connection
        self.validation_cache = {}
        
    def validate_indication(self, lead_data: LeadData) -> ValidationResult:
        """
        Valida uma indicação baseada nos critérios definidos
        
        Lógica: O lead deve atender completamente a OPÇÃO 1 OU completamente a OPÇÃO 2
        - Opção 1: Depósito >= R$ 30,00 E Apostas >= 10
        - Opção 2: Depósito >= R$ 30,00 E GGR >= R$ 20,00
        """
        try:
            logger.info(f"Iniciando validação para lead {lead_data.lead_id}")
            
            # Verifica se está dentro do prazo de validação
            if not self._is_within_validation_period(lead_data):
                return ValidationResult(
                    lead_id=lead_data.lead_id,
                    affiliate_id=lead_data.affiliate_id,
                    is_valid=False,
                    errors=["Lead fora do prazo de validação (30 dias)"]
                )
            
            # Tenta validar pela Opção 1
            option1_result = self._validate_option_1(lead_data)
            if option1_result.is_valid:
                logger.info(f"Lead {lead_data.lead_id} validado pela Opção 1")
                return option1_result
            
            # Tenta validar pela Opção 2
            option2_result = self._validate_option_2(lead_data)
            if option2_result.is_valid:
                logger.info(f"Lead {lead_data.lead_id} validado pela Opção 2")
                return option2_result
            
            # Nenhuma opção foi atendida
            logger.warning(f"Lead {lead_data.lead_id} não atendeu nenhum critério")
            return ValidationResult(
                lead_id=lead_data.lead_id,
                affiliate_id=lead_data.affiliate_id,
                is_valid=False,
                details={
                    "option1_details": option1_result.details,
                    "option2_details": option2_result.details
                },
                errors=option1_result.errors + option2_result.errors
            )
            
        except Exception as e:
            logger.error(f"Erro na validação do lead {lead_data.lead_id}: {str(e)}")
            return ValidationResult(
                lead_id=lead_data.lead_id,
                affiliate_id=lead_data.affiliate_id,
                is_valid=False,
                errors=[f"Erro interno: {str(e)}"]
            )
    
    def _validate_option_1(self, lead_data: LeadData) -> ValidationResult:
        """Valida pela Opção 1: Depósito >= R$ 30,00 E Apostas >= 10"""
        
        deposit_ok = lead_data.total_deposits >= self.config.option1_deposit_min
        bets_ok = lead_data.total_bets >= self.config.option1_bets_min
        
        is_valid = deposit_ok and bets_ok
        
        details = {
            "deposit_required": self.config.option1_deposit_min,
            "deposit_actual": lead_data.total_deposits,
            "deposit_ok": deposit_ok,
            "bets_required": self.config.option1_bets_min,
            "bets_actual": lead_data.total_bets,
            "bets_ok": bets_ok
        }
        
        errors = []
        if not deposit_ok:
            errors.append(f"Depósito insuficiente: R$ {lead_data.total_deposits:.2f} < R$ {self.config.option1_deposit_min:.2f}")
        if not bets_ok:
            errors.append(f"Apostas insuficientes: {lead_data.total_bets} < {self.config.option1_bets_min}")
        
        return ValidationResult(
            lead_id=lead_data.lead_id,
            affiliate_id=lead_data.affiliate_id,
            is_valid=is_valid,
            criteria_met=ValidationCriteria.OPTION_1 if is_valid else None,
            details=details,
            errors=errors
        )
    
    def _validate_option_2(self, lead_data: LeadData) -> ValidationResult:
        """Valida pela Opção 2: Depósito >= R$ 30,00 E GGR >= R$ 20,00"""
        
        deposit_ok = lead_data.total_deposits >= self.config.option2_deposit_min
        ggr_ok = lead_data.total_ggr >= self.config.option2_ggr_min
        
        is_valid = deposit_ok and ggr_ok
        
        details = {
            "deposit_required": self.config.option2_deposit_min,
            "deposit_actual": lead_data.total_deposits,
            "deposit_ok": deposit_ok,
            "ggr_required": self.config.option2_ggr_min,
            "ggr_actual": lead_data.total_ggr,
            "ggr_ok": ggr_ok
        }
        
        errors = []
        if not deposit_ok:
            errors.append(f"Depósito insuficiente: R$ {lead_data.total_deposits:.2f} < R$ {self.config.option2_deposit_min:.2f}")
        if not ggr_ok:
            errors.append(f"GGR insuficiente: R$ {lead_data.total_ggr:.2f} < R$ {self.config.option2_ggr_min:.2f}")
        
        return ValidationResult(
            lead_id=lead_data.lead_id,
            affiliate_id=lead_data.affiliate_id,
            is_valid=is_valid,
            criteria_met=ValidationCriteria.OPTION_2 if is_valid else None,
            details=details,
            errors=errors
        )
    
    def _is_within_validation_period(self, lead_data: LeadData) -> bool:
        """Verifica se o lead está dentro do prazo de validação"""
        cutoff_date = datetime.utcnow() - timedelta(days=self.config.validation_period_days)
        return lead_data.registration_date >= cutoff_date
    
    def validate_batch(self, leads: List[LeadData]) -> List[ValidationResult]:
        """Valida múltiplos leads em lote"""
        results = []
        
        logger.info(f"Iniciando validação em lote de {len(leads)} leads")
        
        for lead in leads:
            result = self.validate_indication(lead)
            results.append(result)
        
        valid_count = sum(1 for r in results if r.is_valid)
        logger.info(f"Validação em lote concluída: {valid_count}/{len(leads)} leads válidos")
        
        return results
    
    def get_validation_statistics(self, affiliate_id: Optional[str] = None, 
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None) -> Dict:
        """Retorna estatísticas de validação"""
        # Esta função seria implementada com consultas ao banco de dados
        # Por enquanto, retorna estrutura de exemplo
        
        stats = {
            "total_validations": 0,
            "valid_indications": 0,
            "invalid_indications": 0,
            "validation_rate": 0.0,
            "criteria_breakdown": {
                "option_1": 0,
                "option_2": 0
            },
            "common_rejection_reasons": []
        }
        
        return stats
    
    def revalidate_pending_indications(self) -> List[ValidationResult]:
        """Revalida indicações pendentes (para execução periódica)"""
        # Esta função seria implementada para buscar leads pendentes no banco
        # e revalidá-los conforme necessário
        
        logger.info("Iniciando revalidação de indicações pendentes")
        
        # Implementação seria algo como:
        # pending_leads = self._get_pending_leads_from_db()
        # return self.validate_batch(pending_leads)
        
        return []

class ValidationAuditLogger:
    """Logger de auditoria para validações"""
    
    def __init__(self, db_connection=None):
        self.db_connection = db_connection
    
    def log_validation(self, result: ValidationResult, additional_context: Dict = None):
        """Registra uma validação para auditoria"""
        
        audit_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "lead_id": result.lead_id,
            "affiliate_id": result.affiliate_id,
            "is_valid": result.is_valid,
            "criteria_met": result.criteria_met.value if result.criteria_met else None,
            "validation_details": result.details,
            "errors": result.errors,
            "context": additional_context or {}
        }
        
        # Log estruturado
        logger.info(f"VALIDATION_AUDIT: {json.dumps(audit_record)}")
        
        # Persistir no banco de dados se disponível
        if self.db_connection:
            self._persist_audit_record(audit_record)
    
    def _persist_audit_record(self, record: Dict):
        """Persiste registro de auditoria no banco"""
        # Implementação específica do banco de dados
        pass

class ValidationConfigManager:
    """Gerenciador de configurações de validação"""
    
    def __init__(self, config_service_url: str = None):
        self.config_service_url = config_service_url or "http://config-service.fature.svc.cluster.local"
        self._cached_config = None
        self._cache_timestamp = None
        self._cache_ttl = 300  # 5 minutos
    
    def get_validation_config(self) -> ValidationConfig:
        """Obtém configuração de validação do config-service"""
        
        # Verifica cache
        if self._is_cache_valid():
            return self._cached_config
        
        try:
            # Busca configurações do config-service
            config_data = self._fetch_from_config_service()
            
            # Cria objeto de configuração
            validation_config = ValidationConfig(
                option1_deposit_min=config_data.get("cpa.validacao.opcao1.deposito_minimo", 30.00),
                option1_bets_min=config_data.get("cpa.validacao.opcao1.numero_apostas", 10),
                option2_deposit_min=config_data.get("cpa.validacao.opcao2.deposito_minimo", 30.00),
                option2_ggr_min=config_data.get("cpa.validacao.opcao2.ggr_minimo", 20.00),
                validation_period_days=config_data.get("cpa.validacao.prazo_dias", 30)
            )
            
            # Atualiza cache
            self._cached_config = validation_config
            self._cache_timestamp = datetime.utcnow()
            
            return validation_config
            
        except Exception as e:
            logger.error(f"Erro ao buscar configurações: {e}")
            
            # Retorna configuração padrão em caso de erro
            return ValidationConfig()
    
    def _is_cache_valid(self) -> bool:
        """Verifica se o cache ainda é válido"""
        if not self._cached_config or not self._cache_timestamp:
            return False
        
        age = (datetime.utcnow() - self._cache_timestamp).total_seconds()
        return age < self._cache_ttl
    
    def _fetch_from_config_service(self) -> Dict:
        """Busca configurações do config-service"""
        import requests
        
        try:
            # Busca configurações de validação CPA
            response = requests.get(
                f"{self.config_service_url}/api/v1/configurations",
                params={"category": "validacao_cpa"},
                timeout=10
            )
            response.raise_for_status()
            
            configs = response.json().get("data", [])
            
            # Converte lista de configurações em dicionário
            config_dict = {}
            for config in configs:
                config_dict[config["key"]] = config["value"]
            
            return config_dict
            
        except Exception as e:
            logger.error(f"Erro ao buscar do config-service: {e}")
            raise

# Exemplo de uso e testes
if __name__ == "__main__":
    # Configuração de exemplo
    config = ValidationConfig()
    
    # Serviço de validação
    validation_service = IndicationValidationService(config)
    
    # Logger de auditoria
    audit_logger = ValidationAuditLogger()
    
    # Dados de teste
    test_lead_1 = LeadData(
        lead_id="lead_001",
        affiliate_id="aff_001",
        registration_date=datetime.utcnow() - timedelta(days=5),
        total_deposits=50.00,
        total_bets=15,
        total_ggr=25.00
    )
    
    test_lead_2 = LeadData(
        lead_id="lead_002",
        affiliate_id="aff_001",
        registration_date=datetime.utcnow() - timedelta(days=10),
        total_deposits=25.00,
        total_bets=8,
        total_ggr=15.00
    )
    
    # Testes de validação
    print("=== Teste de Validação de Indicações ===")
    
    result_1 = validation_service.validate_indication(test_lead_1)
    print(f"Lead 1 - Válido: {result_1.is_valid}, Critério: {result_1.criteria_met}")
    print(f"Detalhes: {result_1.details}")
    
    result_2 = validation_service.validate_indication(test_lead_2)
    print(f"Lead 2 - Válido: {result_2.is_valid}, Critério: {result_2.criteria_met}")
    print(f"Erros: {result_2.errors}")
    
    # Log de auditoria
    audit_logger.log_validation(result_1)
    audit_logger.log_validation(result_2)
    
    # Teste em lote
    batch_results = validation_service.validate_batch([test_lead_1, test_lead_2])
    print(f"\nValidação em lote: {len([r for r in batch_results if r.is_valid])}/2 válidos")

