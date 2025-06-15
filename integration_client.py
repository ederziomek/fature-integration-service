#!/usr/bin/env python3
"""
Cliente de integração para o Serviço de Validação de Indicações
Para ser usado pelo integration-service do Sistema Fature
"""
import requests
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ValidationRequest:
    """Dados para requisição de validação"""
    lead_id: str
    affiliate_id: str
    registration_date: datetime
    total_deposits: float
    total_bets: int
    total_ggr: float
    first_deposit_date: Optional[datetime] = None
    last_activity_date: Optional[datetime] = None

@dataclass
class ValidationResponse:
    """Resposta da validação"""
    lead_id: str
    affiliate_id: str
    is_valid: bool
    criteria_met: Optional[str] = None
    validation_date: Optional[datetime] = None
    details: Optional[Dict] = None
    errors: Optional[List[str]] = None

class IndicationValidationClient:
    """Cliente para comunicação com o serviço de validação"""
    
    def __init__(self, service_url: str = None, timeout: int = 30):
        self.service_url = service_url or "http://indication-validation-service.fature.svc.cluster.local"
        self.timeout = timeout
        self.session = requests.Session()
        
        # Headers padrão
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'integration-service/1.0.0'
        })
    
    def validate_single(self, request: ValidationRequest) -> ValidationResponse:
        """Valida uma única indicação"""
        try:
            # Prepara dados da requisição
            data = {
                'lead_id': request.lead_id,
                'affiliate_id': request.affiliate_id,
                'registration_date': request.registration_date.isoformat(),
                'total_deposits': request.total_deposits,
                'total_bets': request.total_bets,
                'total_ggr': request.total_ggr
            }
            
            if request.first_deposit_date:
                data['first_deposit_date'] = request.first_deposit_date.isoformat()
            
            if request.last_activity_date:
                data['last_activity_date'] = request.last_activity_date.isoformat()
            
            # Faz requisição
            response = self.session.post(
                f"{self.service_url}/api/v1/validate",
                json=data,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Processa resposta
            result = response.json()
            
            if not result.get('success'):
                raise Exception(f"Validation failed: {result.get('error')}")
            
            data = result['data']
            
            return ValidationResponse(
                lead_id=data['lead_id'],
                affiliate_id=data['affiliate_id'],
                is_valid=data['is_valid'],
                criteria_met=data.get('criteria_met'),
                validation_date=datetime.fromisoformat(data['validation_date'].replace('Z', '+00:00')),
                details=data.get('details'),
                errors=data.get('errors')
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error validating {request.lead_id}: {e}")
            raise Exception(f"Network error: {e}")
        
        except Exception as e:
            logger.error(f"Error validating {request.lead_id}: {e}")
            raise
    
    def validate_batch(self, requests: List[ValidationRequest]) -> List[ValidationResponse]:
        """Valida múltiplas indicações em lote"""
        try:
            # Prepara dados das requisições
            leads_data = []
            for req in requests:
                lead_data = {
                    'lead_id': req.lead_id,
                    'affiliate_id': req.affiliate_id,
                    'registration_date': req.registration_date.isoformat(),
                    'total_deposits': req.total_deposits,
                    'total_bets': req.total_bets,
                    'total_ggr': req.total_ggr
                }
                
                if req.first_deposit_date:
                    lead_data['first_deposit_date'] = req.first_deposit_date.isoformat()
                
                if req.last_activity_date:
                    lead_data['last_activity_date'] = req.last_activity_date.isoformat()
                
                leads_data.append(lead_data)
            
            # Faz requisição
            response = self.session.post(
                f"{self.service_url}/api/v1/validate/batch",
                json={'leads': leads_data},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Processa resposta
            result = response.json()
            
            if not result.get('success'):
                raise Exception(f"Batch validation failed: {result.get('error')}")
            
            data = result['data']
            responses = []
            
            for item in data['results']:
                responses.append(ValidationResponse(
                    lead_id=item['lead_id'],
                    affiliate_id=item['affiliate_id'],
                    is_valid=item['is_valid'],
                    criteria_met=item.get('criteria_met'),
                    validation_date=datetime.fromisoformat(item['validation_date'].replace('Z', '+00:00')),
                    details=item.get('details'),
                    errors=item.get('errors')
                ))
            
            return responses
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error in batch validation: {e}")
            raise Exception(f"Network error: {e}")
        
        except Exception as e:
            logger.error(f"Error in batch validation: {e}")
            raise
    
    def get_validation_config(self) -> Dict:
        """Obtém configuração atual de validação"""
        try:
            response = self.session.get(
                f"{self.service_url}/api/v1/config",
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            
            if not result.get('success'):
                raise Exception(f"Failed to get config: {result.get('error')}")
            
            return result['data']
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error getting config: {e}")
            raise Exception(f"Network error: {e}")
        
        except Exception as e:
            logger.error(f"Error getting config: {e}")
            raise
    
    def get_statistics(self, affiliate_id: str = None, 
                      start_date: datetime = None, 
                      end_date: datetime = None) -> Dict:
        """Obtém estatísticas de validação"""
        try:
            params = {}
            
            if affiliate_id:
                params['affiliate_id'] = affiliate_id
            
            if start_date:
                params['start_date'] = start_date.isoformat()
            
            if end_date:
                params['end_date'] = end_date.isoformat()
            
            response = self.session.get(
                f"{self.service_url}/api/v1/stats",
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            
            if not result.get('success'):
                raise Exception(f"Failed to get stats: {result.get('error')}")
            
            return result['data']
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error getting stats: {e}")
            raise Exception(f"Network error: {e}")
        
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            raise
    
    def health_check(self) -> bool:
        """Verifica se o serviço está saudável"""
        try:
            response = self.session.get(
                f"{self.service_url}/health",
                timeout=5
            )
            response.raise_for_status()
            
            result = response.json()
            return result.get('status') == 'healthy'
            
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

class IntegrationServiceValidator:
    """Classe principal para integração com o integration-service"""
    
    def __init__(self, validation_client: IndicationValidationClient = None):
        self.client = validation_client or IndicationValidationClient()
    
    def process_new_lead(self, lead_data: Dict) -> Tuple[bool, Dict]:
        """
        Processa um novo lead e valida a indicação
        
        Args:
            lead_data: Dados do lead do banco externo
            
        Returns:
            Tuple[bool, Dict]: (is_valid, validation_details)
        """
        try:
            # Converte dados do lead para formato de validação
            request = ValidationRequest(
                lead_id=lead_data['id'],
                affiliate_id=lead_data['affiliate_id'],
                registration_date=datetime.fromisoformat(lead_data['registration_date']),
                total_deposits=float(lead_data.get('total_deposits', 0)),
                total_bets=int(lead_data.get('total_bets', 0)),
                total_ggr=float(lead_data.get('total_ggr', 0)),
                first_deposit_date=datetime.fromisoformat(lead_data['first_deposit_date']) if lead_data.get('first_deposit_date') else None,
                last_activity_date=datetime.fromisoformat(lead_data['last_activity_date']) if lead_data.get('last_activity_date') else None
            )
            
            # Valida
            response = self.client.validate_single(request)
            
            # Retorna resultado
            return response.is_valid, {
                'criteria_met': response.criteria_met,
                'validation_date': response.validation_date.isoformat() if response.validation_date else None,
                'details': response.details,
                'errors': response.errors
            }
            
        except Exception as e:
            logger.error(f"Error processing lead {lead_data.get('id')}: {e}")
            return False, {'errors': [str(e)]}
    
    def process_lead_batch(self, leads_data: List[Dict]) -> List[Tuple[str, bool, Dict]]:
        """
        Processa múltiplos leads em lote
        
        Args:
            leads_data: Lista de dados de leads
            
        Returns:
            List[Tuple[str, bool, Dict]]: Lista de (lead_id, is_valid, details)
        """
        try:
            # Converte dados para formato de validação
            requests = []
            for lead_data in leads_data:
                request = ValidationRequest(
                    lead_id=lead_data['id'],
                    affiliate_id=lead_data['affiliate_id'],
                    registration_date=datetime.fromisoformat(lead_data['registration_date']),
                    total_deposits=float(lead_data.get('total_deposits', 0)),
                    total_bets=int(lead_data.get('total_bets', 0)),
                    total_ggr=float(lead_data.get('total_ggr', 0))
                )
                requests.append(request)
            
            # Valida em lote
            responses = self.client.validate_batch(requests)
            
            # Processa resultados
            results = []
            for response in responses:
                details = {
                    'criteria_met': response.criteria_met,
                    'validation_date': response.validation_date.isoformat() if response.validation_date else None,
                    'details': response.details,
                    'errors': response.errors
                }
                results.append((response.lead_id, response.is_valid, details))
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing lead batch: {e}")
            # Retorna erro para todos os leads
            return [(lead['id'], False, {'errors': [str(e)]}) for lead in leads_data]
    
    def update_commission_status(self, lead_id: str, affiliate_id: str, is_valid: bool, details: Dict):
        """
        Atualiza status de comissão baseado na validação
        
        Esta função seria integrada com o commission-service
        """
        try:
            # Aqui seria feita a integração com o commission-service
            # para atualizar o status da comissão baseado na validação
            
            commission_data = {
                'lead_id': lead_id,
                'affiliate_id': affiliate_id,
                'status': 'approved' if is_valid else 'rejected',
                'validation_details': details,
                'updated_at': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Commission status updated for lead {lead_id}: {commission_data}")
            
            # TODO: Implementar chamada para commission-service
            # commission_service.update_commission_status(commission_data)
            
        except Exception as e:
            logger.error(f"Error updating commission status for lead {lead_id}: {e}")

# Exemplo de uso
if __name__ == "__main__":
    # Cliente de validação
    client = IndicationValidationClient()
    
    # Integrador principal
    integrator = IntegrationServiceValidator(client)
    
    # Teste de health check
    if client.health_check():
        print("✅ Validation service is healthy")
    else:
        print("❌ Validation service is not healthy")
    
    # Dados de teste
    test_lead = {
        'id': 'lead_test_001',
        'affiliate_id': 'aff_test_001',
        'registration_date': datetime.utcnow().isoformat(),
        'total_deposits': 50.00,
        'total_bets': 15,
        'total_ggr': 25.00
    }
    
    # Teste de validação
    is_valid, details = integrator.process_new_lead(test_lead)
    print(f"Lead validation result: {is_valid}")
    print(f"Details: {details}")
    
    # Teste de configuração
    try:
        config = client.get_validation_config()
        print(f"Current validation config: {config}")
    except Exception as e:
        print(f"Error getting config: {e}")

