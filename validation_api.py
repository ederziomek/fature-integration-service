#!/usr/bin/env python3
"""
API REST para Validação de Indicações
Para integração com o integration-service do Sistema Fature
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import logging
import json
from typing import Dict, List

# Importa o módulo de validação
from indication_validation import (
    IndicationValidationService,
    ValidationConfig,
    ValidationConfigManager,
    ValidationAuditLogger,
    LeadData,
    ValidationResult
)

# Configuração da aplicação
app = Flask(__name__)
CORS(app)

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Instâncias globais
config_manager = ValidationConfigManager()
audit_logger = ValidationAuditLogger()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'indication-validation-service',
        'timestamp': datetime.utcnow().isoformat(),
        'version': '1.0.0'
    })

@app.route('/api/v1/validate', methods=['POST'])
def validate_single_indication():
    """Valida uma única indicação"""
    try:
        data = request.get_json()
        
        # Validação dos dados de entrada
        required_fields = ['lead_id', 'affiliate_id', 'registration_date', 
                          'total_deposits', 'total_bets', 'total_ggr']
        
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'success': False,
                    'error': f'Missing required field: {field}'
                }), 400
        
        # Converte data de registro
        try:
            registration_date = datetime.fromisoformat(data['registration_date'].replace('Z', '+00:00'))
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'Invalid registration_date format. Use ISO format.'
            }), 400
        
        # Cria objeto LeadData
        lead_data = LeadData(
            lead_id=data['lead_id'],
            affiliate_id=data['affiliate_id'],
            registration_date=registration_date,
            total_deposits=float(data['total_deposits']),
            total_bets=int(data['total_bets']),
            total_ggr=float(data['total_ggr']),
            first_deposit_date=datetime.fromisoformat(data['first_deposit_date'].replace('Z', '+00:00')) if data.get('first_deposit_date') else None,
            last_activity_date=datetime.fromisoformat(data['last_activity_date'].replace('Z', '+00:00')) if data.get('last_activity_date') else None
        )
        
        # Obtém configuração e valida
        config = config_manager.get_validation_config()
        validation_service = IndicationValidationService(config)
        
        result = validation_service.validate_indication(lead_data)
        
        # Log de auditoria
        audit_logger.log_validation(result, {
            'request_ip': request.remote_addr,
            'user_agent': request.headers.get('User-Agent'),
            'endpoint': '/api/v1/validate'
        })
        
        # Resposta
        response_data = {
            'success': True,
            'data': {
                'lead_id': result.lead_id,
                'affiliate_id': result.affiliate_id,
                'is_valid': result.is_valid,
                'criteria_met': result.criteria_met.value if result.criteria_met else None,
                'validation_date': result.validation_date.isoformat(),
                'details': result.details,
                'errors': result.errors
            }
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Erro na validação: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/validate/batch', methods=['POST'])
def validate_batch_indications():
    """Valida múltiplas indicações em lote"""
    try:
        data = request.get_json()
        
        if 'leads' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing leads array'
            }), 400
        
        leads_data = []
        errors = []
        
        # Processa cada lead
        for i, lead_info in enumerate(data['leads']):
            try:
                # Validação dos campos obrigatórios
                required_fields = ['lead_id', 'affiliate_id', 'registration_date', 
                                 'total_deposits', 'total_bets', 'total_ggr']
                
                for field in required_fields:
                    if field not in lead_info:
                        errors.append(f'Lead {i}: Missing field {field}')
                        continue
                
                # Converte data
                registration_date = datetime.fromisoformat(lead_info['registration_date'].replace('Z', '+00:00'))
                
                # Cria LeadData
                lead_data = LeadData(
                    lead_id=lead_info['lead_id'],
                    affiliate_id=lead_info['affiliate_id'],
                    registration_date=registration_date,
                    total_deposits=float(lead_info['total_deposits']),
                    total_bets=int(lead_info['total_bets']),
                    total_ggr=float(lead_info['total_ggr'])
                )
                
                leads_data.append(lead_data)
                
            except Exception as e:
                errors.append(f'Lead {i}: {str(e)}')
        
        if errors:
            return jsonify({
                'success': False,
                'errors': errors
            }), 400
        
        # Valida em lote
        config = config_manager.get_validation_config()
        validation_service = IndicationValidationService(config)
        
        results = validation_service.validate_batch(leads_data)
        
        # Log de auditoria para cada resultado
        for result in results:
            audit_logger.log_validation(result, {
                'request_ip': request.remote_addr,
                'user_agent': request.headers.get('User-Agent'),
                'endpoint': '/api/v1/validate/batch',
                'batch_size': len(results)
            })
        
        # Prepara resposta
        response_data = {
            'success': True,
            'data': {
                'total_processed': len(results),
                'valid_count': sum(1 for r in results if r.is_valid),
                'invalid_count': sum(1 for r in results if not r.is_valid),
                'results': [
                    {
                        'lead_id': r.lead_id,
                        'affiliate_id': r.affiliate_id,
                        'is_valid': r.is_valid,
                        'criteria_met': r.criteria_met.value if r.criteria_met else None,
                        'validation_date': r.validation_date.isoformat(),
                        'details': r.details,
                        'errors': r.errors
                    }
                    for r in results
                ]
            }
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Erro na validação em lote: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/config', methods=['GET'])
def get_validation_config():
    """Retorna a configuração atual de validação"""
    try:
        config = config_manager.get_validation_config()
        
        return jsonify({
            'success': True,
            'data': {
                'option1_deposit_min': config.option1_deposit_min,
                'option1_bets_min': config.option1_bets_min,
                'option2_deposit_min': config.option2_deposit_min,
                'option2_ggr_min': config.option2_ggr_min,
                'validation_period_days': config.validation_period_days,
                'timezone': config.timezone
            }
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar configuração: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/stats', methods=['GET'])
def get_validation_statistics():
    """Retorna estatísticas de validação"""
    try:
        affiliate_id = request.args.get('affiliate_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Converte datas se fornecidas
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00')) if start_date else None
        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00')) if end_date else None
        
        config = config_manager.get_validation_config()
        validation_service = IndicationValidationService(config)
        
        stats = validation_service.get_validation_statistics(
            affiliate_id=affiliate_id,
            start_date=start_dt,
            end_date=end_dt
        )
        
        return jsonify({
            'success': True,
            'data': stats
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/revalidate', methods=['POST'])
def revalidate_pending():
    """Revalida indicações pendentes"""
    try:
        config = config_manager.get_validation_config()
        validation_service = IndicationValidationService(config)
        
        results = validation_service.revalidate_pending_indications()
        
        return jsonify({
            'success': True,
            'data': {
                'revalidated_count': len(results),
                'valid_count': sum(1 for r in results if r.is_valid),
                'invalid_count': sum(1 for r in results if not r.is_valid)
            }
        })
        
    except Exception as e:
        logger.error(f"Erro na revalidação: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/test', methods=['POST'])
def test_validation():
    """Endpoint de teste para validação"""
    try:
        # Dados de teste
        test_data = {
            'lead_id': 'test_lead_001',
            'affiliate_id': 'test_aff_001',
            'registration_date': datetime.utcnow().isoformat(),
            'total_deposits': 50.00,
            'total_bets': 15,
            'total_ggr': 25.00
        }
        
        # Sobrescreve com dados do request se fornecidos
        if request.get_json():
            test_data.update(request.get_json())
        
        # Cria LeadData
        lead_data = LeadData(
            lead_id=test_data['lead_id'],
            affiliate_id=test_data['affiliate_id'],
            registration_date=datetime.fromisoformat(test_data['registration_date'].replace('Z', '+00:00')),
            total_deposits=float(test_data['total_deposits']),
            total_bets=int(test_data['total_bets']),
            total_ggr=float(test_data['total_ggr'])
        )
        
        # Valida
        config = config_manager.get_validation_config()
        validation_service = IndicationValidationService(config)
        
        result = validation_service.validate_indication(lead_data)
        
        return jsonify({
            'success': True,
            'test_data': test_data,
            'validation_result': {
                'is_valid': result.is_valid,
                'criteria_met': result.criteria_met.value if result.criteria_met else None,
                'details': result.details,
                'errors': result.errors
            }
        })
        
    except Exception as e:
        logger.error(f"Erro no teste: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Endpoint not found'
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        'success': False,
        'error': 'Method not allowed'
    }), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'error': 'Internal server error'
    }), 500

if __name__ == '__main__':
    logger.info("Iniciando Indication Validation API...")
    app.run(host='0.0.0.0', port=5000, debug=True)

