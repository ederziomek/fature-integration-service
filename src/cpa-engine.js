const axios = require('axios');
const Redis = require('redis');
const promClient = require('prom-client');

// Métricas Prometheus
const cpaValidationCounter = new promClient.Counter({
    name: 'cpa_validations_total',
    help: 'Total number of CPA validations',
    labelNames: ['result', 'option', 'affiliate_id']
});

const cpaValidationDuration = new promClient.Histogram({
    name: 'cpa_validation_duration_seconds',
    help: 'Duration of CPA validations in seconds',
    labelNames: ['option'],
    buckets: [0.1, 0.5, 1, 2, 5, 10]
});

const configCacheHits = new promClient.Counter({
    name: 'config_cache_hits_total',
    help: 'Total number of config cache hits',
    labelNames: ['key', 'hit_type']
});

class CPARulesEngine {
    constructor() {
        this.configServiceUrl = process.env.CONFIG_SERVICE_URL || 
            'http://config-service.fature.svc.cluster.local:5000';
        this.redisUrl = process.env.REDIS_URL || 
            'redis://redis.fature.svc.cluster.local:6379';
        this.redisClient = null;
        this.cache = new Map();
        this.cacheTTL = parseInt(process.env.CACHE_TTL) || 300000; // 5 minutos
        this.initialized = false;
    }

    async initialize() {
        try {
            // Inicializar Redis
            this.redisClient = Redis.createClient({
                url: this.redisUrl,
                retry_strategy: (options) => {
                    if (options.error && options.error.code === 'ECONNREFUSED') {
                        console.error('Redis connection refused');
                        return new Error('Redis connection refused');
                    }
                    if (options.total_retry_time > 1000 * 60 * 60) {
                        return new Error('Retry time exhausted');
                    }
                    if (options.attempt > 10) {
                        return undefined;
                    }
                    return Math.min(options.attempt * 100, 3000);
                }
            });

            this.redisClient.on('error', (err) => {
                console.error('Redis Client Error:', err);
            });

            this.redisClient.on('connect', () => {
                console.log('Redis Client Connected');
            });

            await this.redisClient.connect();
            this.initialized = true;
            console.log('✓ CPA Rules Engine inicializado com sucesso');
            
        } catch (error) {
            console.error('Erro ao inicializar CPA Rules Engine:', error);
            // Continuar sem Redis se necessário
            this.initialized = true;
        }
    }

    async getConfiguration(key) {
        const startTime = Date.now();
        
        try {
            // Verificar cache local primeiro
            const localCached = this.cache.get(key);
            if (localCached && Date.now() - localCached.timestamp < this.cacheTTL) {
                configCacheHits.inc({ key, hit_type: 'local' });
                return localCached.value;
            }

            // Verificar cache Redis
            if (this.redisClient && this.redisClient.isOpen) {
                try {
                    const redisCached = await this.redisClient.get(`config_cache:${key}`);
                    if (redisCached) {
                        let value;
                        try {
                            value = JSON.parse(redisCached);
                        } catch {
                            value = redisCached;
                        }
                        
                        // Atualizar cache local
                        this.cache.set(key, {
                            value: value,
                            timestamp: Date.now()
                        });
                        
                        configCacheHits.inc({ key, hit_type: 'redis' });
                        return value;
                    }
                } catch (redisError) {
                    console.warn(`Redis cache miss for ${key}:`, redisError.message);
                }
            }

            // Buscar do config-service
            const response = await axios.get(
                `${this.configServiceUrl}/api/v1/configurations/${key}`,
                { timeout: 5000 }
            );
            
            if (response.data.success) {
                const config = response.data.data;
                let value = config.value;
                
                // Converter tipo de dados
                switch (config.data_type) {
                    case 'float':
                        value = parseFloat(value);
                        break;
                    case 'int':
                        value = parseInt(value);
                        break;
                    case 'bool':
                        value = value.toLowerCase() === 'true';
                        break;
                    case 'json':
                        value = JSON.parse(value);
                        break;
                }
                
                // Armazenar nos caches
                this.cache.set(key, {
                    value: value,
                    timestamp: Date.now()
                });

                if (this.redisClient && this.redisClient.isOpen) {
                    try {
                        const cacheValue = typeof value === 'object' ? JSON.stringify(value) : String(value);
                        await this.redisClient.setEx(`config_cache:${key}`, 3600, cacheValue);
                    } catch (redisError) {
                        console.warn(`Failed to cache ${key} in Redis:`, redisError.message);
                    }
                }
                
                configCacheHits.inc({ key, hit_type: 'config_service' });
                return value;
            }
        } catch (error) {
            console.error(`Erro ao buscar configuração ${key}:`, error.message);
            configCacheHits.inc({ key, hit_type: 'error' });
        }
        
        return null;
    }

    async validateCPA(request) {
        const startTime = Date.now();
        const { 
            affiliateId, 
            userId, 
            depositAmount, 
            betCount, 
            ggrAmount, 
            registrationDate, 
            validationOption = 'opcao1' 
        } = request;

        const results = [];
        const rulesApplied = [];
        const validationId = `${affiliateId}_${userId}_${Date.now()}`;

        try {
            console.log(`Iniciando validação CPA ${validationId} para afiliado ${affiliateId}`);

            // Buscar configurações necessárias
            const configKeys = [
                `cpa.validacao.${validationOption}.deposito_minimo`,
                `cpa.validacao.${validationOption}.numero_apostas`,
                `cpa.validacao.${validationOption}.ggr_minimo`,
                'cpa.validacao.prazo_dias',
                'cpa.validacao.timezone',
                'cpa.validacao.deteccao_fraude_ativa'
            ];

            const configs = {};
            for (const key of configKeys) {
                configs[key] = await this.getConfiguration(key);
            }

            console.log(`Configurações carregadas para ${validationId}:`, configs);

            // Validação 1: Depósito mínimo
            const minDeposit = configs[`cpa.validacao.${validationOption}.deposito_minimo`];
            if (minDeposit !== null) {
                const valid = depositAmount >= minDeposit;
                results.push(valid);
                const message = `Depósito ${depositAmount} ${valid ? '>=' : '<'} ${minDeposit} (${valid ? 'APROVADO' : 'REJEITADO'})`;
                rulesApplied.push(message);
                console.log(`${validationId} - Validação depósito: ${message}`);
            }

            // Validação 2: Número de apostas
            const minBets = configs[`cpa.validacao.${validationOption}.numero_apostas`];
            if (minBets !== null) {
                const valid = betCount >= minBets;
                results.push(valid);
                const message = `Apostas ${betCount} ${valid ? '>=' : '<'} ${minBets} (${valid ? 'APROVADO' : 'REJEITADO'})`;
                rulesApplied.push(message);
                console.log(`${validationId} - Validação apostas: ${message}`);
            }

            // Validação 3: GGR mínimo (se configurado)
            const minGGR = configs[`cpa.validacao.${validationOption}.ggr_minimo`];
            if (minGGR !== null) {
                const valid = ggrAmount >= minGGR;
                results.push(valid);
                const message = `GGR ${ggrAmount} ${valid ? '>=' : '<'} ${minGGR} (${valid ? 'APROVADO' : 'REJEITADO'})`;
                rulesApplied.push(message);
                console.log(`${validationId} - Validação GGR: ${message}`);
            }

            // Validação 4: Prazo
            const maxDays = configs['cpa.validacao.prazo_dias'];
            if (maxDays !== null) {
                const registrationTime = new Date(registrationDate).getTime();
                const currentTime = Date.now();
                const daysDiff = Math.floor((currentTime - registrationTime) / (1000 * 60 * 60 * 24));
                const valid = daysDiff <= maxDays;
                results.push(valid);
                const message = `Prazo ${daysDiff} dias ${valid ? '<=' : '>'} ${maxDays} dias (${valid ? 'APROVADO' : 'REJEITADO'})`;
                rulesApplied.push(message);
                console.log(`${validationId} - Validação prazo: ${message}`);
            }

            // Validação 5: Detecção de fraude
            const fraudDetection = configs['cpa.validacao.deteccao_fraude_ativa'];
            if (fraudDetection) {
                const fraudResult = this.detectFraud({
                    depositAmount,
                    betCount,
                    ggrAmount,
                    registrationDate,
                    affiliateId,
                    userId
                });
                results.push(fraudResult.valid);
                rulesApplied.push(fraudResult.message);
                console.log(`${validationId} - Detecção fraude: ${fraudResult.message}`);
            }

            // Determinar resultado final
            const allValid = results.every(r => r);
            const finalResult = allValid ? 'approved' : 'rejected';
            
            // Registrar métricas
            cpaValidationCounter.inc({
                result: finalResult,
                option: validationOption,
                affiliate_id: affiliateId
            });

            cpaValidationDuration.observe(
                { option: validationOption },
                (Date.now() - startTime) / 1000
            );

            const response = {
                validationId,
                result: finalResult,
                reason: allValid ? 'Todas as validações aprovadas' : 'Uma ou mais validações falharam',
                details: {
                    affiliateId,
                    userId,
                    validationOption,
                    configsUsed: configs,
                    individualResults: rulesApplied.map((rule, index) => ({
                        rule,
                        result: results[index] ? 'APROVADO' : 'REJEITADO'
                    })),
                    processingTimeMs: Date.now() - startTime
                },
                timestamp: new Date().toISOString(),
                rulesApplied
            };

            console.log(`Validação ${validationId} concluída: ${finalResult.toUpperCase()}`);
            return response;

        } catch (error) {
            console.error(`Erro na validação ${validationId}:`, error);
            
            cpaValidationCounter.inc({
                result: 'error',
                option: validationOption,
                affiliate_id: affiliateId
            });

            return {
                validationId,
                result: 'error',
                reason: `Erro durante validação: ${error.message}`,
                details: { 
                    error: error.message,
                    stack: error.stack,
                    processingTimeMs: Date.now() - startTime
                },
                timestamp: new Date().toISOString(),
                rulesApplied: []
            };
        }
    }

    detectFraud({ depositAmount, betCount, ggrAmount, registrationDate, affiliateId, userId }) {
        const suspiciousPatterns = [];
        
        try {
            // Padrão 1: Depósito alto com poucas apostas
            if (depositAmount > 1000 && betCount < 5) {
                suspiciousPatterns.push('Depósito alto (>R$1000) com poucas apostas (<5)');
            }
            
            // Padrão 2: GGR muito negativo
            if (ggrAmount < -500) {
                suspiciousPatterns.push(`GGR muito negativo (${ggrAmount})`);
            }
            
            // Padrão 3: Atividade alta em conta nova
            const registrationTime = new Date(registrationDate).getTime();
            const daysSinceRegistration = Math.floor((Date.now() - registrationTime) / (1000 * 60 * 60 * 24));
            
            if (daysSinceRegistration < 1 && betCount > 50) {
                suspiciousPatterns.push(`Atividade muito alta (${betCount} apostas) para conta nova (${daysSinceRegistration} dias)`);
            }
            
            // Padrão 4: Razão depósito/apostas suspeita
            const avgBetAmount = betCount > 0 ? depositAmount / betCount : 0;
            if (avgBetAmount > 200) {
                suspiciousPatterns.push(`Valor médio por aposta muito alto (R$${avgBetAmount.toFixed(2)})`);
            }
            
            // Padrão 5: GGR positivo muito alto (possível manipulação)
            if (ggrAmount > depositAmount * 2) {
                suspiciousPatterns.push(`GGR muito alto comparado ao depósito (${((ggrAmount/depositAmount)*100).toFixed(1)}%)`);
            }
            
            if (suspiciousPatterns.length > 0) {
                return {
                    valid: false,
                    message: `FRAUDE DETECTADA: ${suspiciousPatterns.join('; ')}`
                };
            }
            
            return {
                valid: true,
                message: 'Nenhum padrão suspeito detectado'
            };
            
        } catch (error) {
            console.error('Erro na detecção de fraude:', error);
            return {
                valid: true,
                message: `Erro na detecção de fraude: ${error.message}`
            };
        }
    }

    async getActiveRules(validationOption = 'opcao1') {
        try {
            const configKeys = [
                `cpa.validacao.${validationOption}.deposito_minimo`,
                `cpa.validacao.${validationOption}.numero_apostas`,
                `cpa.validacao.${validationOption}.ggr_minimo`,
                'cpa.validacao.prazo_dias',
                'cpa.validacao.timezone',
                'cpa.validacao.deteccao_fraude_ativa'
            ];

            const rules = {};
            for (const key of configKeys) {
                rules[key] = await this.getConfiguration(key);
            }

            return {
                validationOption,
                rules,
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            console.error('Erro ao obter regras ativas:', error);
            throw error;
        }
    }

    async healthCheck() {
        const health = {
            status: 'healthy',
            timestamp: new Date().toISOString(),
            components: {}
        };

        // Verificar config-service
        try {
            const response = await axios.get(`${this.configServiceUrl}/health`, { timeout: 3000 });
            health.components.configService = {
                status: 'healthy',
                url: this.configServiceUrl,
                responseTime: response.headers['x-response-time'] || 'unknown'
            };
        } catch (error) {
            health.components.configService = {
                status: 'unhealthy',
                error: error.message,
                url: this.configServiceUrl
            };
            health.status = 'degraded';
        }

        // Verificar Redis
        if (this.redisClient && this.redisClient.isOpen) {
            try {
                await this.redisClient.ping();
                health.components.redis = {
                    status: 'healthy',
                    url: this.redisUrl
                };
            } catch (error) {
                health.components.redis = {
                    status: 'unhealthy',
                    error: error.message,
                    url: this.redisUrl
                };
                health.status = 'degraded';
            }
        } else {
            health.components.redis = {
                status: 'disconnected',
                url: this.redisUrl
            };
            health.status = 'degraded';
        }

        // Verificar cache local
        health.components.localCache = {
            status: 'healthy',
            size: this.cache.size,
            ttl: this.cacheTTL
        };

        return health;
    }

    getMetrics() {
        return promClient.register.metrics();
    }
}

module.exports = CPARulesEngine;

