const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const { Pool } = require('pg');
const CPARulesEngine = require('./cpa-engine');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const SERVICE_NAME = 'integration-service';

// Inicializar motor de regras CPA
const cpaEngine = new CPARulesEngine();

// Configuração do banco de dados da operação
const externalDbPool = new Pool({
    host: process.env.EXTERNAL_DB_HOST || '177.115.223.216',
    port: process.env.EXTERNAL_DB_PORT || 5999,
    user: process.env.EXTERNAL_DB_USER || 'userschaphz',
    password: process.env.EXTERNAL_DB_PASSWORD || 'mschaphz8881!',
    database: process.env.EXTERNAL_DB_NAME || 'dados_interno',
    ssl: false,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 30000,
    max: 10
});

// Middleware
app.use(helmet());
app.use(cors());
app.use(morgan('combined'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health check endpoint
app.get('/health', async (req, res) => {
    try {
        const cpaHealth = await cpaEngine.healthCheck();
        
        res.status(200).json({
            status: 'ok',
            service: SERVICE_NAME,
            timestamp: new Date().toISOString(),
            version: '1.0.0',
            environment: process.env.NODE_ENV || 'development',
            components: {
                cpaEngine: cpaHealth
            }
        });
    } catch (error) {
        res.status(503).json({
            status: 'degraded',
            service: SERVICE_NAME,
            timestamp: new Date().toISOString(),
            error: error.message
        });
    }
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({
        service: SERVICE_NAME,
        message: `Microserviço ${SERVICE_NAME} do Sistema Fature`,
        version: '1.0.0',
        features: ['CPA Validation Engine', 'Database Integration', 'Metrics & Monitoring'],
        endpoints: {
            health: '/health',
            metrics: '/metrics',
            api: `/api/v1/${SERVICE_NAME}`,
            cpaValidation: '/api/v1/integration-service/validate-cpa',
            cpaRules: '/api/v1/integration-service/cpa-rules',
            testConnection: '/api/v1/integration-service/test-connection',
            syncData: '/api/v1/integration-service/sync-data'
        }
    });
});

// Métricas Prometheus
app.get('/metrics', async (req, res) => {
    try {
        const metrics = await cpaEngine.getMetrics();
        res.set('Content-Type', 'text/plain');
        res.send(metrics);
    } catch (error) {
        res.status(500).json({
            status: 'error',
            message: 'Erro ao obter métricas',
            error: error.message
        });
    }
});

// ==================== ENDPOINTS CPA ====================

// Endpoint principal para validação CPA
app.post('/api/v1/integration-service/validate-cpa', async (req, res) => {
    try {
        const validationRequest = req.body;
        
        // Validar campos obrigatórios
        const required = ['affiliateId', 'userId', 'depositAmount', 'betCount', 'ggrAmount', 'registrationDate'];
        const missing = required.filter(field => !validationRequest[field] && validationRequest[field] !== 0);
        
        if (missing.length > 0) {
            return res.status(400).json({
                status: 'error',
                message: `Campos obrigatórios ausentes: ${missing.join(', ')}`,
                required_fields: required,
                received_fields: Object.keys(validationRequest)
            });
        }
        
        // Validar tipos de dados
        if (typeof validationRequest.depositAmount !== 'number' || validationRequest.depositAmount < 0) {
            return res.status(400).json({
                status: 'error',
                message: 'depositAmount deve ser um número positivo'
            });
        }
        
        if (typeof validationRequest.betCount !== 'number' || validationRequest.betCount < 0) {
            return res.status(400).json({
                status: 'error',
                message: 'betCount deve ser um número inteiro positivo'
            });
        }
        
        if (typeof validationRequest.ggrAmount !== 'number') {
            return res.status(400).json({
                status: 'error',
                message: 'ggrAmount deve ser um número'
            });
        }
        
        // Validar data de registro
        const registrationDate = new Date(validationRequest.registrationDate);
        if (isNaN(registrationDate.getTime())) {
            return res.status(400).json({
                status: 'error',
                message: 'registrationDate deve ser uma data válida'
            });
        }
        
        // Executar validação CPA
        console.log(`Recebida requisição de validação CPA para afiliado ${validationRequest.affiliateId}`);
        const result = await cpaEngine.validateCPA(validationRequest);
        
        res.json({
            status: 'success',
            message: 'Validação CPA executada com sucesso',
            data: result
        });
        
    } catch (error) {
        console.error('Erro na validação CPA:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro interno na validação CPA',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Endpoint para consultar regras CPA ativas
app.get('/api/v1/integration-service/cpa-rules', async (req, res) => {
    try {
        const option = req.query.option || 'opcao1';
        
        if (!['opcao1', 'opcao2'].includes(option)) {
            return res.status(400).json({
                status: 'error',
                message: 'Opção de validação deve ser "opcao1" ou "opcao2"'
            });
        }
        
        const rules = await cpaEngine.getActiveRules(option);
        
        res.json({
            status: 'success',
            message: 'Regras CPA obtidas com sucesso',
            data: rules
        });
        
    } catch (error) {
        console.error('Erro ao obter regras CPA:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro ao obter regras CPA',
            error: error.message
        });
    }
});

// Endpoint para testar validação CPA com dados simulados
app.post('/api/v1/integration-service/test-cpa', async (req, res) => {
    try {
        const testScenarios = [
            {
                name: 'Cenário Aprovado',
                data: {
                    affiliateId: 'TEST_AFF_001',
                    userId: 'TEST_USER_001',
                    depositAmount: 100.0,
                    betCount: 20,
                    ggrAmount: 50.0,
                    registrationDate: new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString(),
                    validationOption: 'opcao1'
                }
            },
            {
                name: 'Cenário Rejeitado - Depósito Baixo',
                data: {
                    affiliateId: 'TEST_AFF_002',
                    userId: 'TEST_USER_002',
                    depositAmount: 10.0,
                    betCount: 5,
                    ggrAmount: 5.0,
                    registrationDate: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
                    validationOption: 'opcao1'
                }
            },
            {
                name: 'Cenário Fraude - Depósito Alto, Poucas Apostas',
                data: {
                    affiliateId: 'TEST_AFF_003',
                    userId: 'TEST_USER_003',
                    depositAmount: 2000.0,
                    betCount: 3,
                    ggrAmount: 100.0,
                    registrationDate: new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString(),
                    validationOption: 'opcao1'
                }
            }
        ];
        
        const results = [];
        
        for (const scenario of testScenarios) {
            try {
                const result = await cpaEngine.validateCPA(scenario.data);
                results.push({
                    scenario: scenario.name,
                    input: scenario.data,
                    result: result
                });
            } catch (error) {
                results.push({
                    scenario: scenario.name,
                    input: scenario.data,
                    error: error.message
                });
            }
        }
        
        res.json({
            status: 'success',
            message: 'Testes de validação CPA executados',
            data: {
                total_scenarios: testScenarios.length,
                results: results,
                timestamp: new Date().toISOString()
            }
        });
        
    } catch (error) {
        console.error('Erro nos testes CPA:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro ao executar testes CPA',
            error: error.message
        });
    }
});

// ==================== ENDPOINTS ORIGINAIS ====================

// API principal
app.get(`/api/v1/${SERVICE_NAME}`, (req, res) => {
    res.json({
        service: SERVICE_NAME,
        message: `API do ${SERVICE_NAME} funcionando`,
        timestamp: new Date().toISOString(),
        data: {
            status: 'operational',
            features: ['health-check', 'cpa-validation', 'database-integration', 'metrics']
        }
    });
});

// Endpoint para status do serviço
app.get(`/api/v1/${SERVICE_NAME}/status`, (req, res) => {
    res.json({
        service: SERVICE_NAME,
        status: 'running',
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        timestamp: new Date().toISOString()
    });
});

// Endpoint para testar conexão com banco da operação
app.get('/api/v1/integration-service/test-connection', async (req, res) => {
    try {
        const client = await externalDbPool.connect();
        const result = await client.query('SELECT NOW() as current_time, version() as db_version');
        client.release();
        
        res.json({
            status: 'success',
            message: 'Conexão com banco da operação estabelecida com sucesso',
            data: {
                connected: true,
                timestamp: result.rows[0].current_time,
                database_version: result.rows[0].db_version,
                connection_info: {
                    host: process.env.EXTERNAL_DB_HOST,
                    port: process.env.EXTERNAL_DB_PORT,
                    database: process.env.EXTERNAL_DB_NAME
                }
            }
        });
    } catch (error) {
        console.error('Erro ao conectar com banco da operação:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro ao conectar com banco da operação',
            error: error.message,
            connection_info: {
                host: process.env.EXTERNAL_DB_HOST,
                port: process.env.EXTERNAL_DB_PORT,
                database: process.env.EXTERNAL_DB_NAME
            }
        });
    }
});

// Endpoint para listar tabelas disponíveis
app.get('/api/v1/integration-service/list-tables', async (req, res) => {
    try {
        const client = await externalDbPool.connect();
        const result = await client.query(`
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        `);
        client.release();
        
        res.json({
            status: 'success',
            message: 'Tabelas listadas com sucesso',
            data: {
                tables: result.rows,
                total_tables: result.rows.length
            }
        });
    } catch (error) {
        console.error('Erro ao listar tabelas:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro ao listar tabelas',
            error: error.message
        });
    }
});

// Endpoint para sincronizar dados (exemplo básico)
app.post('/api/v1/integration-service/sync-data', async (req, res) => {
    try {
        const { table_name, limit = 10 } = req.body;
        
        if (!table_name) {
            return res.status(400).json({
                status: 'error',
                message: 'Nome da tabela é obrigatório'
            });
        }
        
        const client = await externalDbPool.connect();
        const result = await client.query(`SELECT * FROM ${table_name} LIMIT $1`, [limit]);
        client.release();
        
        res.json({
            status: 'success',
            message: `Dados da tabela ${table_name} sincronizados`,
            data: {
                table: table_name,
                records: result.rows,
                total_records: result.rows.length,
                sync_timestamp: new Date().toISOString()
            }
        });
    } catch (error) {
        console.error('Erro ao sincronizar dados:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro ao sincronizar dados',
            error: error.message
        });
    }
});

// Endpoint para estatísticas do banco
app.get('/api/v1/integration-service/database-stats', async (req, res) => {
    try {
        const client = await externalDbPool.connect();
        
        // Obter estatísticas básicas
        const statsQuery = `
            SELECT 
                schemaname,
                tablename,
                n_tup_ins as inserts,
                n_tup_upd as updates,
                n_tup_del as deletes,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples
            FROM pg_stat_user_tables 
            ORDER BY n_live_tup DESC
            LIMIT 20
        `;
        
        const result = await client.query(statsQuery);
        client.release();
        
        res.json({
            status: 'success',
            message: 'Estatísticas do banco obtidas com sucesso',
            data: {
                table_stats: result.rows,
                generated_at: new Date().toISOString()
            }
        });
    } catch (error) {
        console.error('Erro ao obter estatísticas:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro ao obter estatísticas do banco',
            error: error.message
        });
    }
});

// Error handling
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({
        error: 'Internal Server Error',
        service: SERVICE_NAME,
        timestamp: new Date().toISOString()
    });
});

// 404 handler
app.use('*', (req, res) => {
    res.status(404).json({
        error: 'Not Found',
        service: SERVICE_NAME,
        path: req.originalUrl,
        timestamp: new Date().toISOString()
    });
});

// Inicializar motor CPA e servidor
async function startServer() {
    try {
        console.log('🔧 Inicializando motor de regras CPA...');
        await cpaEngine.initialize();
        console.log('✅ Motor de regras CPA inicializado com sucesso');
        
        // Start server
        app.listen(PORT, '0.0.0.0', () => {
            console.log(`🚀 ${SERVICE_NAME} rodando na porta ${PORT}`);
            console.log(`📊 Health check: http://localhost:${PORT}/health`);
            console.log(`📈 Métricas: http://localhost:${PORT}/metrics`);
            console.log(`🔗 API: http://localhost:${PORT}/api/v1/${SERVICE_NAME}`);
            console.log(`⚖️  Validação CPA: http://localhost:${PORT}/api/v1/integration-service/validate-cpa`);
            console.log(`📋 Regras CPA: http://localhost:${PORT}/api/v1/integration-service/cpa-rules`);
            console.log(`🧪 Teste CPA: http://localhost:${PORT}/api/v1/integration-service/test-cpa`);
        });
        
    } catch (error) {
        console.error('❌ Erro ao inicializar servidor:', error);
        process.exit(1);
    }
}

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('📴 Recebido SIGTERM, encerrando servidor...');
    externalDbPool.end(() => {
        console.log('🔌 Pool de conexões fechado');
        process.exit(0);
    });
});

process.on('SIGINT', () => {
    console.log('📴 Recebido SIGINT, encerrando servidor...');
    externalDbPool.end(() => {
        console.log('🔌 Pool de conexões fechado');
        process.exit(0);
    });
});

// Iniciar servidor
startServer();

