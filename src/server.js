const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const SERVICE_NAME = 'integration-service';

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
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'ok',
        service: SERVICE_NAME,
        timestamp: new Date().toISOString(),
        version: '1.0.0',
        environment: process.env.NODE_ENV || 'development'
    });
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({
        service: SERVICE_NAME,
        message: `Microserviço ${SERVICE_NAME} do Sistema Fature`,
        version: '1.0.0',
        endpoints: {
            health: '/health',
            api: `/api/v1/${SERVICE_NAME}`,
            testConnection: '/api/v1/integration-service/test-connection',
            syncData: '/api/v1/integration-service/sync-data'
        }
    });
});

// API principal
app.get(`/api/v1/${SERVICE_NAME}`, (req, res) => {
    res.json({
        service: SERVICE_NAME,
        message: `API do ${SERVICE_NAME} funcionando`,
        timestamp: new Date().toISOString(),
        data: {
            status: 'operational',
            features: ['health-check', 'basic-api', 'logging', 'database-integration']
        }
    });
});

// Endpoint para teste de conectividade
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

// Start server
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 ${SERVICE_NAME} rodando na porta ${PORT}`);
    console.log(`📊 Health check: http://localhost:${PORT}/health`);
    console.log(`🔗 API: http://localhost:${PORT}/api/v1/${SERVICE_NAME}`);
    console.log(`🔌 Test Connection: http://localhost:${PORT}/api/v1/integration-service/test-connection`);
});

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

