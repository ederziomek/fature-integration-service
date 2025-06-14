const request = require('supertest');
const app = require('../src/server');

describe('integration-service', () => {
    test('GET /health should return 200', async () => {
        const response = await request(app).get('/health');
        expect(response.status).toBe(200);
        expect(response.body.status).toBe('ok');
        expect(response.body.service).toBe('integration-service');
    });

    test('GET / should return service info', async () => {
        const response = await request(app).get('/');
        expect(response.status).toBe(200);
        expect(response.body.service).toBe('integration-service');
    });

    test('GET /api/v1/integration-service should return API info', async () => {
        const response = await request(app).get('/api/v1/integration-service');
        expect(response.status).toBe(200);
        expect(response.body.service).toBe('integration-service');
    });
});
