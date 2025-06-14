# integration-service - Sistema Fature

Microserviço integration-service do Sistema Fature.

## Descrição

Este microserviço é responsável por [funcionalidade específica do integration-service].

## Endpoints

- `GET /health` - Health check
- `GET /` - Informações básicas do serviço
- `GET /api/v1/integration-service` - API principal
- `GET /api/v1/integration-service/status` - Status detalhado

## Desenvolvimento

### Pré-requisitos

- Node.js 18+
- npm ou yarn

### Instalação

```bash
npm install
```

### Execução

```bash
# Desenvolvimento
npm run dev

# Produção
npm start
```

### Testes

```bash
npm test
```

## Docker

### Build

```bash
docker build -t fature-integration-service .
```

### Run

```bash
docker run -p 3000:3000 fature-integration-service
```

## Deploy

O deploy é automatizado via GitHub Actions quando há push para a branch main.

### Deploy manual

```bash
kubectl apply -f k8s/deployment.yaml
```

## Monitoramento

- Health check: `http://localhost:3000/health`
- Métricas: `http://localhost:3000/api/v1/integration-service/status`

## Variáveis de Ambiente

- `NODE_ENV` - Ambiente (development/production)
- `PORT` - Porta do servidor (padrão: 3000)

## Arquitetura

Este microserviço faz parte da arquitetura de microserviços do Sistema Fature e se comunica com:

- PostgreSQL (banco interno)
- Redis (cache)
- Banco da operação (PostgreSQL externo)
- Outros microserviços via APIs REST

## Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request
