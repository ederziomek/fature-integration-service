FROM node:18-alpine

# Criar diretório da aplicação
WORKDIR /app

# Copiar package.json
COPY package.json ./

# Instalar dependências
RUN npm install --production

# Copiar código da aplicação
COPY src/ ./src/

# Criar usuário não-root
RUN addgroup -g 1001 -S nodejs
RUN adduser -S nodejs -u 1001

# Mudar ownership dos arquivos
RUN chown -R nodejs:nodejs /app
USER nodejs

# Expor porta
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD node -e "require('http').get('http://localhost:3000/health', (res) => { process.exit(res.statusCode === 200 ? 0 : 1) }).on('error', () => process.exit(1))"

# Comando para iniciar a aplicação
CMD ["npm", "start"]
