FROM node:22.18-slim AS builder

WORKDIR /app

# Copy package files and tsconfig
COPY package.json tsconfig.json ./
RUN npm install

# Copy source
COPY src/ src/

# Build — include duckdb for MCP-backed agent tools
RUN printf '{\n  "extends": "./tsconfig.json",\n  "exclude": ["node_modules", "dist", "tests", "src/scripts/query.ts", "src/scripts/deploy.ts", "src/seeds/load-seed.ts"]\n}\n' > tsconfig.build.json && \
    npx tsc -p tsconfig.build.json

# Production stage
FROM node:22.18-slim

WORKDIR /app

COPY --from=builder /app/package.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY src/migrations/ dist/migrations/
COPY data/ data/

ENV NODE_ENV=production
ENV PORT=9080
ENV NODE_OPTIONS="--max-old-space-size=6144"

EXPOSE 9080

CMD ["node", "dist/index.js"]
