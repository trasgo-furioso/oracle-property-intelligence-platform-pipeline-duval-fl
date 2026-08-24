import { serve } from '@hono/node-server';
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { createApiRoutes } from './api/routes.js';
import { queryRoutes } from './api/query-routes.js';
import { agentRoutes } from './api/agent-routes.js';

// Start Restate SDK endpoint (binds workflows on port 9081)
import { endpoint as restateEndpoint } from './services/index.js';

// Force tsc to keep the import by referencing it
console.info(`[restate] Endpoint loaded: ${typeof restateEndpoint}`);

const app = new Hono();

// Middleware
app.use('*', logger());
app.use('/api/*', cors());

// Health check
app.get('/api/health', (c) => {
  return c.json({
    status: 'ok',
    service: 'oracle-pipeline-duval',
    timestamp: new Date().toISOString(),
    version: '0.1.0',
  });
});

// Pipeline API routes — runs, sources, stats, trigger (US3 — T047)
const apiRoutes = createApiRoutes();
app.route('/', apiRoutes);

// Property search and detail routes (US4 — T053)
app.route('/', queryRoutes);

// Agent chat routes (US4 — T058)
app.route('/', agentRoutes);

const port = parseInt(process.env.PORT || '9080', 10);

console.info(`Oracle Pipeline Duval — starting on port ${port}`);

serve({
  fetch: app.fetch,
  port,
});

// ---------------------------------------------------------------------------
// Auto-register Restate deployment after server startup
// ---------------------------------------------------------------------------
const RESTATE_ADMIN = process.env.RESTATE_ADMIN_ENDPOINT ?? 'http://restate:9070';
const RESTATE_SERVICE_PORT = process.env.RESTATE_SERVICE_PORT ?? '9081';
const DEPLOYMENT_URI = `http://pipeline:${RESTATE_SERVICE_PORT}`;

async function registerWithRestate(retries = 3, delayMs = 5000): Promise<void> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(`${RESTATE_ADMIN}/deployments`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ uri: DEPLOYMENT_URI }),
      });
      if (res.ok || res.status === 409) {
        // 409 = already registered, which is fine
        console.info(
          `[restate] Registered with Restate at ${DEPLOYMENT_URI} (status ${res.status})`,
        );
        return;
      }
      const body = await res.text().catch(() => '');
      console.warn(
        `[restate] Registration attempt ${attempt}/${retries} failed: ${res.status} ${body}`,
      );
    } catch (err) {
      console.warn(
        `[restate] Registration attempt ${attempt}/${retries} error: ${(err as Error).message}`,
      );
    }
    if (attempt < retries) {
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  console.error('[restate] Failed to register with Restate after all retries — workflows will not be available');
}

// Wait a couple seconds for the Restate endpoint to be ready, then register
setTimeout(() => void registerWithRestate(), 2000);

export default app;
