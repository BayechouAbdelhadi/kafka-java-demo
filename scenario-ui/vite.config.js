import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// The browser only talks to the Vite dev server; it proxies to the services, so they need no CORS setup.
const ORDER = process.env.ORDER_URL ?? 'http://localhost:8080';
const INVENTORY = process.env.INVENTORY_URL ?? 'http://localhost:8082';
const NOTIFICATION = process.env.NOTIFICATION_URL ?? 'http://localhost:8083';
const KAFKA_UI = process.env.KAFKA_UI_URL ?? 'http://localhost:8090';

const health = (target) => ({ target, rewrite: () => '/actuator/health' });

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api/orders': ORDER,
      '/api/inventory': INVENTORY,
      '/health/order': health(ORDER),
      '/health/inventory': health(INVENTORY),
      '/health/notification': health(NOTIFICATION),
      // Kafka UI's REST API: topic messages and consumer group offsets, used to draw each order's path
      '/kafka-ui': { target: KAFKA_UI, rewrite: (path) => path.replace(/^\/kafka-ui/, '') },
    },
  },
});
