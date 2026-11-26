import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    // Dev proxy → ndb-server REST (§15.8: the UI consumes only the public API).
    proxy: { '/api': 'http://localhost:8080' },
  },
});
