import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

const nodeMajorVersion = Number.parseInt(process.versions.node.split('.')[0] || '0', 10);
const workerExecArgv = nodeMajorVersion >= 22
  ? [`--localstorage-file=/tmp/praxis-app-vitest-localstorage-${process.pid}.json`]
  : [];

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    globals: true,
    include: ['src/**/*.test.ts', 'src/**/*.test.tsx'],
    poolOptions: {
      threads: {
        execArgv: workerExecArgv,
      },
      forks: {
        execArgv: workerExecArgv,
      },
    },
    setupFiles: ['./src/test-setup.ts'],
  },
});
