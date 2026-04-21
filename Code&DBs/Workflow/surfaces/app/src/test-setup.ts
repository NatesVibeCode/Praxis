import * as matchers from '@testing-library/jest-dom/matchers';

class MockResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}

if (!('ResizeObserver' in globalThis)) {
  Object.defineProperty(globalThis, 'ResizeObserver', {
    configurable: true,
    writable: true,
    value: MockResizeObserver,
  });
}

if (typeof Element !== 'undefined' && typeof Element.prototype.scrollTo !== 'function') {
  Element.prototype.scrollTo = function () {};
}

const globalExpect = (globalThis as { expect?: { extend?: (nextMatchers: Record<string, unknown>) => void } }).expect;
if (typeof globalExpect?.extend === 'function') {
  globalExpect.extend(matchers);
}

const globalVi = (globalThis as { vi?: unknown }).vi;
if (globalVi !== undefined) {
  Object.defineProperty(globalThis, 'jest', {
    configurable: true,
    writable: true,
    value: globalVi,
  });
}
