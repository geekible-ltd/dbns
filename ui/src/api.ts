// Thin client for the public REST API (§15) — the only surface the UI uses
// (§15.8). No internal interfaces.

const BASE = '/api/v1';

async function call<T = unknown>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(BASE + path, {
    method,
    headers: body !== undefined ? { 'content-type': 'application/json' } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (res.status === 204) return undefined as T;
  const j = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(j.error ?? `${method} ${path}: ${res.status}`);
  return j as T;
}

export const api = {
  get: <T = any>(p: string) => call<T>('GET', p),
  post: <T = any>(p: string, b: unknown) => call<T>('POST', p, b),
  put: <T = any>(p: string, b: unknown) => call<T>('PUT', p, b),
  patch: <T = any>(p: string, b: unknown) => call<T>('PATCH', p, b),
  del: (p: string) => call<void>('DELETE', p),
};
