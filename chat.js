/**
 * /api/chat.js — Vercel serverless function
 * Proxies requests to Ollama Cloud, handling CORS and Bearer auth server-side.
 * The browser calls /api/chat instead of ollama.com directly.
 *
 * Set OLLAMA_API_KEY as a Vercel environment variable (not exposed to browser).
 *
 * Supports streaming — passes through the Ollama NDJSON stream directly.
 */

export const config = {
  runtime: 'edge',   // edge runtime supports streaming responses
};

const OLLAMA_URL = 'https://ollama.com/api/chat';

export default async function handler(req) {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: {
        'Access-Control-Allow-Origin':  '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      },
    });
  }

  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method not allowed' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const apiKey = process.env.OLLAMA_API_KEY;
  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'OLLAMA_API_KEY not configured on server' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }

  let body;
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: 'Invalid JSON body' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }

  // Always force streaming on
  body.stream = true;

  try {
    const upstream = await fetch(OLLAMA_URL, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': 'Bearer ' + apiKey,
        'User-Agent':    'floodwire2/1.0 (github.com/rmkenv/floodwire2)',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(60000),
    });

    if (!upstream.ok) {
      const text = await upstream.text();
      return new Response(JSON.stringify({ error: 'Ollama upstream ' + upstream.status, detail: text }), {
        status: 502,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
      });
    }

    // Stream the NDJSON response straight through
    return new Response(upstream.body, {
      status: 200,
      headers: {
        'Content-Type':                'application/x-ndjson',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control':               'no-store',
        'X-Accel-Buffering':           'no',   // disable nginx buffering on Vercel edge
      },
    });

  } catch (err) {
    const msg = err.name === 'TimeoutError' ? 'Ollama request timed out' : err.message;
    return new Response(JSON.stringify({ error: msg }), {
      status: 504,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }
}
