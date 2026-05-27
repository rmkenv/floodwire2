/**
 * /api/chat.js — Vercel serverless function (Node runtime)
 * Proxies to Ollama Cloud server-side — handles CORS and Bearer auth.
 * Set OLLAMA_API_KEY as a Vercel environment variable.
 */

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const apiKey = process.env.OLLAMA_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'OLLAMA_API_KEY not set in Vercel environment variables' });

  let body;
  try { body = req.body; } catch { return res.status(400).json({ error: 'Invalid body' }); }
  body.stream = true;

  try {
    const upstream = await fetch('https://ollama.com/api/chat', {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': 'Bearer ' + apiKey,
        'User-Agent':    'floodwire2/1.0',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(60000),
    });

    if (!upstream.ok) {
      const text = await upstream.text();
      return res.status(502).json({ error: 'Ollama ' + upstream.status, detail: text });
    }

    // Stream NDJSON through
    res.setHeader('Content-Type', 'application/x-ndjson');
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('X-Accel-Buffering', 'no');

    const reader = upstream.body.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(value);
    }
    res.end();

  } catch (err) {
    const msg = err.name === 'TimeoutError' ? 'Ollama timed out' : err.message;
    if (!res.headersSent) res.status(504).json({ error: msg });
  }
}
