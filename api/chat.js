/**
 * /api/chat.js — Vercel serverless function
 * Proxies to Ollama Cloud. Handles both streaming NDJSON and single JSON responses.
 * Set OLLAMA_API_KEY in Vercel Environment Variables.
 */

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'POST')   return res.status(405).json({ error: 'Method not allowed' });

  const apiKey = process.env.OLLAMA_API_KEY;
  if (!apiKey) return res.status(500).json({ error: 'OLLAMA_API_KEY not set in Vercel environment variables' });

  try {
    const upstream = await fetch('https://ollama.com/api/chat', {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': 'Bearer ' + apiKey,
        'User-Agent':    'floodwire2/1.0',
      },
      body: JSON.stringify({ ...req.body, stream: false }),
      signal: AbortSignal.timeout(55000),
    });

    if (!upstream.ok) {
      const text = await upstream.text();
      return res.status(502).json({ error: 'Ollama ' + upstream.status, detail: text });
    }

    // Ollama may return NDJSON even with stream:false — parse all lines, use last complete object
    const raw = await upstream.text();
    const lines = raw.split('\n').map(l => l.trim()).filter(Boolean);

    let parsed = null;
    for (const line of lines) {
      try { parsed = JSON.parse(line); } catch { /* skip malformed lines */ }
    }

    if (!parsed) {
      return res.status(502).json({ error: 'Could not parse Ollama response', raw: raw.slice(0, 500) });
    }

    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'no-store');
    return res.status(200).json(parsed);

  } catch (err) {
    const msg = err.name === 'TimeoutError' ? 'Ollama timed out after 55s' : err.message;
    return res.status(504).json({ error: msg });
  }
}
