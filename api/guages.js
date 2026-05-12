/**
 * /api/gauges.js — Vercel serverless function
 * Proxies USGS Water Services IV API with dynamic bounding box filtering.
 * 
 * Query params:
 *   bbox   = west,south,east,north  (required — sent by map on moveend/zoomend)
 *   param  = 00065 (gage height ft, default) | 00060 (streamflow cfs)
 * 
 * Example:
 *   /api/gauges?bbox=-77.5,38.8,-76.2,39.5&param=00065
 */

export default async function handler(req, res) {
  const { bbox, param = '00065' } = req.query;

  // Validate bbox
  if (!bbox) {
    return res.status(400).json({ error: 'bbox is required: west,south,east,north' });
  }

  const parts = bbox.split(',').map(Number);
  if (parts.length !== 4 || parts.some(isNaN)) {
    return res.status(400).json({ error: 'bbox must be 4 comma-separated numbers' });
  }

  const [west, south, east, north] = parts;

  // Clamp to CONUS roughly
  const w = Math.max(west, -180).toFixed(6);
  const s = Math.max(south, -90).toFixed(6);
  const e = Math.min(east, 180).toFixed(6);
  const n = Math.min(north, 90).toFixed(6);

  // USGS IV service — bBox param: west,south,east,north
  // period=PT2H returns latest reading within 2 hours
  const usgsUrl =
    `https://waterservices.usgs.gov/nwis/iv/` +
    `?format=geojson` +
    `&bBox=${w},${s},${e},${n}` +
    `&parameterCd=${param}` +
    `&siteStatus=active` +
    `&period=PT2H`;

  try {
    const upstream = await fetch(usgsUrl, {
      headers: {
        'Accept': 'application/geo+json',
        'User-Agent': 'floodwire2/1.0 (github.com/rmkenv/floodwire2)'
      },
      // 8-second timeout
      signal: AbortSignal.timeout(8000)
    });

    if (!upstream.ok) {
      console.error('USGS error:', upstream.status, await upstream.text());
      return res.status(502).json({ error: `USGS upstream returned ${upstream.status}` });
    }

    const data = await upstream.json();

    // Annotate each feature with a flood-stage color bucket
    // based on gage height percentile thresholds
    if (data.features) {
      data.features = data.features.map(f => {
        const val = parseFloat(f.properties?.value);
        f.properties._floodBucket = classifyGauge(val);
        return f;
      });
    }

    // Cache 10 min on CDN edge, serve stale for 60s while revalidating
    res.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate=60');
    res.setHeader('Access-Control-Allow-Origin', '*');
    return res.status(200).json(data);

  } catch (err) {
    if (err.name === 'TimeoutError') {
      return res.status(504).json({ error: 'USGS request timed out' });
    }
    console.error('Gauge fetch error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

/**
 * Classify gage height into flood buckets.
 * These are general thresholds — real stage data comes from NWS AHPS.
 * Color coding is directional: high values = more concern.
 */
function classifyGauge(ft) {
  if (isNaN(ft) || ft === null) return 'unknown';
  if (ft < 0)   return 'unknown';
  if (ft < 5)   return 'normal';
  if (ft < 10)  return 'watch';
  if (ft < 15)  return 'warning';
  return 'major';
}
