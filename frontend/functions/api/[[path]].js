export async function onRequest(context) {
  const url = new URL(context.request.url);
  const backendPath = url.pathname.replace(/^\/api/, '');
  const backendUrl = `${context.env.BACKEND_URL}${backendPath}${url.search}`;

  const req = context.request;
  // Forward the real visitor identity (only visible here at the CF edge) plus
  // a few useful headers so the backend can track and authorize requests.
  const headers = {
    Accept: 'application/json',
    'X-Visitor-IP': req.headers.get('CF-Connecting-IP') || '',
    'X-Visitor-Country': (req.cf && req.cf.country) || req.headers.get('CF-IPCountry') || '',
    'User-Agent': req.headers.get('User-Agent') || '',
    Referer: req.headers.get('Referer') || '',
  };
  const adminToken = req.headers.get('X-Admin-Token');
  if (adminToken) headers['X-Admin-Token'] = adminToken;
  // Shared secret (Pages secret PROXY_TOKEN, ~/.config/rpki-proxy.token on the
  // backend): the backend trusts X-Visitor-IP only alongside it, since any
  // client can send that header.
  if (context.env.PROXY_TOKEN) headers['X-Proxy-Token'] = context.env.PROXY_TOKEN;

  try {
    const resp = await fetch(backendUrl, {
      method: req.method,
      headers,
    });

    // Stream the body through unchanged so large downloads (CSV/JSON exports)
    // don't get buffered entirely in memory.
    const respHeaders = {
      'Content-Type': resp.headers.get('Content-Type') || 'application/json',
      'Cache-Control': 'no-store',
    };
    const disposition = resp.headers.get('Content-Disposition');
    if (disposition) respHeaders['Content-Disposition'] = disposition;

    return new Response(resp.body, {
      status: resp.status,
      headers: respHeaders,
    });
  } catch (e) {
    // The error text can describe the backend host: log it, return a generic body.
    console.error('Backend fetch failed:', e);
    return Response.json({ error: 'Backend unavailable' }, { status: 502 });
  }
}
