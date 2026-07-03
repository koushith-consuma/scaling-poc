/** @type {import('next').NextConfig} */
const BACKEND = process.env.BACKEND_URL || 'http://localhost:3000';

const nextConfig = {
  reactStrictMode: false, // avoid double-mounting our SSE EventSource in dev
  compress: false, // do NOT gzip — gzip buffers SSE streams so EventSource stalls
  async rewrites() {
    // Proxy API + SSE + run calls to the backend web tier so the browser can
    // use same-origin relative URLs (no CORS, SSE works cleanly).
    return [
      { source: '/runs', destination: `${BACKEND}/runs` },
      { source: '/runs/:path*', destination: `${BACKEND}/runs/:path*` },
      { source: '/api/:path*', destination: `${BACKEND}/api/:path*` },
    ];
  },
};

export default nextConfig;
