import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { loadSecrets } from './config/secrets.js';
import authRoutes from './routes/auth.js';
import mastersRoutes from './routes/masters.js';
import jobsRoutes from './routes/jobs.js';
import { initializeS3Bucket, getFromS3, streamToBuffer } from './services/s3Service.js';
import { initializeQueue } from './services/queueService.js';
import { initializeWorker } from './workers/documentWorker.js';

dotenv.config();

const app  = express();
const PORT = process.env.PORT || 3001;

/* ── CORS ──────────────────────────────────────────────────────────────────
   Only meaningful in local dev. In production the browser and API share the
   same ALB origin so CORS headers are never sent.
   ───────────────────────────────────────────────────────────────────────── */
app.use(cors({
  origin:      process.env.FRONTEND_URL || 'http://localhost:3000',
  credentials: true
}));

app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

/* ── HEALTH CHECK ── */
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

/* ── API ROUTES — registered first so /api/* never hits the S3 proxy ── */
app.use('/api/auth',    authRoutes);
app.use('/api/masters', mastersRoutes);
app.use('/api/jobs',    jobsRoutes);

/* ── S3 FRONTEND PROXY ─────────────────────────────────────────────────────
   Serves the React build from a private S3 bucket.

   How it works:
     1. Browser requests GET /  or GET /dashboard  or GET /assets/index-abc.js
     2. Express maps the path → S3 key under FRONTEND_S3_PREFIX
     3. Fetches the object from the private S3 bucket (ECS task role only)
     4. Streams it back with the correct Content-Type + cache headers

   React Router support:
     - Known asset extensions  → served as-is from S3
     - All other paths         → serve index.html so React Router handles routing
   ───────────────────────────────────────────────────────────────────────── */
const FRONTEND_S3_PREFIX = process.env.FRONTEND_S3_PREFIX || 'frontend';

const ASSET_EXTENSIONS = /\.(js|css|png|jpg|jpeg|svg|ico|woff|woff2|ttf|eot|map|json|txt|webmanifest)$/i;

const MIME_TYPES = {
  '.js':          'application/javascript',
  '.css':         'text/css',
  '.html':        'text/html; charset=utf-8',
  '.json':        'application/json',
  '.png':         'image/png',
  '.jpg':         'image/jpeg',
  '.jpeg':        'image/jpeg',
  '.svg':         'image/svg+xml',
  '.ico':         'image/x-icon',
  '.woff':        'font/woff',
  '.woff2':       'font/woff2',
  '.ttf':         'font/ttf',
  '.eot':         'application/vnd.ms-fontobject',
  '.map':         'application/json',
  '.webmanifest': 'application/manifest+json',
  '.txt':         'text/plain',
};

function getMimeType(filePath) {
  const ext = filePath.match(/\.[^.]+$/)?.[0]?.toLowerCase() || '';
  return MIME_TYPES[ext] || 'application/octet-stream';
}

async function proxyFromS3(s3Key, res) {
  const stream   = await getFromS3(s3Key);
  const buffer   = await streamToBuffer(stream);
  const mimeType = getMimeType(s3Key);
  res.setHeader('Content-Type', mimeType);
  if (mimeType === 'text/html; charset=utf-8') {
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  } else {
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
  }
  res.send(buffer);
}

// Catch everything that isn't /api/* or /health
app.get('*', async (req, res) => {
  const reqPath = req.path;
  const s3Key   = (reqPath === '/' || !ASSET_EXTENSIONS.test(reqPath))
    ? `${FRONTEND_S3_PREFIX}/index.html`   // React route → SPA fallback
    : `${FRONTEND_S3_PREFIX}${reqPath}`;   // Real static asset

  try {
    await proxyFromS3(s3Key, res);
  } catch (err) {
    // Asset not found → fall back to index.html (handles deep links after deploy)
    if (err?.$metadata?.httpStatusCode === 404 || err?.name === 'NoSuchKey') {
      try {
        await proxyFromS3(`${FRONTEND_S3_PREFIX}/index.html`, res);
      } catch (fallbackErr) {
        console.error('[Frontend Proxy] index.html not found in S3:', fallbackErr.message);
        res.status(404).send('Frontend not deployed. Upload your React build to S3.');
      }
    } else {
      console.error('[Frontend Proxy] S3 fetch error:', err.message);
      res.status(502).send('Error fetching frontend from S3.');
    }
  }
});

/* ── GLOBAL ERROR HANDLER ── */
app.use((err, req, res, next) => {
  console.error('Error:', err);
  if (err.code === 'LIMIT_FILE_SIZE') {
    return res.status(413).json({ error: 'File too large. Maximum size is 4GB.' });
  }
  res.status(err.status || 500).json({
    error:   err.message || 'Internal server error',
    details: process.env.NODE_ENV === 'development' ? err.stack : undefined
  });
});

/* ── BOOT SEQUENCE ── */
async function start() {
  try {
    await loadSecrets(); // AWS Secrets Manager (prod) or .env (dev)

    console.log('Initializing queue...');
    const queueResult = await initializeQueue();
    console.log(queueResult.useInMemory ? '✓ Using in-memory queue' : '✓ Redis queue initialized');

    await initializeS3Bucket();
    console.log('✓ S3 bucket initialized');

    await initializeWorker();
    console.log('✓ Document processing worker initialized');

    app.listen(PORT, () => {
      console.log(`✓ Server running on port ${PORT}`);
      console.log(`  Environment: ${process.env.NODE_ENV || 'development'}`);
      console.log(`  Frontend S3 prefix: ${FRONTEND_S3_PREFIX}`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

start();