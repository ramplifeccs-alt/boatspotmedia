const express = require('express');
const path = require('path');
const session = require('express-session');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;
const OWNER_USER = process.env.OWNER_USER || 'cp12517';
const OWNER_PASS = process.env.OWNER_PASS || '645231cp';

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.urlencoded({ extended: true }));
app.use(session({
  secret: process.env.SESSION_SECRET || 'boatspotmedia-secret',
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 1000 * 60 * 60 * 8 }
}));

const dbUrl = process.env.DATABASE_URL || process.env.POSTGRES_URL || process.env.POSTGRESQL_URL;
let pool = null;
let dbEnabled = false;
if (dbUrl) {
  pool = new Pool({
    connectionString: dbUrl,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  });
}

const fallback = {
  applications: [],
  creators: [
    { id: 1, public_name: 'RampLifeCCS', company_name: 'RampLife Media', email: 'demo@boatspotmedia.com', username: 'demo', password: 'demo', instagram: '@ramplifeccs', approved: true }
  ],
  orders: [
    { id: 1, buyer_name: 'Luis Ortega', buyer_email: 'luis@example.com', creator_id: 1, package_name: 'Original Video', delivery_type: 'instant', status: 'Delivered', purchased_at: '2026-04-17 14:10', source_file: 'P1002345.MP4', video_date: '04/12/2026', video_time: '2:32 PM', batch_name: 'Boca Afternoon', location: 'Boca Raton Inlet' },
    { id: 2, buyer_name: 'Maria Bello', buyer_email: 'maria@example.com', creator_id: 1, package_name: 'Edited Social Version', delivery_type: 'manual', status: 'Pending Creator Delivery', purchased_at: '2026-04-17 15:44', source_file: 'P1002351.MP4', video_date: '04/12/2026', video_time: '3:15 PM', batch_name: 'Boca Afternoon', location: 'Boca Raton Inlet' }
  ]
};

const latestUploads = [
  { id: 1, creator: 'RampLifeCCS', location: 'Boca Raton Inlet', date: '04/12/2026', time: '2:32 PM', price_from: 20, title: 'Midnight Express passing south jetty' },
  { id: 2, creator: 'Wavy Boats', location: 'Hillsboro Inlet', date: '04/13/2026', time: '1:18 PM', price_from: 20, title: 'Valhalla inbound' },
  { id: 3, creator: 'Boat Snap', location: 'Fort Lauderdale Waterway', date: '04/14/2026', time: '4:05 PM', price_from: 25, title: 'Slow yacht cruise' },
  { id: 4, creator: 'RampLifeCCS', location: 'Boca Raton Inlet', date: '04/15/2026', time: '11:41 AM', price_from: 20, title: 'Center console outbound' }
];

async function initDb() {
  if (!pool) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS creator_applications (
        id SERIAL PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        company_name TEXT,
        email TEXT NOT NULL,
        instagram TEXT NOT NULL,
        facebook TEXT,
        youtube TEXT,
        reason TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS creators (
        id SERIAL PRIMARY KEY,
        public_name TEXT NOT NULL,
        company_name TEXT,
        email TEXT NOT NULL UNIQUE,
        username TEXT NOT NULL UNIQUE,
        password TEXT NOT NULL,
        instagram TEXT,
        approved BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        buyer_name TEXT,
        buyer_email TEXT,
        creator_id INTEGER,
        package_name TEXT,
        delivery_type TEXT,
        status TEXT,
        purchased_at TEXT,
        source_file TEXT,
        video_date TEXT,
        video_time TEXT,
        batch_name TEXT,
        location TEXT
      );
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS locations (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
      );
    `);

    const demoExists = await pool.query('SELECT id FROM creators WHERE username=$1', ['demo']);
    if (demoExists.rows.length === 0) {
      await pool.query(
        'INSERT INTO creators (public_name, company_name, email, username, password, instagram, approved) VALUES ($1,$2,$3,$4,$5,$6,$7)',
        ['RampLifeCCS', 'RampLife Media', 'demo@boatspotmedia.com', 'demo', 'demo', '@ramplifeccs', true]
      );
    }
    const orderCount = await pool.query('SELECT COUNT(*)::int AS c FROM orders');
    if (orderCount.rows[0].c === 0) {
      await pool.query(`
        INSERT INTO orders (buyer_name,buyer_email,creator_id,package_name,delivery_type,status,purchased_at,source_file,video_date,video_time,batch_name,location)
        VALUES
        ('Luis Ortega','luis@example.com',1,'Original Video','instant','Delivered','2026-04-17 14:10','P1002345.MP4','04/12/2026','2:32 PM','Boca Afternoon','Boca Raton Inlet'),
        ('Maria Bello','maria@example.com',1,'Edited Social Version','manual','Pending Creator Delivery','2026-04-17 15:44','P1002351.MP4','04/12/2026','3:15 PM','Boca Afternoon','Boca Raton Inlet')
      `);
    }
    for (const l of ['Boca Raton Inlet', 'Hillsboro Inlet', 'Fort Lauderdale Waterway']) {
      await pool.query('INSERT INTO locations (name) VALUES ($1) ON CONFLICT (name) DO NOTHING', [l]);
    }
    dbEnabled = true;
    console.log('Database ready');
  } catch (err) {
    console.error('DB init failed, falling back to memory:', err.message);
  }
}

function normalizeLocation(value = '') {
  return value
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

function requireOwner(req, res, next) {
  if (req.session.owner) return next();
  return res.redirect('/owner-login');
}

function requireCreator(req, res, next) {
  if (req.session.creator) return next();
  return res.redirect('/creators/login');
}

app.get('/', async (req, res) => {
  let locations = ['Boca Raton Inlet', 'Hillsboro Inlet', 'Fort Lauderdale Waterway'];
  if (dbEnabled) {
    const r = await pool.query('SELECT name FROM locations ORDER BY name');
    locations = r.rows.map(x => x.name);
  }
  const q = {
    location: req.query.location || '',
    date: req.query.date || '',
    from: req.query.from || '',
    to: req.query.to || ''
  };
  const filtered = latestUploads.filter(v => {
    if (q.location && v.location !== q.location) return false;
    if (q.date && v.date !== q.date) return false;
    return true;
  });
  res.render('home', { locations, latestUploads: filtered, q });
});

app.get('/apply-creator', (req, res) => {
  res.render('apply-creator', { success: req.query.success, error: '' });
});

app.post('/apply-creator', async (req, res) => {
  const body = req.body;
  if (dbEnabled) {
    await pool.query(
      `INSERT INTO creator_applications (first_name,last_name,company_name,email,instagram,facebook,youtube,reason)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [body.first_name, body.last_name, body.company_name, body.email, body.instagram, body.facebook, body.youtube, body.reason]
    );
  } else {
    fallback.applications.push({ ...body, id: fallback.applications.length + 1, status: 'pending' });
  }
  res.redirect('/apply-creator?success=1');
});

app.get('/creators/login', (req, res) => {
  res.render('creator-login', { error: '' });
});

app.post('/creators/login', async (req, res) => {
  const { username, password } = req.body;
  let creator = null;
  if (dbEnabled) {
    const r = await pool.query('SELECT * FROM creators WHERE username=$1 AND password=$2 AND approved=true', [username, password]);
    creator = r.rows[0];
  } else {
    creator = fallback.creators.find(c => c.username === username && c.password === password && c.approved);
  }
  if (!creator) return res.render('creator-login', { error: 'Invalid username or password.' });
  req.session.creator = { id: creator.id, public_name: creator.public_name, username: creator.username };
  res.redirect('/creator');
});

app.get('/creator', requireCreator, async (req, res) => {
  let creator = req.session.creator;
  let orders = fallback.orders.filter(o => o.creator_id === creator.id);
  if (dbEnabled) {
    const c = await pool.query('SELECT * FROM creators WHERE id=$1', [creator.id]);
    if (c.rows[0]) creator = c.rows[0];
    const o = await pool.query('SELECT * FROM orders WHERE creator_id=$1 ORDER BY id DESC', [creator.id]);
    orders = o.rows;
  }
  res.render('creator-dashboard', { creator, orders });
});

app.get('/creator/logout', (req, res) => {
  req.session.destroy(() => res.redirect('/creators/login'));
});

app.get('/owner-login', (req, res) => {
  res.render('owner-login', { error: '' });
});

app.post('/owner-login', (req, res) => {
  const { username, password } = req.body;
  if (username !== OWNER_USER || password !== OWNER_PASS) return res.render('owner-login', { error: 'Invalid owner credentials.' });
  req.session.owner = true;
  res.redirect('/admin');
});

app.get('/admin', requireOwner, async (req, res) => {
  let applications = fallback.applications;
  let creators = fallback.creators;
  let orders = fallback.orders;
  if (dbEnabled) {
    applications = (await pool.query('SELECT * FROM creator_applications ORDER BY id DESC')).rows;
    creators = (await pool.query('SELECT * FROM creators ORDER BY id DESC')).rows;
    orders = (await pool.query('SELECT * FROM orders ORDER BY id DESC')).rows;
  }
  res.render('admin-dashboard', { applications, creators, orders });
});

app.post('/admin/applications/:id/approve', requireOwner, async (req, res) => {
  const id = Number(req.params.id);
  if (dbEnabled) {
    const a = await pool.query('SELECT * FROM creator_applications WHERE id=$1', [id]);
    if (a.rows[0]) {
      const appRec = a.rows[0];
      const username = (appRec.company_name || `${appRec.first_name}${appRec.last_name}`).toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 18);
      await pool.query('UPDATE creator_applications SET status=$1 WHERE id=$2', ['approved', id]);
      await pool.query(
        'INSERT INTO creators (public_name,company_name,email,username,password,instagram,approved) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (email) DO NOTHING',
        [appRec.company_name || `${appRec.first_name} ${appRec.last_name}`, appRec.company_name, appRec.email, username, 'changeme123', appRec.instagram, true]
      );
    }
  } else {
    const appRec = fallback.applications.find(x => x.id === id);
    if (appRec) {
      appRec.status = 'approved';
      fallback.creators.push({
        id: fallback.creators.length + 1,
        public_name: appRec.company_name || `${appRec.first_name} ${appRec.last_name}`,
        company_name: appRec.company_name,
        email: appRec.email,
        username: (appRec.company_name || `${appRec.first_name}${appRec.last_name}`).toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 18),
        password: 'changeme123',
        instagram: appRec.instagram,
        approved: true
      });
    }
  }
  res.redirect('/admin');
});

app.get('/health', (_req, res) => res.send('ok'));

app.listen(PORT, async () => {
  await initDb();
  console.log(`BoatSpotMedia running on port ${PORT}`);
});
