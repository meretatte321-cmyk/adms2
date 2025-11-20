/**
 * Express ADMS Server for ZKTeco-style devices
 * Vercel-compatible with Turso SQLite
 */

const express = require('express');
const { createClient } = require('@libsql/client');
const axios = require('axios');

const app = express();
const path = require('path');

// --- Configuration ---
const TURSO_DB_URL = process.env.TURSO_DB_URL || 'libsql://adms-adms-server.aws-ap-south-1.turso.io';
const TURSO_DB_TOKEN = process.env.TURSO_DB_TOKEN || 'eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NjM1OTIyNjIsImlkIjoiNTZmZDZlYjEtMTVkYS00N2I5LTk4NTUtNTk0OGU0OGYwYTVkIiwicmlkIjoiYjljODgwNjktZGY2Zi00OWMwLWE3ZGMtY2E4NmQ3Yjc3YjBhIn0._n1GuIbI63s62UQe-Tj2DKh4pIgH_i147CpYetFzpDuGoWozAeWXeBNC1OEfP-3fwNhqFnFRjBwxHFWSSxXaCQ';
const CALLBACK_URL = process.env.CALLBACK_URL;
const MINUTES_FOR_PRESENT = parseInt(process.env.MINUTES_FOR_PRESENT || '360');
const DEVICE_OFFLINE_THRESHOLD = 30000; // 30 seconds in milliseconds

// Initialize Turso client
const db = createClient({
  url: TURSO_DB_URL,
  authToken: TURSO_DB_TOKEN,
});

// In-memory device status cache
const deviceCache = new Map();

// Middleware - Raw body parser for device data
app.use('/iclock/cdata.aspx', express.raw({ type: '*/*', limit: '10mb' }));

// Standard middleware for other routes
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));


// --- Database initialization ---
async function initDB() {
  try {
    // Create punch table
    await db.execute(`
      CREATE TABLE IF NOT EXISTS punch (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pin TEXT NOT NULL,
        ts TEXT NOT NULL,
        status TEXT,
        verify TEXT,
        workcode TEXT,
        reserved TEXT,
        raw TEXT
      )
    `);

    await db.execute(`
      CREATE INDEX IF NOT EXISTS idx_punch_pin ON punch(pin)
    `);

    await db.execute(`
      CREATE INDEX IF NOT EXISTS idx_punch_ts ON punch(ts)
    `);

    // Create attendance table
    await db.execute(`
      CREATE TABLE IF NOT EXISTS attendance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pin TEXT NOT NULL,
        day TEXT NOT NULL,
        first_ts TEXT,
        last_ts TEXT,
        duration_minutes INTEGER,
        status TEXT NOT NULL DEFAULT 'ABSENT',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await db.execute(`
      CREATE INDEX IF NOT EXISTS idx_attendance_pin ON attendance(pin)
    `);

    await db.execute(`
      CREATE INDEX IF NOT EXISTS idx_attendance_day ON attendance(day)
    `);

    // Create devices table
    await db.execute(`
      CREATE TABLE IF NOT EXISTS devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        serial_number TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL DEFAULT 'OFFLINE',
        last_seen TEXT,
        first_registered TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await db.execute(`
      CREATE INDEX IF NOT EXISTS idx_devices_sn ON devices(serial_number)
    `);

    await db.execute(`
      CREATE INDEX IF NOT EXISTS idx_devices_status ON devices(status)
    `);

    console.log('Database initialized successfully');
    
    // Load existing devices into memory cache
    await loadDevicesIntoCache();
  } catch (error) {
    console.error('Database initialization error:', error);
  }
}

// Load devices from DB into memory cache
async function loadDevicesIntoCache() {
  try {
    const result = await db.execute(`SELECT serial_number, status, last_seen FROM devices`);
    
    result.rows.forEach(row => {
      deviceCache.set(row.serial_number, {
        status: row.status,
        lastSeen: row.last_seen ? new Date(row.last_seen) : null,
        lastHeartbeat: row.last_seen ? new Date(row.last_seen) : null
      });
    });
    
    console.log(`Loaded ${deviceCache.size} devices into cache`);
  } catch (error) {
    console.error('Error loading devices into cache:', error);
  }
}

// Background job to check for offline devices
function startDeviceMonitor() {
  setInterval(async () => {
    const now = new Date();
    const devicesToUpdate = [];

    for (const [serialNumber, deviceInfo] of deviceCache.entries()) {
      if (deviceInfo.lastHeartbeat) {
        const timeSinceLastHeartbeat = now - deviceInfo.lastHeartbeat;
        
        if (timeSinceLastHeartbeat > DEVICE_OFFLINE_THRESHOLD && deviceInfo.status === 'ONLINE') {
          deviceInfo.status = 'OFFLINE';
          devicesToUpdate.push(serialNumber);
        }
      }
    }

    // Batch update offline devices in DB
    for (const sn of devicesToUpdate) {
      try {
        await db.execute({
          sql: `UPDATE devices SET status = 'OFFLINE', updated_at = CURRENT_TIMESTAMP WHERE serial_number = ?`,
          args: [sn]
        });
        console.log(`Device ${sn} marked as OFFLINE`);
      } catch (error) {
        console.error(`Error updating device ${sn}:`, error);
      }
    }
  }, 5000); // Check every 5 seconds
}

// Initialize DB on startup
initDB().then(() => {
  startDeviceMonitor();
});

// --- Utilities ---
function parseAttlogText(text) {
  const rows = [];
  const lines = text.replace(/\r/g, '\n').split('\n');

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    let parts = line.split('\t');
    if (parts.length < 2) {
      parts = line.split(/\s+/);
      if (parts.length < 2) continue;
    }

    const pin = parts[0];
    const tsStr = parts[1];
    
    // Parse timestamp
    let ts = null;
    const formats = [
      /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/,
      /^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})$/
    ];

    for (const fmt of formats) {
      const match = tsStr.match(fmt);
      if (match) {
        ts = new Date(`${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}`);
        break;
      }
    }

    if (!ts || isNaN(ts.getTime())) continue;

    rows.push({
      pin,
      ts: ts.toISOString(),
      status: parts[2] || null,
      verify: parts[3] || null,
      workcode: parts[4] || null,
      reserved: parts[5] || null,
      raw: line
    });
  }

  return rows;
}

async function computeAttendanceForDay(dayStr) {
  const dayDate = new Date(dayStr);
  const startDt = new Date(dayDate.setHours(0, 0, 0, 0)).toISOString();
  const endDt = new Date(dayDate.setHours(23, 59, 59, 999)).toISOString();

  // Get distinct pins for the day
  const pinsResult = await db.execute({
    sql: `SELECT DISTINCT pin FROM punch WHERE ts >= ? AND ts <= ?`,
    args: [startDt, endDt]
  });

  const pins = pinsResult.rows.map(row => row.pin);
  const results = [];

  for (const pin of pins) {
    // Get all punches for this pin on this day
    const punchesResult = await db.execute({
      sql: `SELECT * FROM punch WHERE pin = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC`,
      args: [pin, startDt, endDt]
    });

    const punches = punchesResult.rows;
    if (punches.length === 0) continue;

    const firstTs = new Date(punches[0].ts);
    const lastTs = new Date(punches[punches.length - 1].ts);
    const durationMinutes = Math.floor((lastTs - firstTs) / 60000);
    
    let status = 'ABSENT';
    if (durationMinutes >= MINUTES_FOR_PRESENT) {
      status = 'PRESENT';
    } else if (durationMinutes > 0) {
      status = 'SHORT';
    }

    // Upsert attendance
    const existingResult = await db.execute({
      sql: `SELECT * FROM attendance WHERE pin = ? AND day = ?`,
      args: [pin, dayStr]
    });

    const attendanceData = {
      pin,
      day: dayStr,
      first_ts: punches[0].ts,
      last_ts: punches[punches.length - 1].ts,
      duration_minutes: durationMinutes,
      status
    };

    if (existingResult.rows.length > 0) {
      await db.execute({
        sql: `UPDATE attendance SET first_ts = ?, last_ts = ?, duration_minutes = ?, status = ?, updated_at = CURRENT_TIMESTAMP WHERE pin = ? AND day = ?`,
        args: [attendanceData.first_ts, attendanceData.last_ts, attendanceData.duration_minutes, attendanceData.status, pin, dayStr]
      });
    } else {
      await db.execute({
        sql: `INSERT INTO attendance (pin, day, first_ts, last_ts, duration_minutes, status) VALUES (?, ?, ?, ?, ?, ?)`,
        args: [pin, dayStr, attendanceData.first_ts, attendanceData.last_ts, attendanceData.duration_minutes, attendanceData.status]
      });
    }

    results.push(attendanceData);

    // Optional callback
    if (CALLBACK_URL) {
      try {
        await axios.post(CALLBACK_URL, attendanceData, { timeout: 5000 });
      } catch (error) {
        console.warn('Failed to call callback:', error.message);
      }
    }
  }

  return results;
}

// --- Routes ---

// Device registration/heartbeat endpoint
app.get('/iclock/getrequest.aspx', async (req, res) => {
  try {
    const serialNumber = req.query.SN;
    
    if (!serialNumber) {
      return res.status(400).send('OK');
    }

    const now = new Date();
    const nowISO = now.toISOString();

    // Check if device exists in cache
    let deviceInfo = deviceCache.get(serialNumber);

    if (!deviceInfo) {
      // New device - check DB first
      const dbResult = await db.execute({
        sql: `SELECT serial_number, status, last_seen FROM devices WHERE serial_number = ?`,
        args: [serialNumber]
      });

      if (dbResult.rows.length === 0) {
        // Register new device
        await db.execute({
          sql: `INSERT INTO devices (serial_number, status, last_seen, first_registered, updated_at) VALUES (?, 'ONLINE', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
          args: [serialNumber, nowISO]
        });
        
        console.log(`New device registered: ${serialNumber}`);
      } else {
        // Update existing device to ONLINE
        await db.execute({
          sql: `UPDATE devices SET status = 'ONLINE', last_seen = ?, updated_at = CURRENT_TIMESTAMP WHERE serial_number = ?`,
          args: [nowISO, serialNumber]
        });
      }

      // Add to cache
      deviceInfo = {
        status: 'ONLINE',
        lastSeen: now,
        lastHeartbeat: now
      };
      deviceCache.set(serialNumber, deviceInfo);
    } else {
      // Update existing device in cache
      deviceInfo.lastHeartbeat = now;
      
      // Update status to ONLINE if it was OFFLINE
      if (deviceInfo.status === 'OFFLINE') {
        deviceInfo.status = 'ONLINE';
        
        // Update DB asynchronously
        db.execute({
          sql: `UPDATE devices SET status = 'ONLINE', last_seen = ?, updated_at = CURRENT_TIMESTAMP WHERE serial_number = ?`,
          args: [nowISO, serialNumber]
        }).catch(err => console.error(`Error updating device ${serialNumber}:`, err));
      }
      
      // Update last_seen in DB every 10 seconds to reduce DB writes
      if (!deviceInfo.lastSeen || (now - deviceInfo.lastSeen) > 10000) {
        deviceInfo.lastSeen = now;
        
        db.execute({
          sql: `UPDATE devices SET last_seen = ?, updated_at = CURRENT_TIMESTAMP WHERE serial_number = ?`,
          args: [nowISO, serialNumber]
        }).catch(err => console.error(`Error updating device ${serialNumber}:`, err));
      }
    }

    res.send('OK');
  } catch (error) {
    console.error('Error in getrequest.aspx:', error);
    res.send('OK'); // Always respond OK to keep device happy
  }
});

app.get('/iclock/cdata.aspx', (req, res) => {
  res.send('OK');
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.post('/iclock/cdata.aspx', async (req, res) => {
  try {
    const table = req.query.table || req.query.options;
    
    // Get raw body as string
    let raw = '';
    if (Buffer.isBuffer(req.body)) {
      raw = req.body.toString('utf-8');
    } else if (typeof req.body === 'string') {
      raw = req.body;
    } else {
      return res.status(400).json({ error: 'Invalid body format' });
    }

    if (!raw || raw.trim().length === 0) {
      return res.status(400).json({ error: 'empty body' });
    }

    // Handle ATTLOG
    if (table && table.toUpperCase().includes('ATTLOG')) {
      const rows = parseAttlogText(raw);
      let inserted = 0;

      for (const row of rows) {
        await db.execute({
          sql: `INSERT INTO punch (pin, ts, status, verify, workcode, reserved, raw) VALUES (?, ?, ?, ?, ?, ?, ?)`,
          args: [row.pin, row.ts, row.status, row.verify, row.workcode, row.reserved, row.raw]
        });
        inserted++;
      }

      // Compute attendance for affected days
      const affectedDays = [...new Set(rows.map(r => r.ts.split('T')[0]))];
      const results = [];
      
      for (const day of affectedDays) {
        const dayResults = await computeAttendanceForDay(day);
        results.push(...dayResults);
      }

      return res.json({ inserted, attendance: results });
    }

    // Handle other logs (OPLOG, etc.)
    const lines = raw.replace(/\r/g, '\n').split('\n');
    let inserted = 0;

    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line) continue;

      const parts = line.split('\t');
      const pin = parts[0] || 'UNKNOWN';
      let ts = new Date().toISOString();

      if (parts.length > 1) {
        try {
          const parsedDate = new Date(parts[1].replace(/\//g, '-'));
          if (!isNaN(parsedDate.getTime())) {
            ts = parsedDate.toISOString();
          }
        } catch (error) {
          // Use current timestamp
        }
      }

      await db.execute({
        sql: `INSERT INTO punch (pin, ts, status, verify, workcode, reserved, raw) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: [pin, ts, null, null, null, null, line]
      });
      inserted++;
    }

    res.json({ inserted });
  } catch (error) {
    console.error('Error processing cdata.aspx:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get attendance for a specific day
app.get('/attendance/:day', async (req, res) => {
  try {
    const dayStr = req.params.day;
    
    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dayStr)) {
      return res.status(400).json({ error: 'bad date format, use YYYY-MM-DD' });
    }

    const result = await db.execute({
      sql: `SELECT * FROM attendance WHERE day = ?`,
      args: [dayStr]
    });

    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching attendance:', error);
    res.status(500).json({ error: error.message });
  }
});

// List punches
app.get('/punches', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit || '100');
    
    const result = await db.execute({
      sql: `SELECT * FROM punch ORDER BY ts DESC LIMIT ?`,
      args: [limit]
    });

    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching punches:', error);
    res.status(500).json({ error: error.message });
  }
});

// List devices
app.get('/devices', async (req, res) => {
  try {
    const result = await db.execute(`SELECT * FROM devices ORDER BY updated_at DESC`);
    
    // Enrich with real-time cache data
    const devices = result.rows.map(row => ({
      ...row,
      cached_status: deviceCache.get(row.serial_number)?.status || 'UNKNOWN',
      last_heartbeat: deviceCache.get(row.serial_number)?.lastHeartbeat?.toISOString() || null
    }));

    res.json(devices);
  } catch (error) {
    console.error('Error fetching devices:', error);
    res.status(500).json({ error: error.message });
  }
});

// Home route
app.get('/', (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>ADMS Server</title>
      <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        h1 { color: #333; }
        .endpoint { background: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 5px; }
        code { background: #e0e0e0; padding: 2px 5px; border-radius: 3px; }
      </style>
    </head>
    <body>
      <h1>ADMS Server - Running</h1>
      <p>Simple Python-to-Express ADMS Server for ZKTeco-style devices</p>
      
      <h2>Endpoints:</h2>
      <div class="endpoint">
        <strong>GET /iclock/getrequest.aspx?SN=xxx</strong> - Device registration and heartbeat
      </div>
      <div class="endpoint">
        <strong>GET/POST /iclock/cdata.aspx</strong> - Device communication endpoint
      </div>
      <div class="endpoint">
        <strong>GET /attendance/:day</strong> - Get attendance for specific day (YYYY-MM-DD)
      </div>
      <div class="endpoint">
        <strong>GET /punches?limit=100</strong> - List recent punches
      </div>
      <div class="endpoint">
        <strong>GET /devices</strong> - List all registered devices
      </div>
      
      <h2>Configuration:</h2>
      <ul>
        <li>Minutes for Present: ${MINUTES_FOR_PRESENT}</li>
        <li>Device Offline Threshold: ${DEVICE_OFFLINE_THRESHOLD / 1000}s</li>
        <li>Callback URL: ${CALLBACK_URL || 'Not configured'}</li>
        <li>Devices in Cache: ${deviceCache.size}</li>
      </ul>
    </body>
    </html>
  `);
});

// Export for Vercel
module.exports = app;

// Local development server
if (require.main === module) {
  const PORT = process.env.PORT || 5000;
  app.listen(PORT, () => {
    console.log(`ADMS Server running on port ${PORT}`);
  });
}
