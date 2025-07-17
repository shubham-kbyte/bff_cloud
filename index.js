const express = require('express');
const mysql = require('mysql2/promise');
const { body, validationResult } = require('express-validator');
const dotenv = require('dotenv');
const winston = require('winston');

// Load environment variables
dotenv.config();

// Initialize Express app
const app = express();
app.use(express.json());

// Configure Winston logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' })
  ]
});

// Database configuration for both systems
const dbConfig = {
  system1: {
    host: process.env.DB_HOST_SYSTEM1 || 'localhost',
    user: process.env.DB_USER_SYSTEM1 || 'root',
    password: process.env.DB_PASSWORD_SYSTEM1 || '',
    database: process.env.DB_NAME_SYSTEM1 || 'bff_api',
    port: process.env.DB_PORT_SYSTEM1 || 3306
  },
  system2: {
    host: process.env.DB_HOST_SYSTEM2 || 'localhost',
    user: process.env.DB_USER_SYSTEM2 || 'root',
    password: process.env.DB_PASSWORD_SYSTEM2 || '',
    database: process.env.DB_NAME_SYSTEM2 || 'cloud',
    port: process.env.DB_PORT_SYSTEM2 || 3306
  }
};

// Create connection pools
const poolSystem1 = mysql.createPool(dbConfig.system1);
const poolSystem2 = mysql.createPool(dbConfig.system2);

// API endpoint to handle notifications
app.post('/work/notifyme',
  [
    body('dm_id').isInt().withMessage('dm_id must be an integer'),
    body('notify_check').isIn([0, 1]).withMessage('notify_check must be 0 or 1'),
    body('target_system').isIn(['1', '2', '0']).withMessage('target_system must be 1, 2, or 0')
  ],
  async (req, res) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      logger.error('Validation errors', { errors: errors.array() });
      return res.status(400).json({ errors: errors.array() });
    }

    const { dm_id, notify_check, target_system } = req.body;
    const requestPayload = JSON.stringify(req.body);
    const systems = target_system === '0' ? ['1', '2'] : [target_system];

    try {
      const results = [];
      for (const system of systems) {
        const pool = system === '1' ? poolSystem1 : poolSystem2;
        const connection = await pool.getConnection();

        try {
          await connection.beginTransaction();

          // Insert into notification_check
          await connection.query(
            'INSERT INTO notification_check (dm_id, notify_check, cr_date, update_date) VALUES (?, ?, NOW(), NOW())',
            [dm_id, notify_check]
          );

          // Insert into api_logs
          const [logResult] = await connection.query(
            'INSERT INTO api_logs (endpoint, method, request_payload, response_payload, status_code, created_at) VALUES (?, ?, ?, ?, ?, NOW())',
            ['/work/notifyme', 'POST', requestPayload, JSON.stringify({ success: true }), 200]
          );

          await connection.commit();
          results.push({ system: `system${system}`, status: 'success', log_id: logResult.insertId });
        } catch (error) {
          await connection.rollback();
          logger.error(`Error in system${system}`, { error: error.message });
          results.push({ system: `system${system}`, status: 'error', error: error.message });
        } finally {
          connection.release();
        }
      }

      res.status(200).json({ results });
    } catch (error) {
      logger.error('Unexpected error', { error: error.message });
      res.status(500).json({ error: 'Internal server error' });
    }
  }
);

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error('Server error', { error: err.message });
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logger.info(`BFF server running on port ${PORT}`);
});
