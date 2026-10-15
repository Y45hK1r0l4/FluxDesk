const mongoose = require('mongoose');
const config = require('../config/env');
const logger = require('../config/logger');

let isConnected = false;

async function connectMongo() {
  try {
    mongoose.set('strictQuery', true);
    await mongoose.connect(config.mongo.uri, {
      dbName: config.mongo.dbName,
      serverSelectionTimeoutMS: 5000,
    });
    isConnected = true;
    logger.info('MongoDB connected successfully', { uri: config.mongo.uri, db: config.mongo.dbName });
  } catch (err) {
    isConnected = false;
    logger.error('Failed to connect to MongoDB', { error: err.message });
    throw err;
  }
}

mongoose.connection.on('connected', () => {
  isConnected = true;
});

mongoose.connection.on('disconnected', () => {
  isConnected = false;
  logger.warn('MongoDB connection lost');
});

mongoose.connection.on('error', (err) => {
  isConnected = false;
  logger.error('MongoDB error', { error: err.message });
});

async function disconnectMongo() {
  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
    isConnected = false;
    logger.info('MongoDB disconnected cleanly');
  }
}

function getMongoHealth() {
  const state = mongoose.connection.readyState;
  const states = { 0: 'disconnected', 1: 'connected', 2: 'connecting', 3: 'disconnecting' };
  return {
    status: state === 1 ? 'UP' : 'DOWN',
    state: states[state] || 'unknown',
    database: config.mongo.dbName,
  };
}

module.exports = {
  connectMongo,
  disconnectMongo,
  getMongoHealth,
  mongoose,
};
