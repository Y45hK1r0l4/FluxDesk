const net = require('net');
const { execSync } = require('child_process');

async function checkPort(port, host = 'localhost', timeout = 3000) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    let isConnected = false;

    socket.setTimeout(timeout);
    socket.once('connect', () => {
      isConnected = true;
      socket.destroy();
      resolve(true);
    });

    socket.once('timeout', () => {
      socket.destroy();
      resolve(false);
    });

    socket.once('error', () => {
      resolve(false);
    });

    socket.connect(port, host);
  });
}

async function verifyInfra() {
  console.log('\n=========================================');
  console.log('⚡ FluxDesk Phase 1 Infrastructure Check');
  console.log('=========================================\n');

  const services = [
    { name: 'Redis Cache (Rate Limiter & PubSub)', host: 'localhost', port: 6379 },
    { name: 'MongoDB Document Database', host: 'localhost', port: 27017 },
    { name: 'Apache Kafka Broker (KRaft)', host: 'localhost', port: 9092 }
  ];

  let allOnline = true;

  for (const svc of services) {
    const online = await checkPort(svc.port, svc.host);
    const badge = online ? '✅ [READY]' : '❌ [OFFLINE]';
    console.log(`${badge} ${svc.name} -> ${svc.host}:${svc.port}`);
    if (!online) allOnline = false;
  }

  console.log('\n-----------------------------------------');
  if (allOnline) {
    console.log('🎉 All core infrastructure services are ONLINE and responding!');
    process.exit(0);
  } else {
    console.log('⚠️ Some services are not yet reachable.');
    console.log('Ensure "docker compose up -d" is running.');
    process.exit(1);
  }
}

verifyInfra();
