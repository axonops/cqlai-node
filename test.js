const { CQLSession } = require('.');

const SSL_DIR = '/home/mhmd/Downloads/cassandra-with-ssl/ssl';

async function main() {
  console.log('Testing connection to localhost:9042 with SSL...\n');

  const sslOptions = {
    host: '127.0.0.1',
    port: 9042,
    username: 'cassandra',
    password: 'cassandra',
    sslCaFile: `${SSL_DIR}/ca.pem`,
    sslCertfile: `${SSL_DIR}/server.crt.pem`,
    sslKeyfile: `${SSL_DIR}/server.key.pem`,
    sslValidate: false,
  };

  // Test connection first
  const testResult = await CQLSession.testConnection(sslOptions);

  console.log('Test connection result:', JSON.stringify(testResult, null, 2));

  if (!testResult.success) {
    console.error('\nConnection test failed:', testResult.error);
    process.exit(1);
  }

  console.log('\nCluster info:');
  console.log('  Cassandra version:', testResult.data.build);
  console.log('  CQL version:', testResult.data.cql);
  console.log('  Datacenter:', testResult.data.datacenter);

  // Now establish a session
  console.log('\nConnecting session...');
  const connectResult = await CQLSession.connect(sslOptions);

  if (!connectResult.success) {
    console.error('Session connect failed:', connectResult.error);
    process.exit(1);
  }

  const session = connectResult.data;
  console.log('Connected! Cassandra', session.cassandraVersion);

  try {
    // Get session info
    const info = await session.getInfo();
    console.log('\nSession info:', JSON.stringify(info.data, null, 2));

    // Run a simple query
    const result = await session.execute('SELECT cluster_name, release_version FROM system.local');
    if (result.success && result.data.rows) {
      console.log('\nQuery result:');
      console.log('  Columns:', result.data.columns);
      console.log('  Rows:', result.data.rows);
    }

    // List keyspaces
    const ksResult = await session.execute('SELECT keyspace_name FROM system_schema.keyspaces');
    if (ksResult.success && ksResult.data.rows) {
      console.log('\nKeyspaces:', ksResult.data.rows.map(r => r.keyspace_name).join(', '));
    }
  } finally {
    await session.close();
    console.log('\nSession closed.');
  }
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
