const path = require('path');
const { CQLSession } = require('.');

const cqlshrc = path.join(__dirname, 'test-cqlshrc.rc');

async function main() {
  console.log('Testing connection via cqlshrc file...');
  console.log(`  cqlshrc: ${cqlshrc}\n`);

  // Test connection using cqlshrc
  const testResult = await CQLSession.testConnection({ cqlshrc });

  console.log('Test connection result:', JSON.stringify(testResult, null, 2));

  if (!testResult.success) {
    console.error('\nConnection test failed:', testResult.error);
    process.exit(1);
  }

  console.log('\nCluster info:');
  console.log('  Cassandra version:', testResult.data.build);
  console.log('  CQL version:', testResult.data.cql);
  console.log('  Datacenter:', testResult.data.datacenter);

  // Establish a session using cqlshrc
  console.log('\nConnecting session...');
  const connectResult = await CQLSession.connect({ cqlshrc });

  if (!connectResult.success) {
    console.error('Session connect failed:', connectResult.error);
    process.exit(1);
  }

  const session = connectResult.data;
  console.log('Connected! Cassandra', session.cassandraVersion);

  try {
    const info = await session.getInfo();
    console.log('\nSession info:', JSON.stringify(info.data, null, 2));

    const result = await session.execute('SELECT cluster_name, release_version FROM system.local');
    if (result.success && result.data.rows) {
      console.log('\nQuery result:');
      console.log('  Columns:', result.data.columns);
      console.log('  Rows:', result.data.rows);
    }

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
