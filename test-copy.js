const { CQLSession } = require('.');
const fs = require('fs');

(async () => {
  const r = await CQLSession.connect({ host: '11.11.11.111', username: 'cassandra', password: 'cassandra' });
  if (!r.success) { console.error('Connect failed:', r.error); process.exit(1); }
  const s = r.data;

  // Create a test table
  console.log('--- Setup ---');
  await s.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
  await s.execute('DROP TABLE IF EXISTS test.copy_test');
  await s.execute('CREATE TABLE test.copy_test (id int PRIMARY KEY, name text, score double)');

  // Insert test data
  await s.execute("INSERT INTO test.copy_test (id, name, score) VALUES (1, 'Alice', 95.5)");
  await s.execute("INSERT INTO test.copy_test (id, name, score) VALUES (2, 'Bob', 87.3)");
  await s.execute("INSERT INTO test.copy_test (id, name, score) VALUES (3, 'Charlie', 92.1)");

  // COPY TO - export
  console.log('\n--- COPY TO ---');
  const exportResult = await s.copyTo('test.copy_test', '/tmp/test_roundtrip.csv', { header: true });
  console.log('Export:', JSON.stringify(exportResult, null, 2));

  const csvContent = fs.readFileSync('/tmp/test_roundtrip.csv', 'utf8');
  console.log('\nCSV content:');
  console.log(csvContent);

  // Clear table and import back
  await s.execute('TRUNCATE test.copy_test');
  console.log('Table truncated.');

  // COPY FROM - import
  console.log('\n--- COPY FROM ---');
  const importResult = await s.copyFrom('test.copy_test', '/tmp/test_roundtrip.csv', { header: true });
  console.log('Import:', JSON.stringify(importResult, null, 2));

  // Verify
  console.log('\n--- Verify ---');
  const verify = await s.execute('SELECT * FROM test.copy_test');
  if (verify.success && verify.data) {
    console.log('Row count:', verify.data.row_count || verify.data.length);
    console.log('Data:', JSON.stringify(verify.data.rows || verify.data.data, null, 2));
  } else {
    console.log('Verify result:', JSON.stringify(verify, null, 2));
  }

  await s.close();
  console.log('\nDone!');
})();
