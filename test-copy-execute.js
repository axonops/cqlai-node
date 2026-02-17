const { CQLSession } = require('.');
const fs = require('fs');

(async () => {
  const r = await CQLSession.connect({ host: '11.11.11.111', username: 'cassandra', password: 'cassandra' });
  if (!r.success) { console.error('Connect failed:', r.error); process.exit(1); }
  const s = r.data;

  // Setup: create and populate a test table
  console.log('--- Setup ---');
  await s.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
  await s.execute('DROP TABLE IF EXISTS test.copy_src');
  await s.execute('CREATE TABLE test.copy_src (id int PRIMARY KEY, name text, score double)');
  await s.execute("INSERT INTO test.copy_src (id, name, score) VALUES (1, 'Alice', 95.5)");
  await s.execute("INSERT INTO test.copy_src (id, name, score) VALUES (2, 'Bob', 87.3)");
  await s.execute("INSERT INTO test.copy_src (id, name, score) VALUES (3, 'Charlie', 92.1)");

  // COPY TO via execute()
  console.log('\n--- COPY TO via execute() ---');
  const exportResult = await s.execute("COPY test.copy_src TO '/tmp/copy_roundtrip.csv' WITH HEADER = true;");
  console.log('Result:', exportResult.data.message);
  console.log('CSV:\n' + fs.readFileSync('/tmp/copy_roundtrip.csv', 'utf8'));

  // COPY FROM via execute() - same schema table
  await s.execute('DROP TABLE IF EXISTS test.copy_dst');
  await s.execute('CREATE TABLE test.copy_dst (id int PRIMARY KEY, name text, score double)');

  console.log('--- COPY FROM via execute() ---');
  const importResult = await s.execute("COPY test.copy_dst FROM '/tmp/copy_roundtrip.csv' WITH HEADER = true;");
  console.log('Result:', importResult.data.message);

  // Verify
  console.log('\n--- Verify ---');
  const verify = await s.execute('SELECT * FROM test.copy_dst');
  console.log('Rows:', JSON.stringify(verify.data.rows, null, 2));

  await s.close();
  console.log('\nDone!');
})();
