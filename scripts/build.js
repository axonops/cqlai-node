#!/usr/bin/env node

const { execSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');

const rootDir = path.join(__dirname, '..');
const goDir = path.join(rootDir, 'go');
const libDir = path.join(rootDir, 'lib');

// Determine library filename based on platform
function getLibraryName() {
  switch (process.platform) {
    case 'win32':
      return 'cqlai.dll';
    case 'darwin':
      return 'libcqlai.dylib';
    default:
      return 'libcqlai.so';
  }
}

const outputLib = path.join(libDir, getLibraryName());

// Ensure lib directory exists
if (!fs.existsSync(libDir)) {
  fs.mkdirSync(libDir, { recursive: true });
}

// Check if Go is installed
try {
  execSync('go version', { stdio: 'pipe' });
} catch (err) {
  console.error('Error: Go is not installed or not in PATH');
  console.error('Please install Go from https://golang.org/dl/');
  process.exit(1);
}

console.log('Building native library...');
console.log(`  Source: ${goDir}`);
console.log(`  Output: ${outputLib}`);

try {
  // Build the shared library from Go source
  execSync(
    `go build -buildmode=c-shared -o "${outputLib}" ./bindings/`,
    {
      cwd: goDir,
      stdio: 'inherit',
      env: {
        ...process.env,
        CGO_ENABLED: '1',
      },
    }
  );

  // Verify the library was created
  if (fs.existsSync(outputLib)) {
    const stats = fs.statSync(outputLib);
    console.log(`\nBuild successful!`);
    console.log(`  Library size: ${(stats.size / 1024 / 1024).toFixed(2)} MB`);
  } else {
    console.error('Error: Library was not created');
    process.exit(1);
  }
} catch (err) {
  console.error('Build failed:', err.message);
  process.exit(1);
}
