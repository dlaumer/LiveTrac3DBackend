#!/usr/bin/env node
const fs = require('fs').promises;
const path = require('path');

/**
 * Simulate irregularities on a series of half-minute JSON snapshots.
 *
 * @param {string} inputDir  - Directory containing your raw .json files
 * @param {string} outputDir - Directory where modified files will be written
 * @param {object} [options]
 * @param {number} [options.dropEventCount=5]     - How many drop-events to schedule
 * @param {number} [options.maxDropMinutes=3]      - Max duration (in minutes) for any drop-event
 */
async function simulateIrregularities(inputDir, outputDir, options = {}) {
  const {
    dropEventCount = 5,
    maxDropMinutes    = 3,
  } = options;

  // 1. Ensure output directory exists
  await fs.mkdir(outputDir, { recursive: true });

  // 2. Load and sort all JSON filenames (assumes names are epoch seconds + .json)
  let files = await fs.readdir(inputDir);
  files = files
    .filter(f => f.endsWith('.json'))
    .sort((a, b) => parseInt(a, 10) - parseInt(b, 10));

  if (!files.length) {
    throw new Error(`No .json files found in ${inputDir}`);
  }

  // 3. Collect all rider IDs across all files
  const riderSet = new Set();
  for (const file of files) {
    const data = JSON.parse(await fs.readFile(path.join(inputDir, file), 'utf8'));
    Object.keys(data.riders || {}).forEach(r => riderSet.add(r));
  }
  const riders = Array.from(riderSet);

  // 4. Schedule drop-events
  //    Each event picks one rider, a random start index, and a random duration (in files)
  const dropEvents = [];
  const slotsPerMinute = 2; // half-minute per slot
  for (let i = 0; i < dropEventCount; i++) {
    const rider     = riders[Math.floor(Math.random() * riders.length)];
    const startIdx  = Math.floor(Math.random() * (files.length - slotsPerMinute * maxDropMinutes));
    const duration  = Math.ceil(Math.random() * (slotsPerMinute * maxDropMinutes));
    dropEvents.push({ rider, startIdx, endIdx: startIdx + duration });
  }

  // 5. Process each file: jitter timestamps and apply drop-events
  for (let idx = 0; idx < files.length; idx++) {
    const filePath = path.join(inputDir, files[idx]);
    const data     = JSON.parse(await fs.readFile(filePath, 'utf8'));

    // 5a. Jitter each rider’s timestamp_iso by ±30s
    for (const [riderId, info] of Object.entries(data.riders || {})) {
      const original = new Date(info.timestamp_iso).getTime();
      const jitter   = (Math.random() - 0.5) * 60 * 1000; // -30kms … +30kms
      info.timestamp_iso = new Date(original + jitter).toISOString();
    }

    // 5b. Remove riders for any active drop-event at this index
    for (const ev of dropEvents) {
      if (idx >= ev.startIdx && idx < ev.endIdx) {
        delete data.riders[ev.rider];
      }
    }

    // 5c. Write out the modified snapshot
    await fs.writeFile(
      path.join(outputDir, files[idx]),
      JSON.stringify(data, null, 2),
      'utf8'
    );
  }

  console.log(`Simulation complete: ${files.length} files → ${outputDir}`);
}

// CLI invocation: node simulate.js ./raw ./irregular
if (require.main === module) {
  const [,, inputDir, outputDir] = process.argv;
  if (!inputDir || !outputDir) {
    console.error('Usage: node simulate.js <inputDir> <outputDir>');
    process.exit(1);
  }
  simulateIrregularities(inputDir, outputDir)
    .catch(err => {
      console.error('Error during simulation:', err);
      process.exit(1);
    });
}

module.exports = simulateIrregularities;
