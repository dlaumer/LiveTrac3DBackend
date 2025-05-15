// save as convertRouteObjectToArray.js
// Usage: node convertRouteObjectToArray.js <input.json> [output.json]

const fs = require('fs');
const path = require('path');

const [,, inFile = 'RouteAC_cumulative.json', outFile = 'RouteAC_cumulative_array.json'] = process.argv;

(async () => {
  try {
    // Read and parse the input object
    const raw = await fs.promises.readFile(path.resolve(inFile), 'utf8');
    const obj = JSON.parse(raw);

    // Convert object of { "0": {...}, "1": {...}, ... } to sorted array
    const arr = Object.keys(obj)
      .map(key => ({ idx: Number(key), data: obj[key] }))
      .sort((a, b) => a.idx - b.idx)
      .map(item => item.data);

    // Write out the resulting array
    await fs.promises.writeFile(
      path.resolve(outFile),
      JSON.stringify(arr, null, 2),
      'utf8'
    );

    console.log(`✓ Wrote ${arr.length} entries to ${outFile}`);
  } catch (err) {
    console.error('Error converting object to array:', err);
    process.exit(1);
  }
})();
