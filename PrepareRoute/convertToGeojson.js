// save as convertToGeoJSON.js
const fs = require('fs');
const path = require('path');

// usage: node convertToGeoJSON.js <input.json> [output.geojson]
const [,, inFile = 'RouteAC.json', outFile = 'RouteAC.geojson'] = process.argv;

(async () => {
  try {
    // 1. Read & parse your raw array of points
    const raw = await fs.promises.readFile(path.resolve(inFile), 'utf8');
    const pts = JSON.parse(raw);
    if (!Array.isArray(pts)) throw new Error('Expected top-level JSON array');

    // 2. Normalize to [lon, lat, elev]
    const coords = pts.map((p, i) => {
      if (!Array.isArray(p) || p.length < 3) {
        throw new Error(`Point at index ${i} is not [lon,lat,elev,…]`);
      }
      const [lon, lat, elev] = p;
      return [lon, lat, elev];
    });

    // 3. Build a GeoJSON FeatureCollection with one LineString
    const geojson = {
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          properties: {},
          geometry: {
            type: "LineString",
            coordinates: coords
          }
        }
      ]
    };

    // 4. Write it out
    await fs.promises.writeFile(
      path.resolve(outFile),
      JSON.stringify(geojson, null, 2),
      'utf8'
    );
    console.log(`✓ Wrote GeoJSON with ${coords.length} points to ${outFile}`);
  } catch (err) {
    console.error('✗ Error:', err.message || err);
    process.exit(1);
  }
})();
