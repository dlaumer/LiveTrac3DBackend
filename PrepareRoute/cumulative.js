// save as calcCumulativeWithHeadingArray.js
const fs = require('fs');
const path = require('path');

// usage: node calcCumulativeWithHeadingArray.js <input.json> [output.json]
const [, , inFile = 'GR60.json', outFile = 'GR60_cumulative.json'] = process.argv;

// constant to convert degrees → meters at the equator
const DEG_TO_M = 111_319.9;

/**
 * approxDist: fast planar distance between two [lon, lat] points.
 *  - φ = avg latitude in radians
 *  - x = Δlon ⋅ cos(φ)
 *  - y = Δlat
 *  - returns meters via Pythagoras
 */
function approxDist([lon1, lat1], [lon2, lat2]) {
  const φ = ((lat1 + lat2) / 2) * (Math.PI / 180);
  const x = (lon2 - lon1) * Math.cos(φ);
  const y = (lat2 - lat1);
  return DEG_TO_M * Math.sqrt(x * x + y * y);
}

/**
 * computeBearing: heading from [lon1, lat1] → [lon2, lat2] in degrees.
 * Uses the “forward azimuth” formula and normalizes to [0,360).
 */
function computeBearing([lon1, lat1], [lon2, lat2]) {
  const φ1 = lat1 * Math.PI / 180;
  const φ2 = lat2 * Math.PI / 180;
  const Δλ = (lon2 - lon1) * Math.PI / 180;
  const y = Math.sin(Δλ) * Math.cos(φ2);
  const x = Math.cos(φ1) * Math.sin(φ2)
          - Math.sin(φ1) * Math.cos(φ2) * Math.cos(Δλ);
  const θ = Math.atan2(y, x);
  return (θ * 180/Math.PI + 360) % 360;
}

(async function() {
  try {
    const raw = await fs.promises.readFile(path.resolve(inFile), 'utf8');
    const pts = JSON.parse(raw);
    let total = 0;
    const out = [];
    const seen = new Set();

    for (let i = 0; i < pts.length; i++) {
      const [lon, lat, elev] = pts[i];

      // accumulate distance from previous point
      if (i > 0) {
        total += approxDist(
          [pts[i-1][0], pts[i-1][1]],
          [lon, lat]
        );
      }

      // round to nearest full meter
      const dist = Math.round(total);

      // skip duplicate distances
      if (seen.has(dist)) continue;
      seen.add(dist);

      // determine heading: use next segment for first point, otherwise current
      const refIdx = (i === 0 ? 1 : i);
      const headRaw = computeBearing(
        [pts[refIdx - 1][0], pts[refIdx - 1][1]],
        [pts[refIdx][0], pts[refIdx][1]]
      );
      const head = Math.round(headRaw);

      out.push({
        lat:  lon,
        long: lat,
        alt:  elev,
        dist,
        head
      });
    }

    await fs.promises.writeFile(
      path.resolve(outFile),
      JSON.stringify(out, null, 2),
      'utf8'
    );
    console.log(`✓ Wrote ${out.length} unique-distance points to ${outFile}`);
  } catch (err) {
    console.error('✗ Error:', err);
    process.exit(1);
  }
})();
