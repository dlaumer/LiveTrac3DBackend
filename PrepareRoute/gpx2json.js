#!/usr/bin/env node

/**
 * Usage:
 *   npm install xml2js
 *   node gpx-to-json.js input.gpx [output.json]
 */

const fs = require('fs');
const path = require('path');
const xml2js = require('xml2js');

const [,, inputFile, outputFileArg] = process.argv;

if (!inputFile) {
  console.error('❌ Usage: node gpx-to-json.js input.gpx [output.json]');
  process.exit(1);
}

const outputFile = outputFileArg
  || path.basename(inputFile, path.extname(inputFile)) + '.json';

fs.readFile(inputFile, 'utf8', (err, xml) => {
  if (err) {
    console.error('❌ Error reading GPX:', err);
    process.exit(1);
  }

  const parser = new xml2js.Parser();
  parser.parseString(xml, (err, result) => {
    if (err) {
      console.error('❌ Error parsing GPX XML:', err);
      process.exit(1);
    }

    const coords = [];
    // GPX structure: result.gpx.trk[].trkseg[].trkpt[]
    const tracks = result.gpx.trk || [];
    tracks.forEach(trk => {
      (trk.trkseg || []).forEach(seg => {
        (seg.trkpt || []).forEach(pt => {
          const lon = parseFloat(pt.$.lon);
          const lat = parseFloat(pt.$.lat);
          const ele = pt.ele ? parseFloat(pt.ele[0]) : null;
          coords.push([lon, lat, ele]);
        });
      });
    });

    fs.writeFile(outputFile, JSON.stringify(coords, null, 4), err => {
      if (err) {
        console.error('❌ Error writing JSON:', err);
        process.exit(1);
      }
      console.log(`✅ Converted ${coords.length} points → ${outputFile}`);
    });
  });
});
