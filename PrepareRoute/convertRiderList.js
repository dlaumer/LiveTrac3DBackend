// convert.js
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const inputFile  = path.resolve(__dirname, 'actyvo.csv');
const outputFile = path.resolve(__dirname, 'actyvo.json');

const result = {};

fs.createReadStream(inputFile)
  .pipe(csv())
  .on('data', row => {
    const { Id, FirstName, LastName, Gender, Birthday, Nationality, STARTNUMMER } = row;
    // only include the fields you want
    result[Id] = { FirstName, LastName, Gender, Birthday, Nationality, STARTNUMMER };
  })
  .on('end', () => {
    fs.writeFileSync(outputFile, JSON.stringify(result, null, 2), 'utf8');
    console.log(`✅ Wrote ${Object.keys(result).length} entries to ${outputFile}`);
  })
  .on('error', err => {
    console.error('❌ Error processing CSV:', err);
  });
