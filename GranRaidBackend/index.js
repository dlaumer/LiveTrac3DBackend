const axios = require('axios');
const turf = require('@turf/turf');
const path = require('path');
const fs = require('fs');

const factorCoords = 1e6;
const factorDist = 1e1;

exports.handler = async (event) => {
  try {
    // --- PREPARATION ---
    // Retrieve required environment variables
    const apiKey = process.env.API_KEY;
    if (!apiKey) throw new Error("API_KEY environment variable is not set");

    const latestServiceBaseUrl = process.env.LATEST_FEATURE_SERVICE_URL;
    if (!latestServiceBaseUrl) throw new Error("LATEST_FEATURE_SERVICE_URL environment variable is not set");

    const runHistoryServiceBaseUrl = process.env.RUNHISTORY_FEATURE_SERVICE_URL;
    if (!runHistoryServiceBaseUrl) throw new Error("RUNHISTORY_FEATURE_SERVICE_URL environment variable is not set");

    // Optional token for ArcGIS Feature Services
    const featureServiceToken = process.env.FEATURE_SERVICE_TOKEN;

    // load the keyed JSON { idx: { lat, long, alt, dist, head } }
    const rawRoute = JSON.parse(
      fs.readFileSync(path.join(__dirname, 'route.json'), 'utf8')
    );

    // turn it into a sorted array of [lon, lat]

    // rawRoute is now an Array of { lat, long, alt, dist, head, … }
    const routeCoords = rawRoute.map(pt => [pt.lat, pt.long]);

    // build a Turf LineString on the fly
    const line = turf.lineString(routeCoords);


    // --- GET DATA ---
    // The positions API URL (update with your actual API endpoint)
    const positionsApiUrl = 'https://out.stapp.actyvo.app/api/positions/latest?after=2025-03-18T15%3A00%3A00Z&gigId=ACTYVO_CNC&contestId=e99662f41b3248758e515f7f3c54b3a0&code=';

    // Call the positions API using the API key in the header
    const response = await axios.get(positionsApiUrl, {
      headers: { 'x-functions-key': apiKey }
    });

    // Assume the API returns data like: { positions: [ { userId, timestamp, lat, lng, ... }, ... ] }
    const positions = response.data;
    if (!positions || !Array.isArray(positions)) {
      throw new Error("Positions data is not available or invalid");
    }

    // --- GET EXISTING DATA ---
    // Retrieve existing features
    const userIds = positions.map(p => p.userId);
    const uniqueUserIds = Array.from(new Set(userIds));
    const latestQueryUrl = `${latestServiceBaseUrl}/query`;
    const queryParams = { where: "1=1", outFields: "OBJECTID,userId,ts,distance,routeIndex", f: "json" };
    if (featureServiceToken) queryParams.token = featureServiceToken;
    const queryResult = await axios.get(latestQueryUrl, { params: queryParams });
    const existingFeatures = {};
    (queryResult.data.features || []).forEach(feat => {
      if (feat.attributes?.userId) {
        existingFeatures[feat.attributes.userId] = {
          OBJECTID: feat.attributes.OBJECTID,
          userId: feat.attributes.userId,
          ts: feat.attributes.ts,
          distance: feat.attributes.distance,
          routeIndex: feat.attributes.routeIndex
        };
      }
    });


    // --- PROCESS ---
    // Prepare feature arrays
    const featuresToUpdate = [];
    const featuresToAddLatest = [];
    const featuresToAddRunHistory = [];

    positions.forEach(pos => {
      const snapped = snapPointToRoute(pos.longitude, pos.latitude);
      const f = {
        attributes: {
          userId: pos.userId,
          tsDate: new Date(pos._ts),
          ts: Math.floor(new Date(pos._ts).getTime() / 1000),
          gigId: pos.gigId,
          contestId: pos.contestId,
          runId: pos.runId,
          speed: pos.speed,
          altitude: pos.altitude,
          accuracy: pos.accuracy,
          activityType: pos.activityType,
          longitude: snapped.coordinates[0],
          latitude: snapped.coordinates[1],
          heading: snapped.heading,
          distance: snapped.distance,
          routeIndex: parseInt(snapped.index, 10)
        },
        geometry: {
          x: snapped.coordinates[0],
          y: snapped.coordinates[1],
          z: pos.altitude,
          spatialReference: { wkid: 4326 }
        }
      };

      if (existingFeatures[pos.userId]) {
        const previousPos = existingFeatures[pos.userId];
        if (previousPos.ts !== f.attributes.ts) {


          f.attributes.previousDistance = previousPos.distance;
          f.attributes.previousRouteIndex = previousPos.routeIndex;
          f.attributes.previousTs = previousPos.ts;

          // 2) compute speed [km/h] from delta-distance & delta-time
          const dtMs = f.attributes.ts - previousPos.ts;
          const dMeters = f.attributes.distance - previousPos.distance;
          f.attributes.speed = dtMs > 0
            ? (dMeters / (dtMs)) * 3.6   // m/s → km/h
            : 0;
          f.attributes.OBJECTID = previousPos.OBJECTID;

          featuresToUpdate.push(f);
        }
      } else {
        featuresToAddLatest.push(f);
      }

      featuresToAddRunHistory.push(f);

    });

    /**
     * Snap an arbitrary [lng,lat] to our pre-computed route.
     * Returns:
     *   - coordinates: [lng, lat] of the snapped point
     *   - distance:     exact cumulative meters (from route.json)
     *   - index:        integer index into route.json
     *   - heading:      integer bearing at that segment (0–359°)
     */
    function snapPointToRoute(lng, lat) {

      const point = turf.point([Number(lng), Number(lat)]);
      const snapped = turf.nearestPointOnLine(line, point, { units: 'meters' });

      const idx = snapped.properties.index;
      const entry = rawRoute[idx];

      return {
        coordinates: snapped.geometry.coordinates,
        distance: entry.dist,
        index: idx,
        heading: entry.head
      };
    }

    // --- Batching utilities ---
    function chunkArray(arr, size) {
      const chunks = [];
      for (let i = 0; i < arr.length; i += size) {
        chunks.push(arr.slice(i, i + size));
      }
      return chunks;
    }

    async function postFeatures(url, features) {
      const params = {
        features: JSON.stringify(features),
        f: "json"
      };
      if (featureServiceToken) params.token = featureServiceToken;
      const cfg = { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } };
      const body = new URLSearchParams(params).toString();
      return axios.post(url, body, cfg);
    }

    async function postFeaturesInBatches(url, allFeatures, batchSize = 1000) {
      const results = [];
      const batches = chunkArray(allFeatures, batchSize);
      for (const batch of batches) {
        const res = await postFeatures(url, batch);
        results.push(res.data);
      }
      return results;
    }

    // --- Send updates in batches ---
    let updateResponses = [];
    if (featuresToUpdate.length) {
      const url = `${latestServiceBaseUrl}/updateFeatures`;
      updateResponses = await postFeaturesInBatches(url, featuresToUpdate);
    }

    let addLatestResponses = [];
    if (featuresToAddLatest.length) {
      const url = `${latestServiceBaseUrl}/addFeatures`;
      addLatestResponses = await postFeaturesInBatches(url, featuresToAddLatest);
    }

    let addRunHistoryResponses = [];
    if (featuresToAddRunHistory.length) {
      const url = `${runHistoryServiceBaseUrl}/addFeatures`;
      addRunHistoryResponses = await postFeaturesInBatches(url, featuresToAddRunHistory);
    }


    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Simulated positions updated and history added",
        simulatedTime: simulatedTime.toISOString(),
        latestUpdates: {
          updatedCount: featuresToUpdate.length,
          addedCount: featuresToAddLatest.length,
          historyAddedCount: featuresToAddRunHistory.length,
        },
        updateResponses,
        addLatestResponses,
        addRunHistoryResponses
      })
    };
  } catch (error) {
    console.error('Error processing simulated data:', error.message);
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};
