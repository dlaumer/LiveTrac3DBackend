const axios = require('axios');
const turf = require('@turf/turf');
const path = require('path');
const fs = require('fs');

const factorCoords = 1e6;
const factorDist = 1e1;

exports.handler = async (event) => {
  try {
    const latestServiceBaseUrl = process.env.LATEST_FEATURE_SERVICE_URL;
    if (!latestServiceBaseUrl) throw new Error("LATEST_FEATURE_SERVICE_URL environment variable is not set");

    const runHistoryServiceBaseUrl = process.env.RUNHISTORY_FEATURE_SERVICE_URL;
    if (!runHistoryServiceBaseUrl) throw new Error("RUNHISTORY_FEATURE_SERVICE_URL environment variable is not set");

    const runHistoryServiceBaseUrlTruncate = process.env.RUNHISTORY_FEATURE_SERVICE_URL_TRUNCATE;
    if (!runHistoryServiceBaseUrlTruncate) throw new Error("RUNHISTORY_FEATURE_SERVICE_URL_TRUNCATE environment variable is not set");

    // Timestamp threshold for deletion 
    const deletionTimestamp = "2025-01-01T12:03:00";

    // Optional token for ArcGIS Feature Services
    const featureServiceToken = process.env.FEATURE_SERVICE_TOKEN;

    // load the keyed JSON { idx: { lat, long, alt, dist, head } }
    const rawRoute = JSON.parse(
      fs.readFileSync(path.join(__dirname, 'route.json'), 'utf8')
    );

    const routeCoords = rawRoute.map(pt => [pt.lat, pt.long]);
    // build a Turf LineString on the fly
    const line = turf.lineString(routeCoords);

    // --- Simulation Setup ---
    const simulationStart = new Date('2025-01-01T12:00:00Z');
    const simulationDurationMs = 3 * 60 * 60 * 1000;
    const now = new Date();
    const simulatedTime = new Date(simulationStart.getTime() + (now.getTime() % simulationDurationMs));
    console.log("Simulated Time:", simulatedTime.toISOString());

    // Round to nearest half-minute for filename
    const epochSec = Math.round(simulatedTime.getTime() / 1000);
    const halfMin = 30;
    const roundedTs = Math.round(epochSec / halfMin) * halfMin;
    const filename = `${roundedTs}.json`;
    const filePath = path.join(__dirname, 'simulation', filename);
    if (!fs.existsSync(filePath)) {
      throw new Error(`Snapshot file not found: ${filename}`);
    }

    const snapshot = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const positions = Object.keys(snapshot.riders).map(rider => {
      return {
        userId: rider,
        _ts: snapshot.riders[rider].timestamp_iso,
        gigId: "SIMULATED_GIG",
        contestId: "SIMULATED_CONTEST",
        runId: "SIMULATED_RUN",
        speed: 0,
        longitude: snapshot.riders[rider].coordinates[0],
        latitude: snapshot.riders[rider].coordinates[1],
        accuracy: 5,
        altitude: snapshot.riders[rider].coordinates[2],
        heading: 0,
        distance: 0,
        activityType: "cycling"
      };
    });

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
    console.log(existingFeatures["rider_1"])

    // Delete old history if needed
    const runHistoryDeleteUrl = `${runHistoryServiceBaseUrlTruncate}/truncate`;

    const config = { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } };
    // 1) Grab a token at runtime
    async function fetchArcGISToken() {
      const params = new URLSearchParams({
        username: process.env.AGOL_USERNAME,
        password: process.env.AGOL_PASSWORD,
        client: 'referer',
        referer: 'https://www.arcgis.com',
        expiration: 60,
        f: 'json'
      }).toString();

      const res = await axios.post(
        'https://www.arcgis.com/sharing/rest/generateToken',
        params,
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
      );
      if (res.data.error) throw new Error(res.data.error.message);
      return res.data.token;
    }

    // 2) Use that token when truncating or deleting
    async function deleteOldFeatures() {
      const token = await fetchArcGISToken();
      const params = new URLSearchParams({
        f: 'json',
        token // <-- your freshly minted admin token
      }).toString();

      return axios.post(runHistoryDeleteUrl, params, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
      });
    }

    if (new Date(deletionTimestamp) > new Date(positions[0]._ts)) {
      deleteOldFeatures().then(info => console.log(info)).catch(err => console.error("Deletion failed:", err));
    }

    // Prepare feature arrays
    const featuresToUpdate = [];
    const featuresToAddLatest = [];
    const featuresToAddRunHistory = [];

    positions.forEach(pos => {
      const snapped = snapToWindow(pos.longitude, pos.latitude, p);
      const time = roundDownToAbsoluteMinutes()
      const f = {
        attributes: {
          userId: pos.userId,
          tsDate: new Date(time * 1000),
          ts: time,
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


    import turf from '@turf/turf';

    /**
     * Snap [lng,lat] to just the window [startIdx…endIdx] of your routeCoords.
     *
     * @param {number} lng
     * @param {number} lat
     * @param {number} routeIndex  index into your routeCoords
     * @param {number} distance    distance along your routeCoords
     */
    function snapToWindow(lng, lat, routeIndex) {

      const startIdx = Math.max(0, routeIndex - 100);
      const endIdx = Math.min(routeCoords.length - 1, routeIndex + 100);
      // 1) slice your coords array (inclusive of both ends)
      const subCoords = routeCoords.slice(startIdx, endIdx + 1);
      const subLine = turf.lineString(subCoords);

      // 2) snap
      const pt = turf.point([Number(lng), Number(lat)]);
      const snapped = turf.nearestPointOnLine(subLine, pt, { units: 'meters' });

      // 3) if it’s outside your radius, take the whole route
      if (snapped.properties.dist > 200) {
        const snapped = turf.nearestPointOnLine(line, pt, { units: 'meters' });
        startIdx = 0;
      };

      // 4) map the returned index back to your full route
      //    snapped.properties.index is the index *into* subCoords
      const localIdx = snapped.properties.index;
      const globalIdx = startIdx + localIdx;

      const alongSubline = snapped.properties.location;  // in meters
      const newDistance = rawRoute[startIdx].dist + alongSubline;

      // 5) compute along-route distance & heading as before
      const entry = rawRoute[globalIdx];

      return {
        coordinates: snapped.geometry.coordinates,
        distanceToRoute: snapped.properties.dist,
        distance: newDistance,
        index: globalIdx,
        heading: entry.head
      };
    }

    // Returns the total number of whole minutes since 1970-01-01T00:00:00Z,
    // rounded down to the last full minute.
    function roundDownToAbsoluteMinutes(date = new Date()) {
      const msPerMinute = 60 * 1000;
      return Math.floor(date.getTime() / msPerMinute) * 60;
    }

    function roundCoords(point) {
      return [Math.round(point[0] * factorCoords) / factorCoords, Math.round(point[1] * factorCoords) / factorCoords];
    }

    function roundDist(dist) {
      return Math.round(dist * factorDist) / factorDist;
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
