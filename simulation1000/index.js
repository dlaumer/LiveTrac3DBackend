const axios = require('axios');
const turf = require('@turf/turf');
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

    const routeCoords = require('./routeSmallCSA.json');

    // Function to snap a point (lng, lat) onto the route polyline using turf.nearestPointOnLine
    function snapPointToRoute(lng, lat) {
      const route = turf.lineString(routeCoords, { name: "route" });
      const point = turf.point([Number(lng), Number(lat)]);
      const snapped = turf.nearestPointOnLine(route, point);
      const segIndex = snapped.properties.index;
      const segment = routeCoords.slice(segIndex, segIndex + 2);
      return {
        snappedPoint: snapped.geometry.coordinates,
        segment,
        index: segIndex
      };
    }

    function roundCoords(point) {
      return [Math.round(point[0] * factorCoords) / factorCoords, Math.round(point[1] * factorCoords) / factorCoords];
    }

    function roundDist(dist) {
      return Math.round(dist * factorDist) / factorDist;
    }

    function extractPathSection(pointA, indexA, pointB, indexB) {
      if (indexA > indexB) {
        [pointA, pointB] = [pointB, pointA];
        [indexA, indexB] = [indexB, indexA];
      }
      const path = [roundCoords(pointA)];
      const cumulative = [0];
      let totalDistance = 0;

      for (let i = indexA + 1; i <= indexB; i++) {
        const next = routeCoords[i];
        const segDist = turf.distance(
          turf.point(path[path.length - 1]),
          turf.point(next),
          { units: "meters" }
        );
        totalDistance += segDist;
        path.push(roundCoords(next));
        cumulative.push(roundDist(totalDistance));
      }

      const lastDist = turf.distance(
        turf.point(path[path.length - 1]),
        turf.point(pointB),
        { units: "meters" }
      );
      totalDistance += lastDist;
      path.push(roundCoords(pointB));
      cumulative.push(roundDist(totalDistance));

      return {
        path: turf.lineString(path),
        cumulative
      };
    }

    // --- Simulation Setup ---
    const simulationStart = new Date('2025-01-01T12:00:00Z');
    const simulationDurationMs = 3 * 60 * 60 * 1000;
    const now = new Date();
    const simulatedTime = new Date(simulationStart.getTime() + (now.getTime() % simulationDurationMs));
    console.log("Simulated Time:", simulatedTime.toISOString());

    function getLatestPosition(riderData, simulatedTime) {
      const timestamps = Object.keys(riderData);
      let latest = timestamps[0];
      for (const t of timestamps) {
        if (new Date(t * 1000) <= simulatedTime) latest = t;
        else break;
      }
      return { timestamp: riderData[latest].timestamp_iso, coords: riderData[latest].coordinates };
    }

    const simulatedData = require('./bike_race_simulation_1000_normal.json');
    const positions = Object.keys(simulatedData.riders).map(rider => {
      const pos = getLatestPosition(simulatedData.riders[rider], simulatedTime);
      return {
        userId: rider,
        _ts: pos.timestamp,
        gigId: "SIMULATED_GIG",
        contestId: "SIMULATED_CONTEST",
        runId: "SIMULATED_RUN",
        speed: 0,
        longitude: pos.coords[0],
        latitude: pos.coords[1],
        accuracy: 5,
        altitude: pos.coords[2],
        heading: 0,
        distance: 0,
        activityTy: "cycling"
      };
    });

    // Retrieve existing features
    const userIds = positions.map(p => p.userId);
    const uniqueUserIds = Array.from(new Set(userIds));
    const latestQueryUrl = `${latestServiceBaseUrl}/query`;
    const queryParams = { where: "1=1", outFields: "*", f: "json" };
    if (featureServiceToken) queryParams.token = featureServiceToken;
    const queryResult = await axios.get(latestQueryUrl, { params: queryParams });
    const userIdToObjectId = {};
    (queryResult.data.features || []).forEach(feat => {
      if (feat.attributes?.userId) {
        userIdToObjectId[feat.attributes.userId] = {
          ts: feat.attributes.ts_string,
          objectId: feat.attributes.FID,
          attributes: feat.attributes
        };
      }
    });

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
      const snapped = snapPointToRoute(pos.longitude, pos.latitude);
      const f = {
        attributes: {
          userId: pos.userId,
          ts: new Date(pos._ts),
          ts_string: pos._ts,
          gigId: pos.gigId,
          contestId: pos.contestId,
          runId: pos.runId,
          speed: pos.speed,
          longitude: snapped.snappedPoint[0],
          latitude: snapped.snappedPoint[1],
          accuracy: pos.accuracy,
          altitude: pos.altitude,
          heading: pos.heading,
          distance: pos.distance,
          activityTy: pos.activityTy,
          routeIndex: snapped.index
        },
        geometry: {
          x: snapped.snappedPoint[0],
          y: snapped.snappedPoint[1],
          z: pos.altitude,
          spatialReference: { wkid: 4326 }
        }
      };

      if (userIdToObjectId[pos.userId]) {
        const prev = userIdToObjectId[pos.userId];
        if (prev.ts !== pos._ts) {
          const section = extractPathSection(
            [prev.attributes.longitude, prev.attributes.latitude],
            prev.attributes.routeIndex,
            [f.attributes.longitude, f.attributes.latitude],
            f.attributes.routeIndex
          );
          const dt = new Date(pos._ts) - new Date(prev.ts);

          prev.attributes.previousPos = null;
          if (Math.abs(dt) > 3 * 60 * 1000) {
            // dt is larger than 3 minutes
            f.attributes.path = "";
            f.attributes.cumulative = "";
            f.attributes.previousPos = "";
            f.attributes.speed = 0;
          } else {
            f.attributes.path = JSON.stringify(section.path);
            f.attributes.cumulative = JSON.stringify(section.cumulative);
            f.attributes.previousPos = JSON.stringify(prev.attributes);
            f.attributes.speed = section.cumulative.slice(-1)[0] / dt * 3600;
          }

          //console.log(Math.max(f.attributes.previousPos.length, f.attributes.path.length, f.attributes.cumulative.length))
          f.attributes.FID = prev.objectId;

          featuresToUpdate.push(f);
          featuresToAddRunHistory.push(f);
        }
      } else {
        featuresToAddLatest.push(f);
        featuresToAddRunHistory.push(f);
      }
    });

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
