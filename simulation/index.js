const axios = require('axios');
const turf = require('@turf/turf');

exports.handler = async (event) => {
  try {
    const latestServiceBaseUrl = process.env.LATEST_FEATURE_SERVICE_URL;
    if (!latestServiceBaseUrl) throw new Error("LATEST_FEATURE_SERVICE_URL environment variable is not set");

    const runHistoryServiceBaseUrl = process.env.RUNHISTORY_FEATURE_SERVICE_URL;
    if (!runHistoryServiceBaseUrl) throw new Error("RUNHISTORY_FEATURE_SERVICE_URL environment variable is not set");

    // Timestamp threshold for deletion 
    const deletionTimestamp = "2025-01-01T12:02:00";

    // Optional token for ArcGIS Feature Services
    const featureServiceToken = process.env.FEATURE_SERVICE_TOKEN;

    const routeCoords = require('./route.json');


    // Function to snap a point (lng, lat) onto the route polyline using turf.nearestPointOnLine
    function snapPointToRoute(lng, lat) {

      var route = turf.lineString(
        routeCoords,
        { name: "route" },
      );

      lng = Number(lng);
      lat = Number(lat);
      // Create a turf point from the given coordinates
      const point = turf.point([lng, lat]);
      // Calculate the nearest point on the route
      const snapped = turf.nearestPointOnLine(route, point);
      // Identify the segment by taking the two vertices from the route.
      const segIndex = snapped.properties.index;
      const segment = routeCoords.slice(segIndex, segIndex + 2);

      return {
        snappedPoint: snapped.geometry.coordinates,
        segment: segment,
        index: segIndex
      };
    }

    function extractPathSection(pointA, indexA, pointB, indexB) {

      // Ensure proper ordering: indexA should be lower than indexB.
      if (indexA > indexB) {
        [pointA, pointB] = [pointB, pointA];
        [indexA, indexB] = [indexB, indexA];
      }

      // Build the path:
      // Start with the snapped coordinate from snapA.
      const path = [pointA];
      const cumulative = [0];
      let totalDistance = 0;

      // Append all route vertices from after snapA's segment up to snapB's segment.
      // For example, if snapA lies between routeCoords[indexA] and routeCoords[indexA+1],
      // then add routeCoords[indexA+1] to routeCoords[indexB].
      for (let i = indexA + 1; i <= indexB; i++) {

        const nextCoord = routeCoords[i];
        const segmentDistance = turf.distance(turf.point(path[path.length - 1]), turf.point(nextCoord), { units:"meters" });
        totalDistance += segmentDistance;
        path.push(nextCoord);
        cumulative.push(totalDistance);
      }

      // Finally, add the exact snapped coordinate from snapB.
      // Finally, add the snapped coordinate from snapB.
      const lastSegmentDistance = turf.distance(turf.point(path[path.length - 1]), turf.point(pointB), { units:"meters" });
      totalDistance += lastSegmentDistance;
      path.push(pointB);
      cumulative.push(totalDistance);

      // Return the extracted section as a GeoJSON LineString.
      return {
        path: turf.lineString(path),
        cumulative: cumulative
      };
    }

    // --- Simulation Setup ---
    // The simulation data covers a two-hour period on January 1, 2025.
    // We map the actual time into this two-hour period using modulo arithmetic.
    const simulationStart = new Date('2025-01-01T12:00:00Z'); // simulation begins here
    const simulationDurationMs = 2 * 60 * 60 * 1000; // two hours in milliseconds
    const now = new Date();
    const simulatedTime = new Date(simulationStart.getTime() + (now.getTime() % simulationDurationMs));
    console.log("Simulated Time:", simulatedTime.toISOString());

    // Utility function to get the latest position for a rider up to the simulated time
    function getLatestPosition(riderData, simulatedTime) {
      const timestamps = Object.keys(riderData).sort(); // ISO timestamps sort correctly
      let latestTimestamp = timestamps[0];
      for (const t of timestamps) {
        if (new Date(t) <= simulatedTime) {
          latestTimestamp = t;
        } else {
          break;
        }
      }
      return { timestamp: latestTimestamp, coords: riderData[latestTimestamp] };
    }

    const simulatedData = require('./simulated_bike_riders_final.json');

    // Build the positions array by simulating for each rider
    const positions = [];
    for (const rider in simulatedData.riders) {
      const posData = getLatestPosition(simulatedData.riders[rider], simulatedTime);
      positions.push({
        userId: rider,
        _ts: posData.timestamp,
        gigId: "SIMULATED_GIG",
        contestId: "SIMULATED_CONTEST",
        runId: "SIMULATED_RUN",
        speed: 0,       // default or simulated value
        longitude: posData.coords[0],
        latitude: posData.coords[1],
        accuracy: 5,    // arbitrary default
        altitude: posData.coords[2],
        heading: 0,     // arbitrary default
        distance: 0,    // arbitrary default
        activityTy: "cycling"
      });
    }

    // --- The rest of the processing is similar to the original ---
    // Retrieve the unique user IDs
    const userIds = positions.map(pos => pos.userId);
    const uniqueUserIds = Array.from(new Set(userIds));
    console.log("Unique userIds:", uniqueUserIds);

    // Build query URL for the latest service
    const latestQueryUrl = `${latestServiceBaseUrl}/query`;
    const queryParams = {
      where: `userId IN ('${uniqueUserIds.join("','")}')`,
      outFields: "*",
      f: "json"
    };
    if (featureServiceToken) queryParams.token = featureServiceToken;

    const queryResult = await axios.get(latestQueryUrl, { params: queryParams });
    // Map userId to OBJECTID and ts for existing features
    const userIdToObjectId = {};
    if (queryResult.data.features && Array.isArray(queryResult.data.features)) {
      queryResult.data.features.forEach(feature => {
        if (feature.attributes && feature.attributes.userId) {
          userIdToObjectId[feature.attributes.userId] = { ts: feature.attributes.ts_string, objectId: feature.attributes.FID, attributes: feature.attributes };
        }
      });
    }

    // Construct the URL for deletion 
    const runHistoryDeleteUrl = `${runHistoryServiceBaseUrl}/deleteFeatures`;

    // Build the parameters for the deletion request 
    const deleteParams = { where: "1=1", f: "json" }; if (featureServiceToken) { deleteParams.token = featureServiceToken; }

    const config = { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } };

    async function deleteOldFeatures() {
      try {
        // Convert parameters into URL-encoded string 
        const paramsString = new URLSearchParams(deleteParams).toString();
        const response = await axios.post(runHistoryDeleteUrl, paramsString, config);
        console.log("Delete response:", response.data); return response.data;
      }
      catch (error) { console.error("Error deleting features:", error.message); throw error; }
    }

    if (new Date(deletionTimestamp) > new Date(positions[0]._ts)) {
      // Execute the function
      deleteOldFeatures()
        .then((result) => {
          console.log("Deletion completed successfully:", result);
        })
        .catch((err) => {
          console.error("Deletion failed:", err);
        });
    }

    // Prepare features for update (if existing) and for addition (if new)
    const featuresToUpdate = [];
    const featuresToAddLatest = [];
    const featuresToAddRunHistory = [];
    positions.forEach(pos => {

      const snapped = snapPointToRoute(pos.longitude, pos.latitude);
      const snappedCoords = snapped.snappedPoint;

      const feature = {
        attributes: {
          userId: pos.userId,
          ts: new Date(pos._ts),
          ts_string: pos._ts,
          gigId: pos.gigId,
          contestId: pos.contestId,
          runId: pos.runId,
          speed: pos.speed,
          longitude: snappedCoords[0],
          latitude: snappedCoords[1],
          accuracy: pos.accuracy,
          altitude: pos.altitude,
          heading: pos.heading,
          distance: pos.distance,
          activityTy: pos.activityType,
          routeIndex: snapped.index,
        },
        geometry: {
          x: snappedCoords[0],
          y: snappedCoords[1],
          z: pos.altitude,
          spatialReference: { wkid: 4326 }
        }
      };

      if (userIdToObjectId.hasOwnProperty(pos.userId)) {
        if (userIdToObjectId[pos.userId].ts_string != pos._ts) {
          const previousPoint = [userIdToObjectId[pos.userId].attributes.longitude, userIdToObjectId[pos.userId].attributes.latitude]
          const previousIndex = userIdToObjectId[pos.userId].attributes.routeIndex
          const pathSection = extractPathSection(previousPoint, previousIndex, [feature.attributes.longitude, feature.attributes.latitude], feature.attributes.routeIndex)
          const path = pathSection.path;
          const cumulative = pathSection.cumulative;
          const timeDiff = new Date(pos._ts) - new Date(userIdToObjectId[pos.userId].attributes.ts);
          
          console.log(cumulative[cumulative.length - 1] / timeDiff * 3600)

          userIdToObjectId[pos.userId].attributes.previousPos = null;
          feature.attributes.speed = cumulative[cumulative.length - 1] / timeDiff * 3600; 
          feature.attributes.path = JSON.stringify(path);
          feature.attributes.cumulative = JSON.stringify(cumulative); 
          feature.attributes.previousPos = JSON.stringify(userIdToObjectId[pos.userId].attributes);
          featuresToAddRunHistory.push(feature);
          
          feature.attributes.FID = userIdToObjectId[pos.userId].objectId;
          featuresToUpdate.push(feature);
        }
      } else {
        featuresToAddLatest.push(feature);
        featuresToAddRunHistory.push(feature);
      }
    });

    // Utility function to post features (for add/update operations)
    async function postFeatures(url, features) {
      const params = {
        features: JSON.stringify(features),
        f: "json"
      };
      if (featureServiceToken) params.token = featureServiceToken;
      const config = {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
      };
      const paramsString = new URLSearchParams(params).toString();
      return axios.post(url, paramsString, config);
    }

    // Update existing features in the latest service
    let updateResponse = null;
    if (featuresToUpdate.length > 0) {
      const latestUpdateUrl = `${latestServiceBaseUrl}/updateFeatures`;
      updateResponse = await postFeatures(latestUpdateUrl, featuresToUpdate);
    }

    // Add new features to the latest service
    let addLatestResponse = null;
    if (featuresToAddLatest.length > 0) {
      const latestAddUrl = `${latestServiceBaseUrl}/addFeatures`;
      addLatestResponse = await postFeatures(latestAddUrl, featuresToAddLatest);
    }

    // Add features to the runHistory service
    let addRunHistoryResponse = null;
    if (featuresToAddRunHistory.length > 0) {
      const runHistoryAddUrl = `${runHistoryServiceBaseUrl}/addFeatures`;
      addRunHistoryResponse = await postFeatures(runHistoryAddUrl, featuresToAddRunHistory);
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Simulated positions updated and history added",
        simulatedTime: simulatedTime.toISOString(),
        latestUpdates: {
          updatedCount: featuresToUpdate.length,
          addedCount: featuresToAddLatest.length,
          addedCountHistory: featuresToAddRunHistory.length
        },
        updateResponse: updateResponse ? updateResponse.data : null,
        addLatestResponse: addLatestResponse ? addLatestResponse.data : null,
        addRunHistoryResponse: addRunHistoryResponse ? addRunHistoryResponse.data : null
      })
    }
  }
  catch (error) { console.error('Error processing simulated data:', error.message); return { statusCode: 500, body: JSON.stringify({ error: error.message }) }; }
};

