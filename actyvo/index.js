const axios = require('axios');

exports.handler = async (event) => {
  try {
    // Retrieve required environment variables
    const apiKey = process.env.API_KEY;
    if (!apiKey) throw new Error("API_KEY environment variable is not set");

    const latestServiceBaseUrl = process.env.LATEST_FEATURE_SERVICE_URL;
    if (!latestServiceBaseUrl) throw new Error("LATEST_FEATURE_SERVICE_URL environment variable is not set");

    const runHistoryServiceBaseUrl = process.env.RUNHISTORY_FEATURE_SERVICE_URL;
    if (!runHistoryServiceBaseUrl) throw new Error("RUNHISTORY_FEATURE_SERVICE_URL environment variable is not set");

    // Optional token for ArcGIS Feature Services
    const featureServiceToken = process.env.FEATURE_SERVICE_TOKEN;

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

    // 1. Process the Latest Positions Service
    // Retrieve the unique user IDs from the positions array
    const userIds = positions.map(pos => pos.userId);
    const uniqueUserIds = Array.from(new Set(userIds));

    // Build the query URL for the latest service by appending '/query'
    const latestQueryUrl = `${latestServiceBaseUrl}/query`;
    const queryParams = {
      where: `userId IN ('${uniqueUserIds.join("','")}')`,
      outFields: "*",
      f: "json"
    };
    if (featureServiceToken) queryParams.token = featureServiceToken;

    const queryResult = await axios.get(latestQueryUrl, { params: queryParams });
    // Build a mapping from userId to OBJECTID for existing features
    const userIdToObjectId = {};
    if (queryResult.data.features && Array.isArray(queryResult.data.features)) {
      queryResult.data.features.forEach(feature => {
        if (feature.attributes && feature.attributes.userId && feature.attributes.OBJECTID !== undefined) {
          userIdToObjectId[feature.attributes.userId] = { objectId: feature.attributes.OBJECTID, ts: feature.attributes.ts_string, attributes: feature.attributes };
        }
      });
    }

    // Prepare features for update (existing) and addition (new) in the latest service
    const featuresToUpdate = [];
    const featuresToAddLatest = [];
    const featuresToAddRunHistory = [];
    positions.forEach(pos => {
      const feature = {
        attributes: {
          userId: pos.userId,
          ts: new Date(pos._ts),
          ts_string: pos._ts,
          gigId: pos.gigId,
          contestId: pos.contestId,
          userId: pos.userId,
          runId: pos.runId,
          speed: pos.speed,
          longitude: pos.longitude,
          latitude: pos.latitude,
          accuracy: pos.accuracy,
          altitude: pos.altitude,
          heading: pos.heading,
          distance: pos.distance,
          activityType: pos.activityType
          // add additional attributes as needed
        },
        geometry: {
          x: pos.longitude,
          y: pos.latitude,
          z: pos.altitude, // Elevation added here
          spatialReference: { wkid: 4326 } // Using WGS84
        }
      };
      // If the user already exists, add the OBJECTID to update the feature
      if (userIdToObjectId.hasOwnProperty(pos.userId)) {
        if (userIdToObjectId[pos.userId].ts != pos._ts) {
          featuresToAddRunHistory.push(feature);
          userIdToObjectId[pos.userId].attributes.previousPos = null;
          feature.attributes.previousPos = JSON.stringify(userIdToObjectId[pos.userId].attributes);
          feature.attributes.OBJECTID = userIdToObjectId[pos.userId].objectId;
          featuresToUpdate.push(feature);
        }
      } else {
        featuresToAddLatest.push(feature);
        featuresToAddRunHistory.push(feature);
      }
    });

    // Utility function to post features (for both add and update)
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

    console.log(featuresToAddLatest)
    console.log(featuresToUpdate)
    console.log(featuresToAddRunHistory)
    // Update existing features in the latest service (if any)
    let updateResponse = null;
    if (featuresToUpdate.length > 0) {
      const latestUpdateUrl = `${latestServiceBaseUrl}/updateFeatures`;
      updateResponse = await postFeatures(latestUpdateUrl, featuresToUpdate);
    }

    // Add new features to the latest service (if any)
    let addLatestResponse = null;
    if (featuresToAddLatest.length > 0) {
      const latestAddUrl = `${latestServiceBaseUrl}/addFeatures`;
      addLatestResponse = await postFeatures(latestAddUrl, featuresToAddLatest);
    }

    // Add new features to the runHistory service (if any)
    let addRunHistoryResponse = null;
    if (featuresToAddRunHistory.length > 0) {
      const runHistoryAddUrl = `${runHistoryServiceBaseUrl}/addFeatures`;
      addRunHistoryResponse = await postFeatures(runHistoryAddUrl, featuresToAddRunHistory);
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Latest positions updated and history added",
        latestUpdates: {
          updatedCount: featuresToUpdate.length,
          addedCount: featuresToAddLatest.length,
          addedCountHistory: featuresToAddRunHistory.length
        },
        updateResponse: updateResponse ? updateResponse.data : null,
        addLatestResponse: addLatestResponse ? addLatestResponse.data : null,
        addRunHistoryResponse: addRunHistoryResponse ? addRunHistoryResponse.data : null  
      })
    };

  } catch (error) {
    console.error('Error processing data:', error.message);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};
