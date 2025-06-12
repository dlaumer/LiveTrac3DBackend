// 1_createService.js
const axios = require('axios');

const AGOL_PORTAL = 'https://egregis.maps.arcgis.com/';
const USERNAME    = 'Trac3D';
const PASSWORD    = 'Trac2025';
const SERVICE_NAME = 'MyNewService'; // unique name in your org

async function generateToken() {
  const params = new URLSearchParams({
    f:        'json',
    username: USERNAME,
    password: PASSWORD,
    client:   'referer',
    referer:  AGOL_PORTAL
  });
  const resp = await axios.post(`${AGOL_PORTAL}/sharing/rest/generateToken`, params);
  if (resp.data.error) throw new Error(resp.data.error.message);
  return resp.data.token;
}

async function createEmptyService(token) {
  const createParameters = {
    name:               SERVICE_NAME,
    serviceDescription: 'Truly empty service; schema added separately',
    hasStaticData:      false,
    maxRecordCount:     1000,
    supportedQueryFormats: 'JSON',
    capabilities:       'Create,Delete,Query,Update,Editing',
    spatialReference:   { wkid: 4326 },
    initialExtent: {
      xmin: -180, ymin: -90, xmax: 180, ymax: 90,
      spatialReference: { wkid: 4326 }
    },
    units:              'esriDecimalDegrees',
    xssPreventionInfo: {
      xssPreventionEnabled: true,
      xssPreventionRule:    'input',
      xssInputRule:         'rejectInvalid'
    }
    // no "layers" here
  };

  const params = new URLSearchParams({
    f:                'json',
    token,
    outputType:       'featureService',
    createParameters: JSON.stringify(createParameters)
  });

  const url = `${AGOL_PORTAL}/sharing/rest/content/users/${USERNAME}/createService`;
  const resp = await axios.post(url, params);
  if (resp.data.error) throw new Error(resp.data.error.message);
  return resp.data; // contains itemId and serviceurl
}

(async () => {
  try {
    const token = await generateToken();
    console.log('Token generated:', token);
    const { serviceurl } = await createEmptyService(token);
    console.log('Empty service ready at:', serviceurl);
  } catch (err) {
    console.error(err);
  }
})();
