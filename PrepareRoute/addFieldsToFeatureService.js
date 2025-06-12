// addSchemaCombined.js

const axios = require('axios');
const AGOL_PORTAL = 'https://egregis.maps.arcgis.com/';
const USERNAME = 'Trac3D';
const PASSWORD = 'Trac2025';
const SERVICE_URL = 'https://services1.arcgis.com/i9MtZ1vtgD3gTnyL/arcgis/rest/services/Trac3DTest/FeatureServer/0';

async function generateToken() {
    const params = new URLSearchParams({
        f: 'json',
        username: USERNAME,
        password: PASSWORD,
        client: 'referer',
        referer: AGOL_PORTAL
    });

    const resp = await axios.post(
        `${AGOL_PORTAL}/sharing/rest/generateToken`,
        params
    );
    if (resp.data.error) throw new Error(resp.data.error.message);
    return resp.data.token;
}

async function addLayerDefinition(token) {
    const layerDef = {
        fields: [
            {
                name: "str2",
                type: "esriFieldTypeString",
                alias: "str2",
                nullable: true,
                editable: true,
                domain: null,
            }

        ]
    };

    const params = new URLSearchParams({
        f: 'json',
        token,
        addToDefinition: JSON.stringify(layerDef)
    });

    // This is the correct admin URL
    const resp = await axios.post(
        `${SERVICE_URL}/addToDefinition`,
        params
    );
    if (resp.data.error) throw new Error(resp.data.error);
    return resp.data;
}

(async () => {
    try {
        console.log('🔑 Generating token…');
        const token = await generateToken();

        console.log('📐 Adding schema…');
        const result = await addLayerDefinition(token);

        console.log('✅ Schema added!', result);
    } catch (err) {
        // If it’s an HTTP error, show the full JSON
        if (err.response && err.response.data) {
            console.error('❌ AGOL Error:', JSON.stringify(err.response.data, null, 2));
        } else {
            console.error('❌ Error:', err);
        }
    }
})();
