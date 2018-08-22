function getOpenWhiskConfig(triggerData) {
    if (triggerData.additionalData && triggerData.additionalData.iamApikey) {
        var iam = require('@ibm-functions/iam-token-manager');
        var tm = new iam({
            iamApikey: triggerData.additionalData.iamApikey,
            iamUrl: triggerData.additionalData.iamUrl
        });
        return {ignore_certs: true, namespace: triggerData.namespace, auth_handler: tm};
    }
    else {
        return {ignore_certs: true, namespace: triggerData.namespace, api_key: triggerData.apikey};
    }
}

function addAdditionalData(params) {
    var additionalData = {};

    if (process.env.__OW_IAM_NAMESPACE_API_KEY) {
        additionalData.iamApikey = process.env.__OW_IAM_NAMESPACE_API_KEY;
        additionalData.iamUrl = 'https://iam.bluemix.net/identity/token';
    }

    params.additionalData = JSON.stringify(additionalData);
}

module.exports = {
    'addAdditionalData': addAdditionalData,
    'getOpenWhiskConfig': getOpenWhiskConfig
};
