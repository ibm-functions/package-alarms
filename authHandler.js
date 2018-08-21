const iam = require('@ibm-functions/iam-token-manager');

function handleAuth(triggerData) {

    if (triggerData.additionalData && triggerData.additionalData.iamApikey) {
        return new Promise(function(resolve, reject) {
            getToken(triggerData)
            .then(token => {
                resolve({bearer: token});
            })
            .catch(err => {
               reject(err);
            });
        });
    }
    else {
        var auth = triggerData.apikey.split(':');
        return Promise.resolve({
            user: auth[0],
            pass: auth[1]
        });
    }
}

function getToken(triggerData) {

    if (!triggerData.additionalData.tokenManager) {
        var tm = new iam({
            iamApikey: triggerData.additionalData.iamApikey,
            iamUrl: triggerData.additionalData.iamUrl
        });
        triggerData.additionalData.tokenManager = tm;
    }
    return triggerData.additionalData.tokenManager.getToken();
}

module.exports = {
    'handleAuth': handleAuth
};
