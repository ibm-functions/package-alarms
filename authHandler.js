const iam = require('@ibm-functions/iam-token-manager');

var tokenManagers = {};

function handleAuth(triggerData, options) {

    if (triggerData.additionalData && triggerData.additionalData.iamApikey) {
        return new Promise(function(resolve, reject) {
            getToken(triggerData)
            .then(token => {
                options.auth = {bearer: token};
                if (options.method === 'delete') {
                    options.headers = {'x-functions-firedonce': 'true'};
                }
                resolve(options);
            })
            .catch(err => {
               reject(err);
            });
        });
    }
    else {
        var auth = triggerData.apikey.split(':');
        options.auth = {
            user: auth[0],
            pass: auth[1]
        };
        return Promise.resolve(options);
    }
}

function getToken(triggerData) {

    if (!(triggerData.additionalData.iamApikey in tokenManagers)) {
        var tm = new iam({
            iamApikey: triggerData.additionalData.iamApikey,
            iamUrl: triggerData.additionalData.iamUrl
        });
        tokenManagers[triggerData.additionalData.iamApikey] = tm;
    }
    return tokenManagers[triggerData.additionalData.iamApikey].getToken();
}

module.exports = {
    'handleAuth': handleAuth
};
