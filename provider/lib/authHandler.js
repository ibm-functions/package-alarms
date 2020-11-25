const iam = require('@ibm-functions/iam-token-manager');
const crypto = require("crypto");
const constants = require('./constants.js');

const encryptKeyID = process.env.CONFIG_WHISK_CRYPT_KEKI
const encryptKeyValue = process.env.CONFIG_WHISK_CRYPT_KEK
const encryptFallBackKeyID = process.env.CONFIG_WHISK_CRYPT_KEKIF
const encryptFallBackKeyValue = process.env.CONFIG_WHISK_CRYPT_KEKF

var tokenManagers = {};

function handleAuth(triggerData, options) {

    if (triggerData.additionalData && triggerData.additionalData.iamApikey) {
        return new Promise(function (resolve, reject) {
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
    } else {
        let decryptedAuth = decryptAuth(triggerData.apikey);
        if (decryptedAuth && decryptedAuth.indexOf(':') !== -1) {
            let auth = decryptedAuth.split(':');
            options.auth = {
                user: auth[0],
                pass: auth[1]
            };
            return Promise.resolve(options);
        } else {
            let error = new Error('Unable to decrypt auth');
            error.statusCode = 400;
            return Promise.reject(error);
        }
    }
}

function getToken(triggerData) {

    if (!(triggerData.additionalData.iamApikey in tokenManagers)) {
        let decryptedAuth = decryptAuth(triggerData.additionalData.iamApikey);
        if (decryptedAuth) {
            tokenManagers[triggerData.additionalData.iamApikey] = new iam({
                iamApikey: decryptedAuth,
                iamUrl: triggerData.additionalData.iamUrl
            });
        } else {
            let error = new Error('Unable to decrypt auth');
            error.statusCode = 400;
            return Promise.reject(error);
        }
    }
    return tokenManagers[triggerData.additionalData.iamApikey].getToken();
}

function decryptAuth(authDBString) {
    let authDBStringArray = authDBString.split('::');
    if (authDBStringArray.length === 1) {
        return authDBString;
    } else if (authDBStringArray.length > 3) {
        let key;
        let cryptVersionID = authDBStringArray[2];
        let base64NonceAndCiphertext = authDBStringArray[3];
        if (cryptVersionID === encryptKeyID) {
            key = encryptKeyValue;
        } else if (cryptVersionID === encryptFallBackKeyID) {
            key = encryptFallBackKeyValue
        } else {
            return "";
        }

        // decode and retrieve nonce, ciphertext and tag
        let nonceAndCiphertext = Buffer.from(base64NonceAndCiphertext, "base64");
        let nonce = nonceAndCiphertext.slice(0, constants.ALGORITHM_NONCE_SIZE_12);
        let ciphertext = nonceAndCiphertext.slice(constants.ALGORITHM_NONCE_SIZE_12, nonceAndCiphertext.length - constants.ALGORITHM_TAG_SIZE_16);
        let tag = nonceAndCiphertext.slice(constants.ALGORITHM_NONCE_SIZE_12 + ciphertext.length);
        // create cipher instance and set tag
        let cipher = crypto.createDecipheriv(constants.ALGORITHM_AES_256_GCM, Buffer.from(key, "utf8"), nonce);
        cipher.setAuthTag(tag);
        // decrypt and return
        return Buffer.concat([cipher.update(ciphertext), cipher.final()]).toString("utf8");
    } else {
        return "";
    }
}

module.exports = {
    'handleAuth': handleAuth
};
