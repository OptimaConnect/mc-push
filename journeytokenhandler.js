'use strict';

const axios = require('axios');

const restUrl = process.env.restUrl;

exports.validateToken = async function (req, res, next) {
    const fuelAuth = req.headers.authorization;
    const contextUrl = restUrl + "platform/v1/tokenContext";

    console.log(`Authentication Header: ${fuelAuth}`);

    if (!fuelAuth){
        res.locals.authenticated = false;
        res.sendStatus(403);
        return;
    }

    try {
        const response = await axios({
            url: contextUrl,
            headers: { "Authorization": fuelAuth }
        });

        res.locals.authenticated = true;
        next();
    } catch (error) {
        console.log(error);
        res.locals.authenticated = false;
        res.sendStatus(403);
    }
}