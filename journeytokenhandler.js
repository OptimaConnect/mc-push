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


const validateTokenContext = (fuel2Token) => new Promise((resolve, reject) => {

	console.dir("The context endpoint is: ");

	console.dir(contextUrl);

	console.dir("The fuel token passed to this function is: ");

	console.dir(fuel2Token);

	axios({
		method: 'get',
		url: contextUrl,
		headers: {'Authorization': 'Bearer '.concat(fuel2Token)}
	})
	.then(function (tokenResponse) {
		console.dir('Token Response');
		console.dir(tokenResponse);
		return resolve(tokenResponse);
	})
	.catch(function (error) {
		console.dir("Error getting token context response");
		console.dir(error);
		return reject(error);
	});
});