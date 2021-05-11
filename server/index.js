const https = require('https');
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const {PubSub} = require('@google-cloud/pubsub');
const { v4: uuidv4 } = require('uuid');
const redis = require('redis');
const { promisifyAll } = require('bluebird');
const kjur = require('jsrsasign');

const port = process.env.PORT || 3000;
const MY_URL = process.env.MY_URL || "http://localhost:3000"
const PUBSUB_API_ENDPOINT = process.env.PUBSUB_API_ENDPOINT || "us-central1-pubsub.googleapis.com:443"
const PUBSUB_PROJECT_ID = process.env.PROJECT_ID || "jkwng-pubsub-cmdq"
const PUBSUB_TOPIC_NAME = process.env.TOPIC_NAME || "cmdq"

const REDIS_HOST = process.env.REDISHOST || 'localhost';
const REDIS_PORT = process.env.REDISPORT || 6379;

const GOOGLE_OIDC_CONF_URL = "https://accounts.google.com/.well-known/openid-configuration"
const GOOGLE_TOKEN_ISS = "https://accounts.google.com"

// load the public cert list for verification for ID tokens
const googleOIDCConf = {};
const googleSecureTokenCerts = {};
function fetchGoogleOIDCConf() {
  console.log("loading oidc configuration from URL: " + GOOGLE_OIDC_CONF_URL);
  const oidcreq = https.get(GOOGLE_OIDC_CONF_URL, {}, (res) => {
    var confStr = '';
    res.on('data', (d) => {
      confStr = confStr + d;
    });

    res.on('end', () => {
      const googleOIDCConf = JSON.parse(confStr);
      //console.log(googleOIDCConf);
      //return googleOIDCConf;
      cacheGoogleCerts(googleOIDCConf.jwks_uri);
    });
  });
}

function cacheGoogleCerts(url) {
  console.log("loading secure token certs from google URL: " + url);
  const certsreq = https.get(url, {}, (res) => {
    var certsStr = '';
    res.on('data', (d) => {
      certsStr = certsStr + d;
    });

    res.on('end', () => {
      const googleCerts = JSON.parse(certsStr);
      //console.log(googleCerts);

      googleCerts.keys.forEach((value) => {
        googleSecureTokenCerts[value.kid] = kjur.KEYUTIL.getKey(value);
      });

      //console.log(googleSecureTokenCerts);
      //return googleOIDCConf;
      //cacheGoogleKeys(googleOIDCConf.jwks_uri);
    });

  });
}

const app = express();
const server = require('http').createServer(app);

app.use(express.json());
const corsOptions = {
  exposedHeaders: 'Location',
};
app.use(cors(corsOptions));

// Creates a pubsub client; cache this for further use
const pubSubClient = new PubSub({
  // Sending messages to the same region ensures they are received in order
  // even when multiple publishers are used.
  apiEndpoint: PUBSUB_API_ENDPOINT,
  projectId: PUBSUB_PROJECT_ID,
});

const redisClient = redis.createClient(REDIS_PORT, REDIS_HOST);
promisifyAll(redisClient);

redisClient.on("error", function(error) {
  console.error("REDIS ERROR", error);
});

async function publishMessage(data) {

  console.log(data);
  // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
  const dataBuffer = Buffer.from(data);

  // Be sure to set an ordering key that matches other messages
  // you want to receive in order, relative to each other.
  const message = {
    data: dataBuffer,
  };

  // Publishes the message
  const messageId = await pubSubClient
    .topic(PUBSUB_TOPIC_NAME)
    .publishMessage(message);

  console.log(`Message ${messageId} published.`);

  return messageId;
}

// from the client - create a cmd for backend agent
app.post('/api/cmd', async (req, res) => {
  const uuid = uuidv4();

  req.accepts('application/json');

  console.log(`cmd uuid: ${uuid}`);
  console.log(`request body: ${JSON.stringify(req.body)}`);

  const reqBody = req.body;
  const targetAgent = reqBody.agent;

  // create a redis key with the cmd including a TTL (?)
  // TODO: add the user/agent involved for authorization
  await redisClient.setAsync(
    uuid, 
    JSON.stringify({
      cmdId: uuid,
      user: "",
      agentId: targetAgent,
      reqBody: req.body,
      resultUrl: `${MY_URL}/api/results/${uuid}`,
      result: null,
    }),
    'EX',
    60*60, // expire in 1 hour
  );

  // TODO: publish the message on pubsub to the right queue
  await publishMessage(JSON.stringify({
    cmdId: uuid,
    reqBody: req.body,
    resultUrl: `${MY_URL}/api/results/${uuid}`,
  }));

  // return the UUID to the client for polling
  res.set('Location', `${MY_URL}/api/results/${uuid}`);
  return res.status(201).end();
});

// from the client - poll for cmd results
app.get('/api/results/:cmdId', async (req, res) => {
  const cmdId = req.params.cmdId;

  req.accepts('application/json');

  const cmd = await redisClient.getAsync(cmdId);
  console.log(cmd);

  // if redis key does not exist, return HTTP 404
  if (cmd === null) {
    return res.status(404).send();
  }

  const output = JSON.parse(cmd).result;
  console.log(output);

  // if redis key exists but is empty/pending, return HTTP 202
  if (output === null ) {
    return res.status(202).send();
  }

  // if redis key exists and populated, return HTTP 200 with results
  return res.json(output);
});

async function authenticateAgent(req, res, next) {
  const authHeader = req.headers.authorization;

  if (!authHeader.toLowerCase().startsWith('bearer ')) {
    // we don't support basic auth
    return res.status(403).send();
  }

  const authToken = authHeader.split(" ")[1];
  console.log("Auth token: " + authToken);

  // get the kid from the token header
  var jwt = kjur.KJUR.jws.JWS.parse(authToken);
  var kid = jwt.headerObj['kid'];
  var alg = jwt.headerObj['alg'];

  // get the pubkey for this kid
  const pubkey = googleSecureTokenCerts[kid];
  if (! pubkey) {
    console.error(`Invalid token -- Unknown kid ${kid}`);
    res.status(403).send('Invalid token -- Unknown kid');
  }

  // validate the signature on the token using the pubkey
  // -- also validates iat and exp
  const isValid = kjur.KJUR.jws.JWS.verifyJWT(authToken, pubkey, {alg: [alg]});
  if (!isValid) {
    console.error('Invalid token - verification failed');
    res.status(403).send('Invalid token');
  }

  // iss must be the securetoken URL with our project in it
  if (jwt.payloadObj['iss'] !== GOOGLE_TOKEN_ISS) {
    isValid = false;
    console.error(`Invalid token -- invalid issuer ${jwt.payload['iss']}`);
    res.status(403).send('Invalid token');
  }

  // make sure aud is for this URL
  const aud = jwt.payloadObj['aud'];
  if (aud !== `${MY_URL}${req.originalUrl}`) {
    isValid = false;
    console(`Invalid token - invalid aud ${aud}, expecting ${MY_URL}${req.originalUrl}`);
    res.status(403).send('Invalid token');
  }

  // auth_time must be in the past
  const tNow = kjur.jws.IntDate.get('now');
  if (jwt.payloadObj['auth_time'] > tNow) {
    isValid = false;
    console.error('Invalid token authtime');
    res.status(403).send('Invalid token');
  }
  
  // sub must be non-empty -- corresponds to the user id
  if (!jwt.payloadObj['sub'] || jwt.payloadObj['sub'] === '') {
    isValid = false;
    console.error('Invalid token - invalid sub');
    res.status(403).send('Invalid token');
  }

  next();
}

// from the agent - report the cmd results
app.post('/api/results/:cmdId', authenticateAgent, async (req, res) => {
  // TODO: check the caller and make sure they're authorized to post the results
  const cmdId = req.params.cmdId;
  const authHeader = req.headers.authorization;
  const authToken = authHeader.split(" ")[1];

  // validate the agent that posted the response is the same one that we expect
  var jwt = kjur.KJUR.jws.JWS.parse(authToken);
  const agent_email = jwt.payloadObj['email'];

  console.log(`Response from agent ${agent_email}: ${JSON.stringify(req.body)}`);

  // populate the redis key with results
  const cmdStr = await redisClient.getAsync(cmdId);

  // if redis key does not exist, return HTTP 404
  if (cmdStr == null) {
    console.error(`Error - key for cmd ${cmdId} doesn't exist!`)
    return res.status(404).send();
  }

  const cmd = JSON.parse(cmdStr);
  // if redis key exists but is empty/pending, return HTTP 409 conflict
  if (cmd.result != null ) {
    console.error(`Error - key for cmd ${cmdId} already populated!`)
    return res.status(409).send();
  }

  if (cmd.agentId != null && cmd.agentId !== agent_email) {
    console.error(`Error - key for cmd ${cmdId} doesn't match agentId ${cmd.agentId} != ${agent_email}!`)
    return res.status(403).send();
  }

  // if redis key has an empty result, populate it with our result 
  console.log(`Updating cmd ${cmdId} with results`);
  await redisClient.setAsync(
    cmdId, 
    JSON.stringify({
      cmdId: cmdId,
      user: cmd.user,
      agentId: cmd.agentId,
      reqBody: cmd.reqBody,
      resultUrl: cmd.resultUrl,
      result: req.body,
    }),
    'EX',
    60*60, // expire in 1 hour
  );

  return res.status(201).send();
});

    
server.listen(port, () => {
  fetchGoogleOIDCConf();
  console.log('Server listening at port %d', port);
  console.log(`Agents post responses to: ${MY_URL}`);
});
