const express = require('express');
const bodyParser = require('body-parser');
const port = process.env.PORT || 3000;
const {PubSub} = require('@google-cloud/pubsub');
const { v4: uuidv4 } = require('uuid');
const redis = require('redis');

const MY_URL = process.env.MY_URL || "http://localhost:3000"
const PUBSUB_API_ENDPOINT = process.env.PUBSUB_API_ENDPOINT || "us-central1-pubsub.googleapis.com:443"
const PUBSUB_PROJECT_ID = process.env.PROJECT_ID || "jkwng-pubsub-cmdq"
const PUBSUB_TOPIC_NAME = process.env.TOPIC_NAME || "cmdq"

const REDIS_HOST = process.env.REDISHOST || 'localhost';
const REDIS_PORT = process.env.REDISPORT || 6379;

const app = express();
const server = require('http').createServer(app);

app.use(bodyParser.json());

// Creates a pubsub client; cache this for further use
const pubSubClient = new PubSub({
  // Sending messages to the same region ensures they are received in order
  // even when multiple publishers are used.
  apiEndpoint: PUBSUB_API_ENDPOINT,
  projectId: PUBSUB_PROJECT_ID,
});

const redisClient = redis.createClient(REDIS_PORT, REDIS_HOST);

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
app.post('/api/cmd', (req, res) => {
  const uuid = uuidv4();

  console.log(`cmd uuid: ${uuid}`);
  console.log(`request body: ${JSON.stringify(req.body)}`);

  // create a redis key with the cmd including a TTL (?)
  // TODO: add the user/agent involved for authorization
  redisClient.set(
    uuid, 
    JSON.stringify({
      cmdId: uuid,
      user: "",
      agentId: "",
      reqBody: req.body,
      resultUrl: `${MY_URL}/api/results/${uuid}`,
      result: null,
    }),
    redis.print);

  // TODO: publish the message on pubsub to the right queue
  publishMessage(JSON.stringify({
    cmdId: uuid,
    reqBody: req.body,
    resultUrl: `${MY_URL}/api/results/${uuid}`,
  }));

  // return the UUID to the client for polling
  res.set('Location', `${MY_URL}/api/results/${uuid}`);
  return res.status(201).end();
});

// from the client - poll for cmd results
app.get('/api/results/:cmdId', (req, res) => {
  const cmdId = req.params.cmdId;

  redisClient.get(cmdId, function(err, reply) {
    console.log(reply);

    if (err) {
      console.err("Error connecting to redis!", err);
      // uhh?
      return res.status(502).send();
    }

    // TODO: if redis key does not exist, return HTTP 404
    if (reply === null) {
      return res.status(404).send();
    }

    const output = JSON.parse(reply).result;
    console.log(output);

    // TODO: if redis key exists but is empty/pending, return HTTP 202
    if (output === null ) {
      return res.status(202).send();
    }

    // TODO: if redis key exists and populated, return HTTP 200 with results
    return res.json(output);
  });


});

authenticateAgent(req, res, next) {
  if (!authHeader.toLowerCase().startsWith('bearer ')) {
    // we don't support basic auth
    return res.status(403).send();
  }

  const authToken = authHeader.split()[1];
  console.log("Auth token: " + authToken);

  // validate the token
  
  

}

// from the agent - report the cmd results
app.post('/api/results/:cmdId', authenticateAgent, (req, res) => {
  // TODO: check the caller and make sure they're authorized to post the results
  const cmdId = req.params.cmdId;
  const authHeader = req.headers.authorization;

  console.log("From agent: " + JSON.stringify(req.body));
  console.log("Auth token: " + authToken);

  // populate the redis key with results
  redisClient.get(cmdId, function(err, reply) {
    //console.log(reply);

    if (err) {
      console.err("Error connecting to redis!", err);
      // uhh?
      return res.status(502).send();
    }

    // if redis key does not exist, return HTTP 404
    if (reply == null) {
      return res.status(404).send();
    }

    // if redis key exists but is empty/pending, return HTTP 409 conflict
    if (reply.result != null ) {
      return res.status(409).send();
    }

    // if redis key has an empty result, populate it with our result 
    redisClient.set(
      cmdId, 
      JSON.stringify({
        cmdId: cmdId,
        user: reply.user,
        agentId: reply.agentId,
        reqBody: reply.reqBody,
        resultUrl: reply.resultUrl,
        result: req.body,
      }),
      redis.print);
  });

  return res.status(201).send();
});


    
server.listen(port, () => {
  console.log('Server listening at port %d', port);
});
