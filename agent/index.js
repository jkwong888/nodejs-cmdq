const https = require('https');
const {PubSub} = require('@google-cloud/pubsub');
const {IAMCredentialsClient} = require('@google-cloud/iam-credentials');
const faker = require('faker');
const { URL } = require('url');

const PUBSUB_API_ENDPOINT = process.env.PUBSUB_API_ENDPOINT || "us-central1-pubsub.googleapis.com:443"
const PUBSUB_PROJECT_ID = process.env.PROJECT_ID || "jkwng-pubsub-cmdq"
const PUBSUB_CMD_SUB = process.env.PUBSUB_CMD_SUB || "cmdq-sub"
const PUBSUB_HEARTBEAT_TOPIC = process.env.PUBSUB_HEARTBEAT_TOPIC || "agent-heartbeat";
const HEARTBEAT_URL = process.env.HEARTBEAT_URL || "localhost:3000/api/agent/heartbeat";

const keys = require(process.env.GOOGLE_APPLICATION_CREDENTIALS ||  null);
const SERVICE_ACCOUNT_EMAIL = keys.client_email || null;
const SCOPES = 'https://www.googleapis.com/auth/iam'


// Creates a pubsub client; cache this for further use
const pubSubClient = new PubSub({
  // Sending messages to the same region ensures they are received in order
  // even when multiple publishers are used.
  apiEndpoint: PUBSUB_API_ENDPOINT,
  projectId: PUBSUB_PROJECT_ID,
});

const iamClient = new IAMCredentialsClient();

async function generateAccessToken(audience) {
  const token = await iamClient.generateIdToken({
    name: `projects/-/serviceAccounts/${SERVICE_ACCOUNT_EMAIL}`,
    audience: audience,
    includeEmail: true,
  }).then(response => {
    console.info(response[0].token);
    return response[0].token;
  }).catch(error => {
    console.error("Error generating access token", error);

  });
  //console.info(token);

  return token;
}

async function getSubscription(subscriptionName) {
  console.log(`Listening for messages on subscription: ${subscriptionName}`);

  // Gets the metadata for the subscription
  const subscription = await pubSubClient
    .subscription(subscriptionName)

  return subscription;
}

async function listenForMessages(subscription) {
  // Create an event handler to handle messages
  let messageCount = 0;
  let lastMessageNum = -1;

  // Listen for new messages until timeout is hit
  subscription.on('message', messageHandler);
  
  var timeout = 60;
}

function generateResponse() {
  // TODO generate some random data - should we encode it in avro?
  result = [];
  i = 0;
  for (i = 0; i < Math.random() * 10; i++) {
    var name = faker.name.findName();
    var email = faker.internet.email();
    var quantity = faker.datatype.number(1000);
    result.push({
      i,
      name,
      email,
      quantity,
    })
  }

  return result;
}

const messageHandler = async message => {
    console.log(`Received message ${message.id}: Data: ${message.data}`);

    console.log("Acknowledging message");
    message.ack();

    const data = JSON.parse(message.data);
    const resultUrl = data.resultUrl;
    const myURL = new URL(resultUrl);


    const accessToken = await generateAccessToken(resultUrl);

    const result = generateResponse();
    const resultStr = JSON.stringify(result);

    const options = {
      hostname: myURL.host,
      port: myURL.port,
      path: myURL.pathname,
      protocol: myURL.protocol,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': resultStr.length,
        'Authorization': `Bearer ${accessToken}`,
      },
    }

    //console.log(options);
    console.log(`Posting ${result.length} records to ${resultUrl}`);

    const req = https.request(options, res => {
      console.log(`statusCode: ${res.statusCode}`);
    });

    req.on('error', error => {
      console.error(error);
    });

    req.write(resultStr);
    req.end();


}

async function heartbeatHttpPost() {
  console.log(`Heartbeat to: ${HEARTBEAT_URL}`)
  const accessToken = await generateAccessToken(HEARTBEAT_URL);
  const myURL = new URL(HEARTBEAT_URL);

  const options = {
    hostname: myURL.host,
    port: myURL.port,
    path: myURL.pathname,
    protocol: myURL.protocol,
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
    },
  }

  const req = https.request(options, res => {
    console.log(`statusCode: ${res.statusCode}`);
  });

  req.end();

}

async function heartbeatPubsub() {
  const agentId = SERVICE_ACCOUNT_EMAIL.split("@")[0];
  const heartbeatMessage = {
    agent:agentId,
  };

  const dataBuffer = Buffer.from(JSON.stringify(heartbeatMessage));

  // message payload
  const message = {
    data: dataBuffer,
  };

  // Publishes the message
  const messageId = await pubSubClient
    .topic(PUBSUB_HEARTBEAT_TOPIC)
    .publishMessage(message);

  console.log(`Heartbeat message ${messageId} published to topic ${PUBSUB_HEARTBEAT_TOPIC}.`);
}

function heartbeat() {
  heartbeatPubsub();
}

// heartbeat every 15 seconds
setInterval(heartbeat, 15000);

getSubscription(PUBSUB_CMD_SUB).then(subscription => {
  listenForMessages(subscription);
});

