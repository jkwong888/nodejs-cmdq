const https = require('https');
const {PubSub} = require('@google-cloud/pubsub');
const {IAMCredentialsClient} = require('@google-cloud/iam-credentials');
const faker = require('faker');
const { URL } = require('url');
const { access } = require('fs');

const PUBSUB_API_ENDPOINT = process.env.PUBSUB_API_ENDPOINT || "us-central1-pubsub.googleapis.com:443"
const PUBSUB_PROJECT_ID = process.env.PROJECT_ID || "jkwng-pubsub-cmdq"
const PUBSUB_SUBSCRIPTION_NAME = process.env.SUBSCRIPTION_NAME || "cmdq-sub"

const keys = require(process.env.GOOGLE_APPLICATION_CREDENTIALS ||  null);
const SERVICE_ACCOUNT_EMAIL = keys.client_email || null;
const SCOPES = 'https://www.googleapis.com/auth/iam'

const HEARTBEAT_URL = process.env.HEARTBEAT_URL || "localhost:3000/api/agent/heartbeat";

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

async function getSubscription() {
  // Gets the metadata for the subscription
  const subscription = await pubSubClient
    .subscription(PUBSUB_SUBSCRIPTION_NAME)
  const [metadata] = await subscription.getMetadata();

  console.log(`Subscription: ${metadata.name}`);
  console.log(`Topic: ${metadata.topic}`);
  console.log(`Push config: ${metadata.pushConfig.pushEndpoint}`);
  console.log(`Ack deadline: ${metadata.ackDeadlineSeconds}s`);

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

const messageHandler = async message => {
    console.log(`Received message ${message.id}: Data: ${message.data}`);

    console.log("Acknowledging message");
    message.ack();

    const data = JSON.parse(message.data);
    const resultUrl = data.resultUrl;
    const myURL = new URL(resultUrl);

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

    const accessToken = await generateAccessToken(resultUrl);

    resultStr = JSON.stringify(result);
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

    console.log(options);
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

async function heartbeat() {
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

// heartbeat every 15 seconds
setInterval(heartbeat, 15000);

getSubscription().then((subscription => {
  listenForMessages(subscription);
}));

