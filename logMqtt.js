// node
const fs = require('fs');

// 3rd party
const mqtt = require('mqtt'); // https://www.npmjs.com/package/mqtt
const WaveFile = require('wavefile').WaveFile; // https://www.npmjs.com/package/wavefile

// consts
const topicFilters = ['hermes/#', 'rhasspy/#'];
const logger = console;
const RIFF = "RIFF";

// TODO
// add printing of timestamps? optional? can mess with de-duping and can make it difficult to compare two sets. OTOH is useful to cross reference with logs.

// load config
const dotenv = require('dotenv');
dotenv.config();

// start run
if (!process.env.MQTT_BROKER_URL) {
	logger.error('Invalid parameters passed');
	process.exit();
}

// connect to mqtt broker
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL);

// main logic
let audioMessagesBuffer = {}; // store them here for dedup

function queueMessage(message) {
	if (message.type === 'audio') {
		// store description of audio event in buffer
		if (!audioMessagesBuffer[message.topic]) {
			audioMessagesBuffer[message.topic] = {};
		}
		if (!audioMessagesBuffer[message.topic][message.text]) {
			audioMessagesBuffer[message.topic][message.text] = 0;
		}
		audioMessagesBuffer[message.topic][message.text] += 1;
	} else {
		// flush audio buffers
		Object.keys(audioMessagesBuffer).forEach(topic => {
			Object.keys(audioMessagesBuffer[topic]).forEach(text => {
				logger.info(`${topic}, ${text} x ${audioMessagesBuffer[topic][text]}`);
			});
		});
		audioMessagesBuffer = {}; // reset
		// flush event
		logger.info(`${message.topic}, ${message.text}`);
	}
}

function handleMessage(topic, message) {
	// message is Buffer
	if(message.slice(0,4).toString().valueOf() === RIFF) {
		const wav = new WaveFile(message); // https://www.npmjs.com/package/wavefile#the-wavefile-properties
		queueMessage({
			type: 'audio',
			topic,
			text: `${message.length} ${wav.container} ${wav.chunkSize} ${wav.format} ${wav.fmt.chunkId} ${wav.fmt.numChannels} ${wav.fmt.sampleRate} ${wav.fmt.bitsPerSample}`,
		});
	} else {
		queueMessage({
			type: 'event',
			topic,
			text: message.toString(),
		});
	}
}

mqttClient.on('connect', () => {
	// subscribe
	logger.info(`Subscribe to topics...: ${topicFilters}`);
	mqttClient.subscribe(topicFilters);
	// handle incoming messages
	mqttClient.on('message', handleMessage);
});

mqttClient.on('error', (error) => {
	logger.error('MQTT error', error);
});

mqttClient.on('reconnect', () => {
	logger.warn('MQTT reconnect');
});

mqttClient.on('disconnect', () => {
	logger.warn('MQTT disconnect');
});

mqttClient.on('offline', () => {
	logger.warn('MQTT offline');
});

mqttClient.on('close', () => {
	logger.warn('MQTT close');
});

mqttClient.on('end', () => {
	logger.warn('MQTT end');
});
