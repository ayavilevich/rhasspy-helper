// node
const fs = require('fs');

// 3rd party
const mqtt = require('mqtt'); // https://www.npmjs.com/package/mqtt
const WaveFile = require('wavefile').WaveFile; // https://www.npmjs.com/package/wavefile

// consts
const logger = console;
const RIFF = "RIFF";

// load config
const dotenv = require('dotenv');
dotenv.config();

// get command line arguments
const args = process.argv.slice(2);
if (args.length < 2) {
	console.error('Pass topic and output filename as param');
	process.exit(1);
}
const topicFilters = [args[0]];
const outputFilepath = args[1]

// start run
if (!process.env.MQTT_BROKER_URL) {
	logger.error('Invalid parameters passed');
	process.exit();
}

// connect to mqtt broker
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL);

// main logic
// store buffers and join+flush them in the end
// https://stackoverflow.com/questions/49129643/how-do-i-merge-an-array-of-uint8arrays
const buffers = []; // array of Uint8Array buffers
let lastFmt;

function handleMessage(topic, message) {
	// message is Buffer
	if (message.slice(0, 4).toString().valueOf() === RIFF) {
		const wav = new WaveFile(message); // https://www.npmjs.com/package/wavefile#the-wavefile-properties
		// store
		lastFmt = wav.fmt;
		// buffers.push(wav.data.samples); // uint8
		buffers.push(wav.getSamples(true, Int16Array)); // to fit "wav.fromScratch" later
		// log
		logger.info(`${message.length} ${wav.container} ${wav.chunkSize} ${wav.format} ${wav.fmt.chunkId} ${wav.fmt.numChannels} ${wav.fmt.sampleRate} ${wav.fmt.bitsPerSample}`);
	} else {
		logger.warn('Non audio message');
	}
}

function exitWithFlush() {
	logger.info('Exiting');
	logger.info(buffers.length);
	if (buffers.length === 0 || !lastFmt) {
		logger.warn('No data was collected, nothing to dump');
	} else {
		// join buffers
		const totalSampleCount = buffers.reduce((acc, val) => acc + val.length, 0);
		logger.info(buffers.length, buffers[0].length, totalSampleCount);
		const mergedArray = new Int16Array(totalSampleCount);
		let offset = 0;
		buffers.forEach(buffer => {
			mergedArray.set(buffer, offset);
			offset += buffer.length;
		})
		// save to file
		const wav = new WaveFile();
		wav.fromScratch(lastFmt.numChannels, lastFmt.sampleRate, lastFmt.bitsPerSample.toString(), mergedArray);
		fs.writeFileSync(outputFilepath, wav.toBuffer(), 'binary');
	}
	process.exit();
}

// exit handling
process.on('SIGINT', () => {
	logger.warn('Caught interrupt signal');
	exitWithFlush();
});

// is this interesting?!
process.on('SIGHUP', () => {
	logger.warn('Caught SIGHUP');
	exitWithFlush();
});

// mqtt events
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
