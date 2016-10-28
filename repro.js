var bunyan = require('bunyan');
var config = require('./config.json');
var jsprim = require('jsprim');
var moray = require('moray');
var vasync = require('vasync');

var test = require('./lib/test.js');

var morayConfig = jsprim.deepCopy(config);
morayConfig.log = bunyan.createLogger({
    name: 'moray-client',
    level: 'error'
});

var morayClient = moray.createClient(morayConfig);

console.log('Connecting to moray...');

morayClient.on('connect', function onMorayClientConnected() {
    console.log('connected to moray!');
    console.log('Testing moray filter on newly indexed boolean values');

    vasync.pipeline({funcs: [
        function testBooleanExistingValues(arg, next) {
            console.log ('Testing with values for newly indexed property ' +
                'present _before_ new index added');
            test.testFilterWithExistingValues(morayClient, {
                newIndexedFieldType: 'boolean'
            }, next);
        },
        function testBooleanNewValues(arg, next) {
            console.log ('Testing with values for newly indexed property ' +
                'added _after_ new index added')
            test.testFilterWithNewValues(morayClient, {
                newIndexedFieldType: 'boolean'
            }, next);
        },
        function testNumberExistingValues(arg, next) {
            console.log ('Testing with values for newly indexed property ' +
                'present _before_ new index added');
            test.testFilterWithExistingValues(morayClient, {
                newIndexedFieldType: 'number'
            }, next);
        },
        function testNumberNewValues(arg, next) {
            console.log ('Testing with values for newly indexed property ' +
                'added _after_ new index added')
            test.testFilterWithNewValues(morayClient, {
                newIndexedFieldType: 'number'
            }, next);
        },
        function testStringExistingValues(arg, next) {
            console.log ('Testing with values for newly indexed property ' +
                'present _before_ new index added');
            test.testFilterWithExistingValues(morayClient, {
                newIndexedFieldType: 'string'
            }, next);
        },
        function testNumberStringValues(arg, next) {
            console.log ('Testing with values for newly indexed property ' +
                'added _after_ new index added')
            test.testFilterWithNewValues(morayClient, {
                newIndexedFieldType: 'string'
            }, next);
        }
    ]}, function allTestsDone(err) {
        morayClient.close();
        console.log('All tests done!');
    })
});