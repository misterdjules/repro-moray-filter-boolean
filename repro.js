var bunyan = require('bunyan');
var config = require('./config.json');
var jsprim = require('jsprim');
var moray = require('moray');
var tape = require('tape');
var vasync = require('vasync');

var test = require('./lib/test.js');

var morayConfig = jsprim.deepCopy(config);
morayConfig.log = bunyan.createLogger({
    name: 'moray-client',
    level: 'error'
});

tape('moray findobjects search filters using unindexed fields', function (t) {
    var morayClient = moray.createClient(morayConfig);

    console.log('Connecting to moray...');

    morayClient.on('connect', function onMorayClientConnected() {
        console.log('connected to moray!');

        vasync.pipeline({funcs: [
            function testBooleanExistingValues(arg, next) {
                console.log('Testing with boolean values for newly indexed ' +
                    'property present _before_ new index added');
                    test.testFilterWithExistingValues(t, morayClient, {
                        newIndexedFieldType: 'boolean'
                    }, function testDone() {
                        next();
                    });
            },
            function testBooleanNewValues(arg, next) {
                console.log('Testing with boolean values for newly indexed ' +
                    'property added _after_ new index added')
                    test.testFilterWithNewValues(t, morayClient, {
                        newIndexedFieldType: 'boolean'
                    }, function testDone() {
                        next();
                    });
            },
            function testNumberExistingValues(arg, next) {
                console.log('Testing with number values for newly indexed ' +
                    'property present _before_ new index added');
                    test.testFilterWithExistingValues(t, morayClient, {
                        newIndexedFieldType: 'number'
                    }, function testDone() {
                        next();
                    });
            },
            function testNumberNewValues(arg, next) {
                console.log('Testing with number values for newly indexed ' +
                    'property added _after_ new index added');
                    test.testFilterWithNewValues(t, morayClient, {
                        newIndexedFieldType: 'number'
                    }, function testDone() {
                        next();
                    });
            },
            function testStringExistingValues(arg, next) {
                console.log('Testing with string values for newly indexed ' +
                    'property present _before_ new index added');
                    test.testFilterWithExistingValues(t, morayClient, {
                        newIndexedFieldType: 'string'
                    }, function testDone() {
                        next();
                    });
            },
            function testNumberStringValues(arg, next) {
                console.log('Testing with string values for newly indexed ' +
                    'property added _after_ new index added');
                    test.testFilterWithNewValues(t, morayClient, {
                        newIndexedFieldType: 'string'
                    }, function testDone() {
                        next();
                    });
            }
        ]}, function allTestsDone(err) {
            t.end();
            morayClient.close();
            console.log('All tests done!');
        })
    });
});
