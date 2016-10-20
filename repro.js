var bunyan = require('bunyan');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var moray = require('moray');
var vasync = require('vasync');

var config = require('./config.json');

var TEST_BUCKET_NAME = 'test_filter_reindexing';

var TEST_BUCKET_CFG_V1 = {
    index: {
        uuid: {
            type: 'string',
            unique: true
        },
        name: {
            type: 'string'
        }
    },
    options: {
        version: 1
    }
}

var TEST_BUCKET_CFG_V2 = {
    index: {
        uuid: {
            type: 'string',
            unique: true
        },
        name: {
            type: 'string'
        },
        valid: {
            type: 'boolean'
        }
    },
    options: {
        version: 2
    }
}

var morayConfig = jsprim.deepCopy(config);
morayConfig.log = bunyan.createLogger({
    name: 'moray-client',
    level: 'error'
});

var morayClient = moray.createClient(morayConfig);

morayClient.on('connect', function onMorayClientConnected() {
    var uuidFoo = libuuid.create();
    var uuidBar = libuuid.create();
    var uuidBarBis = libuuid.create();

    vasync.pipeline({funcs: [
        function deleteTestBucket(ctx, next) {
            console.log('deleting test bucket...');
            morayClient.deleteBucket(TEST_BUCKET_NAME,
                function onBucketDeleted(delErr) {
                    if (delErr && delErr.name !== 'BucketNotFoundError') {
                        next(delErr)
                    } else {
                        next();
                    }
                });
        },
        function createOriginalBucket(ctx, next) {
            console.log('creating test bucket...');
            morayClient.createBucket(TEST_BUCKET_NAME, TEST_BUCKET_CFG_V1,
                next);
        },
        function addFoo(ctx, next) {
            console.log('adding foo...');
            morayClient.putObject(TEST_BUCKET_NAME, uuidFoo, {
                uuid: uuidFoo,
                name: 'foo',
                valid: true
            }, {noBucketCache: true}, next);
        },
        function addBar(ctx, next) {
            console.log('adding bar...');
            morayClient.putObject(TEST_BUCKET_NAME, uuidBar, {
                uuid: uuidBar,
                name: 'bar',
                valid: true
            }, {noBucketCache: true}, next);
        },
        function addAnotherBar(ctx, next) {
            console.log('adding second entry with name=bar...');
            morayClient.putObject(TEST_BUCKET_NAME, uuidBarBis, {
                uuid: uuidBarBis,
                name: 'bar',
                valid: true
            }, {noBucketCache: true}, next);
        },
        function searchForBarEntries(ctx, next) {
            var filter = '(name=bar)';
            var req;

            console.log('searching with filter [%s]', filter);

            req = morayClient.findObjects(TEST_BUCKET_NAME, filter);

            req.on('error', next);
            req.on('end', next);

            req.on('record', function onRecord(obj) {
                console.log('found: ', obj);
            });
        },
        function searchForValidBarEntries(ctx, next) {
            var filter = '&(valid=true)(name=bar)';
            var req;

            console.log('searching with filter [%s]', filter);

            req = morayClient.findObjects(TEST_BUCKET_NAME, filter);

            req.on('error', next);
            req.on('end', next);

            req.on('record', function onRecord(obj) {
                console.log('found: ', obj);
            });
        },
        function invalidateBar(ctx, next) {
            console.log('setting bar duplicate to valid=false');

            morayClient.putObject(TEST_BUCKET_NAME, uuidBarBis, {
                uuid: uuidBarBis,
                name: 'bar',
                valid: 'false'
            }, next);
        },
        function searchForValidBarEntriesAfterBarInvalidated(ctx, next) {
            var filter = '&(name=bar)(valid=true)';
            var req;

            console.log('searching with filter [%s] after setting bar to ' +
                'valid=false', filter);

            req = morayClient.findObjects(TEST_BUCKET_NAME, filter);

            req.on('error', next);
            req.on('end', next);

            req.on('record', function onRecord(obj) {
                console.log('found: ', obj);
            });
        },
        function addIndexOnValidField(ctx, next) {
            morayClient.updateBucket(TEST_BUCKET_NAME, TEST_BUCKET_CFG_V2,
                next);
        },
        function searchForValidBarEntriesAfterAddingIndex(ctx, next) {
            var filter = '&(name=bar)(valid=true)';
            var req;

            console.log('searching with filter [%s] after adding index on ' +
                '"valid" field', filter);

            req = morayClient.findObjects(TEST_BUCKET_NAME, filter);

            req.on('error', next);
            req.on('end', next);

            req.on('record', function onRecord(obj) {
                console.log('found: ', obj);
            });
        },
        function reindexBucket(ctx, next) {
            console.log('reindexing bucket...');

            function _reindex() {
                morayClient.reindexObjects(TEST_BUCKET_NAME, 100,
                    function onReindexed(err, res) {
                        if (err) {
                            next(err);
                            return;
                        }

                        if (res.processed === 0) {
                            next();
                            return;
                        }

                        _reindex();
                    });
            }

            _reindex();
        },
        function searchForValidBarEntriesAfterReindexingBucket(ctx, next) {
            var filter = '&(name=bar)(valid=true)';
            var req;

            console.log('searching with filter [%s] after reindexing bucket',
                filter);

            req = morayClient.findObjects(TEST_BUCKET_NAME, filter);

            req.on('error', next);
            req.on('end', next);

            req.on('record', function onRecord(obj) {
                console.log('found: ', obj);
            });
        }
    ]}, function allDone(err) {
        morayClient.close();
        if (err) {
            console.error('Error:', err);
        } else {
            console.log('all done!');
        }
    });
});