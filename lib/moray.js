var assert = require('assert-plus');

function findObjectsWithFilter(morayClient, bucketName, filter, options, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.optionalNumber(options.requiredBucketVersion,
        'options.requiredBucketVersion');
    assert.func(callback, 'callback');

    var objects = [];
    var req;

    req = morayClient.findObjects(bucketName, filter, {
        requiredBucketVersion: options.requiredBucketVersion
    });

    req.on('error', function onError(err) {
        callback(err, objects);
    });

    req.on('end', function onEnd() {
        callback(null, objects);
    });

    req.on('record', function onRecord(obj) {
        objects.push(obj);
    });
}

function reindexBucket(morayClient, bucketName, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.func(callback, 'callback');

    console.log('reindexing bucket [%s]', bucketName);

    function _reindex() {
        morayClient.reindexObjects(bucketName, 100,
            function onReindexed(err, res) {
                if (err) {
                    callback(err);
                    return;
                }

                if (res.processed === 0) {
                    callback();
                    return;
                }

                _reindex();
            });
    }

    _reindex();
}

function createBucket(morayClient, bucketName, bucketCfg, callback) {
    assert.object(morayClient)
    assert.string(bucketName, 'bucketName');
    assert.object(bucketCfg, 'bucketCfg');
    assert.func(callback, 'callback');

    console.log('creating bucket [%s] with config [%j]', bucketName,
        bucketCfg);

    morayClient.createBucket(bucketName, bucketCfg, callback);
}

function putObject(morayClient, bucketName, objectParams, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.object(objectParams, 'objectParams');
    assert.func(callback, 'callback');

    console.log('putting object with params:', objectParams);
    /*
     * Use noBucketCache: true here because this program can run
     * frequently, and there's a possibility'of hitting a moray server
     * that has a now-deleted bucket's schema in its cache. If that's
     * the case, that server might use a version of the bucket schema
     * that has the property "valid" as an indexed property, and thus it
     * would try to put the value for that property in a PG column that
     * doesn't exist, which would generate an error.
     */
    morayClient.putObject(bucketName, objectParams.uuid, objectParams,
        {noBucketCache: true}, function onObjectPut(err) {
            callback(err);
        });
}

function deleteBucket(morayClient, bucketName, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.func(callback, 'callback');

    console.log('deleting bucket [%s]', bucketName);

    morayClient.deleteBucket(bucketName,
        function onBucketDeleted(delErr) {
            if (delErr && delErr.name !== 'BucketNotFoundError') {
                callback(delErr)
            } else {
                callback();
            }
        });
}

function searchForObjects(morayClient, bucketName, filter, options,
    expectedResults, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.object(expectedResults, 'expectedResults');
    assert.optionalNumber(expectedResults.nbObjectsExpected,
        'expectedResults.nbObjectsExpected');
    assert.func(callback, 'callback');

    console.log('searching objects with filter [%s] and options [%j]',
        filter, options);

    findObjectsWithFilter(morayClient, bucketName, filter, options,
        function objectsFound(err, objectsFound) {
            var nbObjectsExpected = expectedResults.nbObjectsExpected || 0;
            var nbObjectsFound = 0;

            assert.number(nbObjectsExpected, nbObjectsExpected);

            if (err) {
                callback(err);
                return;
            }

            if (objectsFound !== undefined) {
                nbObjectsFound = objectsFound.length;
            }

            var foundExpectedNbObjects = nbObjectsFound === nbObjectsExpected;

            if (foundExpectedNbObjects) {
                console.log('Found expected number of objects (%d)',
                    objectsFound.length);
            } else {
                console.log('Did not find expected number of objects ' +
                    '(%d found instead of %d expected)',
                        objectsFound.length, nbObjectsExpected);
            }

            assert.ok(objectsFound);
            assert.ok(foundExpectedNbObjects);

            if (expectedResults.expectedProperties) {
                expectedResults.expectedProperties.forEach(function (expectedProperty) {
                    var expectedPropertyName = expectedProperty.name;
                    var expectedPropertyValue = expectedProperty.value;

                    assert.string(expectedPropertyName,
                        'expectedPropertyName');

                    objectsFound.forEach(function checkObject(object) {
                        var value = object.value;
                        var foundExpectedPropertyValue =
                            value[expectedPropertyName] ===
                                expectedPropertyValue;
                        assert.ok(foundExpectedPropertyValue);
                    });
                });
            }

            callback();
        });
}

function updateBucket(morayClient, bucketName, bucketCfg, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.object(bucketCfg, 'bucketCfg');
    assert.func(callback, 'callback');

    console.log('updating bucket [%s] with config [%j]', bucketName,
        bucketCfg);

    morayClient.updateBucket(bucketName, bucketCfg,
        function onBucketUpdated(err) {
            callback(err);
        });
}

module.exports = {
    findObjectsWithFilter: findObjectsWithFilter,
    reindexBucket: reindexBucket,
    createBucket: createBucket,
    putObject: putObject,
    deleteBucket: deleteBucket,
    searchForObjects: searchForObjects,
    updateBucket: updateBucket
};