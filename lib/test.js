var assert = require('assert-plus');
var libuuid = require('libuuid');
var util = require('util');
var vasync = require('vasync');

var morayTools = require('./moray');

var VALID_INDEX_TYPES = ['number', 'boolean', 'string'];
var NB_TOTAL_OBJECTS = 2;

var NB_OBJECTS_FOO = NB_TOTAL_OBJECTS / 2;
assert.ok(NB_OBJECTS_FOO > 0, 'NB_OBJECTS_FOO must be > 0');

var NB_OBJECTS_BAR = NB_TOTAL_OBJECTS / 2;
assert.ok(NB_OBJECTS_BAR > 0, 'NB_OBJECTS_BAR must be > 0');

function getSentinelValueForType(typeName) {
    assert.string(typeName, 'typeName');

    switch (typeName) {
        case 'string':
            return 'sentinel';
        case 'boolean':
            return true;
        case 'number':
            return 42;
        default:
            assert(false, 'unsupported type: ' + typeName);
    }
}

function getNonSentinelValueForType(typeName) {
    assert.string(typeName, 'typeName');

    switch (typeName) {
        case 'string':
            return 'nonSentinel';
        case 'boolean':
            return false;
        case 'number':
            return 24;
        default:
            assert(false, 'unsupported type: ' + typeName);
    }
}

function searchForObjects(t, morayClient, bucketName, filter, options,
    expectedResults, callback) {
    assert.object(t, 't');
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

    morayTools.findObjectsWithFilter(morayClient, bucketName, filter, options,
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

            t.strictEqual(nbObjectsFound, nbObjectsExpected,
                util.format('%d/%d objects found', nbObjectsFound,
                    nbObjectsExpected));

            if (expectedResults.expectedProperties) {
                expectedResults.expectedProperties.forEach(function (expectedProperty) {
                    var expectedPropertyName = expectedProperty.name;
                    var expectedPropertyValue = expectedProperty.value;
                    var allValuesMatch = false;

                    assert.string(expectedPropertyName,
                        'expectedPropertyName');

                    allValuesMatch =
                        objectsFound.some(function checkObject(object) {
                        var value = object.value;
                        return value[expectedPropertyName] ===
                            expectedPropertyValue;
                    });

                    t.ok(allValuesMatch, 'all values for property ' +
                        expectedPropertyName + ' match expected value ' +
                            expectedPropertyValue);
                });
            }

            callback();
        });
}

function addObjects(morayClient, bucketName, objectTemplate, nbObjects,
    callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.object(objectTemplate, 'objectTemplate');
    assert.number(nbObjects, 'nbObjects');
    assert.func(callback, 'callback');

    var totalNbObjectsCreated = 0;
    var ADD_CONCURRENCY = 100;

    function _addObjects() {
        var i = 0;
        var keys = [];
        var nbObjectsToCreate =
            Math.min(nbObjects - totalNbObjectsCreated, ADD_CONCURRENCY);

        if (nbObjectsToCreate === 0) {
            callback();
            return;
        }

        for (i = 0; i < nbObjectsToCreate; ++i) {
            keys.push(libuuid.create());
        }

        vasync.forEachParallel({
            func: function addObject(key, done) {
                morayClient.putObject(bucketName, key, objectTemplate,
                    function onObjectAdded(addErr) {
                        var nonTransientErrorNames = [
                            'InvalidIndexTypeError',
                            'UniqueAttributeError'
                        ];

                        if (addErr &&
                            nonTransientErrorNames.indexOf(addErr.name)
                                !== -1) {
                            done(addErr);
                            return;
                        }

                        if (!addErr) {
                            ++totalNbObjectsCreated;
                        }

                        done();
                    });
            },
            inputs: keys
        }, function onObjectsAdded(err) {
            if (err) {
                callback(err);
            } else {
                setImmediate(_addObjects);
            }
        });
    }

    _addObjects();
}

function testFilterWithExistingValues(t, morayClient, options, callback) {
    assert.object(t, 't');
    assert.object(morayClient, 'morayClient');
    assert.object(options, 'options');
    assert.string(options.newIndexedFieldType, 'options.newIndexedFieldType');

    var newIndexedFieldType = options.newIndexedFieldType;
    assert.ok(VALID_INDEX_TYPES.indexOf(newIndexedFieldType) !== -1,
        'options.newIndexedFieldType must be one of the following values: ' +
            VALID_INDEX_TYPES.join(', '));

    var TEST_BUCKET_NAME = 'test_filter_' + newIndexedFieldType +
        '_existing_values';

    var NEW_INDEXED_FIELD_SENTINEL_VALUE =
        getSentinelValueForType(newIndexedFieldType);

    var NEW_INDEXED_FIELD_NON_SENTINEL_VALUE =
        getNonSentinelValueForType(newIndexedFieldType);

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
    };

    var TEST_BUCKET_CFG_V2 = {
        index: {
            uuid: {
                type: 'string',
                unique: true
            },
            name: {
                type: 'string'
            },
            newly_indexed_property: {
                type: options.newIndexedFieldType
            }
        },
        options: {
            version: 2
        }
    };

    var FILTER_NAME_EQ_BAR = '(name=bar)';
    var FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR =
        '(&(newly_indexed_property=' + NEW_INDEXED_FIELD_SENTINEL_VALUE +
            ')(name=bar))';

    vasync.waterfall([
        morayTools.deleteBucket.bind(null, morayClient, TEST_BUCKET_NAME),
        morayTools.createBucket.bind(null, morayClient, TEST_BUCKET_NAME,
            TEST_BUCKET_CFG_V1),
        addObjects.bind(null, morayClient, TEST_BUCKET_NAME, {
            name: 'foo',
            newly_indexed_property: NEW_INDEXED_FIELD_SENTINEL_VALUE
        }, NB_OBJECTS_FOO),
        addObjects.bind(null, morayClient, TEST_BUCKET_NAME, {
            name: 'bar',
            newly_indexed_property: NEW_INDEXED_FIELD_SENTINEL_VALUE
        }, NB_OBJECTS_BAR),
        searchForObjects.bind(null, t, morayClient, TEST_BUCKET_NAME,
            FILTER_NAME_EQ_BAR,
            {
                requiredBucketVersion: 1
            },
            {
                nbObjectsExpected: NB_OBJECTS_BAR,
                expectedProperties: [
                    {
                        name: 'name',
                        value: 'bar'
                    }
                ]
            }),
        searchForObjects.bind(null, t, morayClient, TEST_BUCKET_NAME,
            FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR,
            {
                requiredBucketVersion: 1
            }, {
                /*
                 * We expect to find no object using a filter that filters on an
                 * unindexed property, even though there are 2 objects that
                 * match that filter. The reason is that in the case of a
                 * non-indexed non-string property, the filter -- which is
                 * passed to findobjects as a string -- cannot be updated with
                 * the proper type (boolean) because the buckets_config table
                 * doesn't have the property listed as an index with its type.
                 */
                nbObjectsExpected: options.newIndexedFieldType === 'string' ?
                    NB_OBJECTS_BAR : 0
            }),
        morayTools.updateBucket.bind(null, morayClient, TEST_BUCKET_NAME,
            TEST_BUCKET_CFG_V2),
        searchForObjects.bind(null, t, morayClient, TEST_BUCKET_NAME,
            FILTER_NAME_EQ_BAR,
            {
                requiredBucketVersion: 2
            }, {
                nbObjectsExpected: NB_OBJECTS_BAR,
                expectedProperties: [
                    {
                        name: 'name',
                        value: 'bar'
                    }
                ]
            }),
        searchForObjects.bind(null, t, morayClient, TEST_BUCKET_NAME,
            FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR, {
            requiredBucketVersion: 2
        }, {
            /*
             * Now that newly_indexed_property has been added as an indexed
             * property, even though reindexing is not complete, the type of
             * that attribute is present in the buckets_config table, and so
             * moray is able to update the string filter passed to findobjects
             * to have the proper type for that property.
             */
            nbObjectsExpected: NB_OBJECTS_BAR,
            expectedProperties: [
                {
                    name: 'newly_indexed_property',
                    value: NEW_INDEXED_FIELD_SENTINEL_VALUE
                }
            ]
        }),
        morayTools.reindexBucket.bind(null, morayClient, TEST_BUCKET_NAME),
        searchForObjects.bind(null, t, morayClient, TEST_BUCKET_NAME,
            FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR, {
            requiredBucketVersion: 2
        }, {
            /*
             * After reindexing is complete, we expect the same results from
             * before reindexing is complete.
             */
            nbObjectsExpected: NB_OBJECTS_BAR,
            expectedProperties: [
                {
                    name: 'newly_indexed_property',
                    value: NEW_INDEXED_FIELD_SENTINEL_VALUE
                }
            ]
        }),
    ], callback);
}

function testFilterWithNewValues(t, morayClient, options, callback) {
    assert.object(t, 't');
    assert.object(morayClient, 'morayClient');
    assert.object(options, 'options');
    assert.string(options.newIndexedFieldType, 'options.newIndexedFieldType');

    var newIndexedFieldType = options.newIndexedFieldType;
    assert.ok(VALID_INDEX_TYPES.indexOf(newIndexedFieldType) !== -1,
        'options.newIndexedFieldType must be one of the following values: ' +
            VALID_INDEX_TYPES.join(', '));

    var TEST_BUCKET_NAME = 'test_filter_' + newIndexedFieldType +
        '_new_values';

    var NEW_INDEXED_FIELD_SENTINEL_VALUE =
        getSentinelValueForType(newIndexedFieldType);

    var NEW_INDEXED_FIELD_NON_SENTINEL_VALUE =
        getNonSentinelValueForType(newIndexedFieldType);

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
    };

    var TEST_BUCKET_CFG_V2 = {
        index: {
            uuid: {
                type: 'string',
                unique: true
            },
            name: {
                type: 'string'
            },
            newly_indexed_property: {
                type: options.newIndexedFieldType
            }
        },
        options: {
            version: 2
        }
    };

    var FILTER_NAME_EQ_BAR = '(name=bar)';
    var FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR =
        '(&(newly_indexed_property=' + NEW_INDEXED_FIELD_SENTINEL_VALUE +
            ')(name=bar))';

    vasync.waterfall([
        morayTools.deleteBucket.bind(null, morayClient, TEST_BUCKET_NAME),
        morayTools.createBucket.bind(null, morayClient, TEST_BUCKET_NAME,
            TEST_BUCKET_CFG_V1),
        morayTools.updateBucket.bind(null, morayClient, TEST_BUCKET_NAME,
            TEST_BUCKET_CFG_V2),
        addObjects.bind(null, morayClient, TEST_BUCKET_NAME, {
            name: 'foo',
            newly_indexed_property: NEW_INDEXED_FIELD_SENTINEL_VALUE
        }, NB_OBJECTS_FOO),
        addObjects.bind(null, morayClient, TEST_BUCKET_NAME, {
            name: 'bar',
            newly_indexed_property: NEW_INDEXED_FIELD_SENTINEL_VALUE
        }, NB_OBJECTS_BAR),
        searchForObjects.bind(null, t, morayClient, TEST_BUCKET_NAME,
            FILTER_NAME_EQ_BAR,
            {
                requiredBucketVersion: 2
            }, {
                nbObjectsExpected: NB_OBJECTS_BAR,
                expectedProperties: [
                    {
                        name: 'name',
                        value: 'bar'
                    }
                ]
            }),
        searchForObjects.bind(null, t, morayClient, TEST_BUCKET_NAME,
            FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR, {
            requiredBucketVersion: 2
        }, {
            /*
             * Now that newly_indexed_property has been added as an indexed
             * property, and that we put one object with a value
             * NEW_INDEXED_FIELD_SENTINEL_VALUE for the property
             * newly_indexed_property _after_ the index was added, moray put
             * that value into the new table's column created for that indexed
             * property. When searching for objects using a filter that includes
             * that property, moray omits that table column from the where
             * clause of the SQL query used to find the objects, but it's able
             * to fix the filter passed as a string to update it with the proper
             * property type.
             */
            nbObjectsExpected: NB_OBJECTS_BAR,
            expectedProperties: [
                {
                    name: 'newly_indexed_property',
                    value: NEW_INDEXED_FIELD_SENTINEL_VALUE
                }
            ]
        }),
        morayTools.reindexBucket.bind(null, morayClient, TEST_BUCKET_NAME),
        searchForObjects.bind(null, t, morayClient, TEST_BUCKET_NAME,
            FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR, {
            requiredBucketVersion: 2
        }, {
            /*
             * After reindexing is complete, we expect the same results from
             * before reindexing is complete.
             */
            nbObjectsExpected: NB_OBJECTS_BAR,
            expectedProperties: [
                {
                    name: 'newly_indexed_property',
                    value: NEW_INDEXED_FIELD_SENTINEL_VALUE
                }
            ]
        }),
    ], callback);
}

module.exports = {
    testFilterWithExistingValues: testFilterWithExistingValues,
    testFilterWithNewValues: testFilterWithNewValues
};