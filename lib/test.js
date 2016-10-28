var assert = require('assert-plus');
var libuuid = require('libuuid');
var vasync = require('vasync');

var morayTools = require('./moray');

var VALID_INDEX_TYPES = ['number', 'boolean', 'string'];

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

function testFilterWithExistingValues(morayClient, options, callback) {
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

    var uuidFoo = libuuid.create();
    var uuidBar = libuuid.create();
    var uuidBarBis = libuuid.create();

    vasync.waterfall([
        morayTools.deleteBucket.bind(null, morayClient, TEST_BUCKET_NAME),
        morayTools.createBucket.bind(null, morayClient, TEST_BUCKET_NAME,
            TEST_BUCKET_CFG_V1),
        morayTools.putObject.bind(null, morayClient, TEST_BUCKET_NAME, {
            uuid: uuidFoo,
            name: 'foo',
            newly_indexed_property: NEW_INDEXED_FIELD_SENTINEL_VALUE
        }),
        morayTools.putObject.bind(null, morayClient, TEST_BUCKET_NAME, {
            uuid: uuidBar,
            name: 'bar',
            newly_indexed_property: NEW_INDEXED_FIELD_SENTINEL_VALUE
        }),
        morayTools.putObject.bind(null, morayClient, TEST_BUCKET_NAME, {
            uuid: uuidBarBis,
            name: 'bar',
            newly_indexed_property: NEW_INDEXED_FIELD_SENTINEL_VALUE
        }),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
            FILTER_NAME_EQ_BAR,
            {
                requiredBucketVersion: 1
            },
            {
                nbObjectsExpected: 2,
                expectedProperties: [
                    {
                        name: 'name',
                        value: 'bar'
                    }
                ]
            }),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
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
                nbObjectsExpected: options.newIndexedFieldType === 'string' ? 2 : 0
            }),
        morayTools.putObject.bind(null, morayClient, TEST_BUCKET_NAME, {
            uuid: uuidBarBis,
            name: 'bar',
            newly_indexed_property: NEW_INDEXED_FIELD_NON_SENTINEL_VALUE
        }),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
            FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR, {
            requiredBucketVersion: 1
        }, {
            nbObjectsExpected: options.newIndexedFieldType === 'string' ? 1 : 0
        }),
        morayTools.updateBucket.bind(null, morayClient, TEST_BUCKET_NAME,
            TEST_BUCKET_CFG_V2),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
            FILTER_NAME_EQ_BAR,
            {
                requiredBucketVersion: 2
            }, {
                nbObjectsExpected: 2,
                expectedProperties: [
                    {
                        name: 'name',
                        value: 'bar'
                    }
                ]
            }),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
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
            nbObjectsExpected: 1,
            expectedProperties: [
                {
                    name: 'newly_indexed_property',
                    value: NEW_INDEXED_FIELD_SENTINEL_VALUE
                }
            ]
        }),
        morayTools.reindexBucket.bind(null, morayClient, TEST_BUCKET_NAME),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
            FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR, {
            requiredBucketVersion: 2
        }, {
            /*
             * After reindexing is complete, we expect the same results from
             * before reindexing is complete.
             */
            nbObjectsExpected: 1,
            expectedProperties: [
                {
                    name: 'newly_indexed_property',
                    value: NEW_INDEXED_FIELD_SENTINEL_VALUE
                }
            ]
        }),
    ], callback);
}

function testFilterWithNewValues(morayClient, options, callback) {
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

    var uuidFoo = libuuid.create();
    var uuidBar = libuuid.create();
    var uuidBarBis = libuuid.create();

    vasync.waterfall([
        morayTools.deleteBucket.bind(null, morayClient, TEST_BUCKET_NAME),
        morayTools.createBucket.bind(null, morayClient, TEST_BUCKET_NAME,
            TEST_BUCKET_CFG_V1),
        morayTools.putObject.bind(null, morayClient, TEST_BUCKET_NAME, {
            uuid: uuidFoo,
            name: 'foo'
        }),
        morayTools.putObject.bind(null, morayClient, TEST_BUCKET_NAME, {
            uuid: uuidBar,
            name: 'bar'
        }),
        morayTools.putObject.bind(null, morayClient, TEST_BUCKET_NAME, {
            uuid: uuidBarBis,
            name: 'bar'
        }),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
            FILTER_NAME_EQ_BAR,
            {
                requiredBucketVersion: 1
            },
            {
                nbObjectsExpected: 2,
                expectedProperties: [
                    {
                        name: 'name',
                        value: 'bar'
                    }
                ]
            }),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
            FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR, {
            requiredBucketVersion: 1
        }, {
            nbObjectsExpected: 0
        }),
        morayTools.updateBucket.bind(null, morayClient, TEST_BUCKET_NAME,
            TEST_BUCKET_CFG_V2),
        morayTools.putObject.bind(null, morayClient, TEST_BUCKET_NAME, {
            uuid: uuidBarBis,
            name: 'bar',
            newly_indexed_property: NEW_INDEXED_FIELD_SENTINEL_VALUE
        }),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
            FILTER_NAME_EQ_BAR,
            {
                requiredBucketVersion: 2
            }, {
                nbObjectsExpected: 2,
                expectedProperties: [
                    {
                        name: 'name',
                        value: 'bar'
                    }
                ]
            }),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
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
            nbObjectsExpected: 1,
            expectedProperties: [
                {
                    name: 'newly_indexed_property',
                    value: NEW_INDEXED_FIELD_SENTINEL_VALUE
                }
            ]
        }),
        morayTools.reindexBucket.bind(null, morayClient, TEST_BUCKET_NAME),
        morayTools.searchForObjects.bind(null, morayClient, TEST_BUCKET_NAME,
            FILTER_NEW_PROPERTY_AND_NAME_EQ_BAR, {
            requiredBucketVersion: 2
        }, {
            /*
             * After reindexing is complete, we expect the same results from
             * before reindexing is complete.
             */
            nbObjectsExpected: 1,
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