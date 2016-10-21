This repository can be used to reproduce a potential problem with moray search
filters.

## How to run this code

1. `git clone` this repository

2. run `npm install`

3. edit `config.json` at the root of the repository and set `host` to the IP
address of the moray

4. run `node ./repro.js`.

## Actual output

```
➜  repro-moray-filter-reindexing git:(master) ✗ node repro.js
deleting test bucket...
creating test bucket...
adding foo...
adding bar...
adding second entry with name=bar...
searching with filter [(name=bar)]
found:  { bucket: 'test_filter_reindexing',
  key: '893bd3c0-5ff1-406d-ae95-9c8a617dd123',
  value: 
   { uuid: '893bd3c0-5ff1-406d-ae95-9c8a617dd123',
     name: 'bar',
     valid: true },
  _id: 2,
  _etag: 'F9108B43',
  _mtime: 1477007737379,
  _txn_snap: null,
  _count: 2 }
found:  { bucket: 'test_filter_reindexing',
  key: '6a61be3c-58df-4104-a5fb-d61d61e1f227',
  value: 
   { uuid: '6a61be3c-58df-4104-a5fb-d61d61e1f227',
     name: 'bar',
     valid: true },
  _id: 3,
  _etag: '3740E4C0',
  _mtime: 1477007737384,
  _txn_snap: null,
  _count: 2 }
searching with filter [&(valid=true)(name=bar)]
setting bar duplicate to valid=false
searching with filter [&(name=bar)(valid=true)] after setting bar to valid=false
searching with filter [&(name=bar)(valid=true)] after adding index on "valid" field
reindexing bucket...
searching with filter [&(name=bar)(valid=true)] after reindexing bucket
found:  { bucket: 'test_filter_reindexing',
  key: '893bd3c0-5ff1-406d-ae95-9c8a617dd123',
  value: 
   { uuid: '893bd3c0-5ff1-406d-ae95-9c8a617dd123',
     name: 'bar',
     valid: true },
  _id: 2,
  _etag: 'F9108B43',
  _mtime: 1477007737379,
  _txn_snap: null,
  _count: 1 }
all done!
```

## Expected output

The following output:

```
searching with filter [&(valid=true)(name=bar)]
setting bar duplicate to valid=false
searching with filter [&(name=bar)(valid=true)] after setting bar to valid=false
searching with filter [&(name=bar)(valid=true)] after adding index on "valid" field
```

indicates that _before reindexing the bucket_ after the index on the `valid`
boolean property was added, searches using filters that include the `valid`
property do not match entries that should match.

After reindexing the bucket, the search works as expected and finds the only
entry with `valid === true` with a name of `'bar'`:

```
reindexing bucket...
searching with filter [&(name=bar)(valid=true)] after reindexing bucket
found:  { bucket: 'test_filter_reindexing',
  key: '893bd3c0-5ff1-406d-ae95-9c8a617dd123',
  value: 
   { uuid: '893bd3c0-5ff1-406d-ae95-9c8a617dd123',
     name: 'bar',
     valid: true },
  _id: 2,
  _etag: 'F9108B43',
  _mtime: 1477007737379,
  _txn_snap: null,
  _count: 1 }
```

## Current status of investigation

It seems that the part of [the filtering process that doesn't use
indexes](https://github.com/joyent/moray/blob/master/lib/objects/find.js#L147)
somehow doesn't work as expected for boolean value. Using values of type
"string" for the `valid` property/column works as expected.


