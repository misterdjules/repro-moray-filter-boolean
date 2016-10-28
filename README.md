This repository can be used to reproduce potential problems with moray search
filters that filter on indexed properties.

## How to run this code

1. `git clone` this repository

2. run `npm install`

3. edit `config.json` at the root of the repository and set `host` to the IP
address of the moray

4. run `node ./repro.js`.