# sparrow-server
Retrieves data from twitter based on provided parameters (Node.js)

##Init

`npm install` - To install all the required packages

Please make sure that your node.js version is the same as in `.nvmrc`

##Configuration
Don't forget to fill up the configs in `./config` folder!
The current environment file (production, test, develop, etc) would be merged by default with `default.json` config file. You'll get it merged into your application itself.

##Development
`npm test` - to run tests
`npm run lint` - to check code styles
`npm run develop` - to start develop process

###BEWARE OF RUNNING BIRDMAN TOO OFTEN!
There is restriction from twitter side. If you'll try to run birdman 
too often you can be banned for 15 minutes.