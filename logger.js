var bunyan = require('bunyan');
var log = bunyan.createLogger({name: "EM"});

log.level("info")

module.exports = log;
