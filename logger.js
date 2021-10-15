var bunyan = require('bunyan');
var log = bunyan.createLogger({name: "EM"});

log.level("trace")

module.exports = log;
