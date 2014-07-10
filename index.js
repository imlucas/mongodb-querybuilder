var QueryBuilder = module.exports = require('./lib/mongodb-querybuilder.js');

var builder = new QueryBuilder({namespace: "xgen.jira", seed: "mongodb://localhost:10000"});

builder
    .match({"fields.assignee": "thomasr"})
    .group("component", "fields.components.name")
    .agg("watchers", "$avg", "fields.watches.watchCount")
    .agg("fixVersion", "$sum", "fields.fixVersions.name")

console.log(builder);

builder.exec(function (err, res) {
    if (err) return console.log("ERROR", err);
    console.log("SUCCESS", res);
});

console.log("END");