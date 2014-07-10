var QueryBuilder = module.exports = require('./lib/mongodb-querybuilder.js');

var builder = new QueryBuilder({namespace: "xgen.jira", seed: "mongodb://localhost:10000"});

builder
    .match( {"fields.reporter.name": "thomasr"} )
    .group("x-axis", "fields.fixVersions.name")
    .agg("y-axis", "$sum", 1)
    .agg("ids", "$push", "$_id")
    .sort("watchers", 1)
    .limit(5);

builder.end(function (err, res) {
    if (err) return console.log("ERROR", err);
    console.log("SUCCESS", JSON.stringify(res, null, '\t'));
});


