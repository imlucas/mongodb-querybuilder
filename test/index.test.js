var assert = require('assert'),
  query = require('../');

describe('QueryBuilder', function(){
  it('should should work', function(){
    var ns = 'xgen.jira';

    query.schema(ns, {
      'fields.reporter.name': {$type: 'string'},
      'fields.components.name': {$type: 'string'},
      'changelog.total': {$type: 'number'}
    });

    var pipeline = query(ns)
      .match('fields.reporter.name', ['thomasr', 'ramon.fernandez', 'spencer'])
      .match('fields.components.name', ['Security', 'Sharding'])
      .match('changelog.total', [0, 50])
      .pick('x-axis', 'fields.fixVersions.name')
      .pick('y-axis', 'changelog.total')
      .pick('color', 'fields.components.name')
      .sort('size', -1)
      .limit(5)
      .pipeline();

    var expected = [
      {
        "$match": {
          "fields.reporter.name": {
            "$in": [
              "thomasr",
              "ramon.fernandez",
              "spencer"
            ]
          },
          "fields.components.name": {
            "$in": [
              "Security",
              "Sharding"
            ]
          },
          "changelog.total": {
            "$gte": 0,
            "$lte": 50
          }
        }
      },
      {
        "$project": {
          "x-axis": "$fields.fixVersions.name",
          "y-axis": "$changelog.total",
          "color": "$fields.components.name"
        }
      },
      {
        "$sort": {
          "size": -1
        }
      },
      {
        "$limit": 5
      }
    ];

    assert(pipeline);
    assert.deepEqual(pipeline, expected);
  });
});
