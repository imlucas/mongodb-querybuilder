var assert = require('assert'),
  QueryBuilder = require('../');

describe('QueryBuilder', function(){
  var builder;

  before(function(done){
    // create query builder and point it to a mongodb instance and namespace
    builder = new QueryBuilder({seed: 'mongodb://localhost:10000', namespace: 'xgen.jira'});
    done();
  });

  it('should should work', function(done){
    // call .match, .group, .agg and .limit functions as much as you want
    // QueryBuilder will store the state according to the given 'slot' (first), overwrite
    // old values, and delete the slot if you pass in a value of null.
    builder
      .match('fields.reporter.name', ['thomasr', 'ramon.fernandez', 'spencer'])
      .match('fields.components.name', ['Security', 'Sharding'])
      .match('changelog.total', [0, 50])
      // .group('x-axis', ['fields.fixVersions.name', 'fields.status'])
      //     .agg('y-axis', '$sum', 1)
      //     .agg('size', '$avg', 'changelog.total')
      //     .agg('_ids', '$push', '_id')
      .pick('x-axis', 'fields.fixVersions.name')
      .pick('y-axis', 'changelog.total')
      .pick('color', 'fields.components.name')
      .sort('size', -1)
      .limit(5)
      // finally, call .end to send the aggregation pipeline and return the data
      .end(function(err, res){
        assert.ifError(err);
        console.log('DATA', JSON.stringify(res, null, '\t'));
      });
  });
});
