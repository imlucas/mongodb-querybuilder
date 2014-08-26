var debug = require('debug')('mongodb-querybuilder:builder');

module.exports = Builder;

function Builder(ns) {
  if(!(this instanceof Builder)) return new Builder(ns);
  this.schema = Builder.schema(ns);
  if(!this.schema){
    throw new TypeError('No schema registered for `'+ns+'`');
  }
  this.reset();
}

Builder._schemas = {};

Builder.schema = function(ns, def){
  if(!def) return Builder._schemas[ns];
  Builder._schemas[ns] = def;
};

Builder.prototype.reset = function () {
  this._match = {};
  this._slots = {};
  this._aggs = {};
  this._limit = 0;
  this._sort = [];
  this._group = null;
  this._unwind = [];
  this._pick = {};

  this._picked = false;
  this._grouped = false;

  this.errors = [];
};

Builder.prototype.match = function(field, values) {
  if (!values) {
    delete this._match[field];
    return this;
  }

  if (!Array.isArray(values)) values = [ values ];

  this._match[field] = values;
  return this;
};

Builder.prototype.pick = function(key, path) {
  this._picked = true;
  if (!path) {
    delete this._pick[key];
    return this;
  }

  this._pick[key] = path;
  return this;
};

Builder.prototype.agg = function(slot, agg, path){
  this._grouped = true;

  var doc = {};
  doc[agg] = path;
  this._aggs[slot] = doc;
  return this;
};

Builder.prototype.limit = function(n) {
  this._limit = n;
  return this;
};

Builder.prototype.sort = function(key, direction) {
  this._sort = {};
  this._sort[key] = direction;
  return this;
};

Builder.prototype.group = function(slot, fields) {
  this._grouped = true;

  if (!fields) {
  this._group = null;
  return this;
  }

  if (!Array.isArray(fields)) fields = [fields];

  this._group = {'slot': slot, 'fields': fields };
  return this;
};

/**
 * Based on schema, automatically add $unwind conditions.
 *
 * @todo: chatted with @redbeard and he confirmed unneccessary $unwinds are
 * expensive.
 */
Builder.prototype.$unwind = function(path){
  // get value from arbitrary key
  if (typeof path !== 'string') return;

  // check if field needs to be unwound
  var tokens = path.split('.');

  for (var i = 1; i < tokens.length + 1; i++) {
    var f = tokens.slice(0, i).join('.');
    // check if field is array, then add to list for unwind
    if ((typeof this.schema[f] === 'object') && (this.schema[f].$array === true)) {
      this._unwind.push({'$unwind': '$' + f});
    }
  }
};

/**
 * Create $match phrase.
 */
Builder.prototype.$match = function(){
  var match = {}, field;
  for (field in this._match) {
    if (!this._match.hasOwnProperty(field)) continue;

    if(!this.schema[field]) throw new Error('Field `'+field+'` not defined in schema');
    // strings are categories
    if (this.schema[field].$type === 'string'){
      // single matches, no $in
      if (this._match[field].length === 1) {
        match[field] = this._match[field][0];
      }
      // multiple matches, use $in
      else {
        match[field] = { $in: this._match[field] };
      }
    }
    // assume length is always 2 (min and max)
    else if (['number', 'date'].indexOf(this.schema[field].$type) !== -1) {
      var min = this._match[field][0],
        max = this._match[field][1];

      match[field] = { $gte: min, $lte: max };
    }
  }
  return {$match: match};
};

Builder.prototype.validate = function(){
  // sanity check
  if (this._grouped && this._picked){
    throw new Error('can\'t use .pick() and .group() / .agg() together.');
  }
  return this;
};

Builder.prototype.pipeline = function(){
  this.validate();

  var match = this.$match(),
    project = {},
    group = null,
    rename = null;

  // group and aggs
  if (this._group) {
    // project group fields (because dot-notation is not allowed in sub docs)
    var slots = this._group.fields.slice(0);

    for (var k in this._aggs) {
      if (!this._aggs.hasOwnProperty(k)) continue;
      slots.push( this._aggs[k][Object.keys(this._aggs[k])[0]] );
    }

    // build project object
    slots = slots.filter(function (s) {
      return (typeof s === 'string');
    });

    project = slots.reduce(function (prev, curr) {
      prev[curr.replace(/\./g, '_')] = '$' + curr;
      return prev;
    }, {});

    project = {$project: project};

    if (this._group.fields.length > 1) {
      // prep for compound group key
      var compound = {};

      // build compound group key
      this._group.fields.forEach(function (f) {
        f = f.replace(/\./g, '_');
        compound[f] = '$' + f;
      });
      group = {_id: compound};
    }
    // single group key
    else {
      group = { _id: '$'+this._group.fields[0].replace(/\./g, '_') };
    }

    // unwind group field if necessary
    this._unwind = [];
    this._group.fields.map(this.$unwind.bind(this));

    // add aggs to group
    var slot;

    for (slot in this._aggs) {
      if (!this._aggs.hasOwnProperty(slot)) continue;

      var getter = Object.keys(this._aggs[slot])[0],
        path = this._aggs[slot][getter];

      // unwind aggs automatically if necessary
      this.$unwind(path);

      // add aggregations to group
      var agg = {};
      if (typeof value === 'string') {
        path = '$' + path.replace(/\./g, '_');
      }
      agg[getter] = path;
      group[slot] = agg;
    }
    group = {$group: group};

    // rename _id to the group slot
    rename = Object.keys(this._aggs).reduce(function (doc, curr) {
    doc[curr] = '$' + curr;
    return doc;
    }, {});

    rename[this._group.slot] = '$_id';
    rename._id = 0;
    rename = {'$project': rename};

  } else {
    // project picked values
    for (var key in this._pick) {
      if (!this._pick.hasOwnProperty(key)) continue;

      debug('pick %s', key);
      var val = this._pick[key];

      // unwind if necessary
      this.$unwind(val);

      // then project
      project[key] = '$' + val;
    }
    project = {$project: project};
    debug('project %j', project);
  }

  // === assembling the pipeline ===
  var pipeline = [];
  pipeline = pipeline.concat(
    match,
    this._unwind,
    project,
    group,
    {$sort: this._sort},
    [{$limit: this._limit}],
    rename
  ).filter(function (f) { return f !== null; });

  debug('pipeline %j', pipeline);
  return pipeline;
};
