var _ = require('underscore');

var QueryBuilder = function(options) {
    options = options || {};

    this._namespace = require('mongodb-ns')(options.namespace);
    this._mongoscope = require('mongoscope-client')({seed: options.seed || 'mongodb://localhost:10000'});
    this._schema = null;

    // get schema early on
    var _this = this;
    
    this._mongoscope.find("xgen.jira", {query: {"project": "SERVER"}, limit: 100, sort: {"_id": -1}}, function (err, docs) {
        if (err) throw err;    

        // analyse schema
        require('mongodb-schema').schema(docs, {flat: true}, function(err, schema) {
            if (err) throw err;    
            _this._schema = schema;
        });
    });

    this.reset();
};

QueryBuilder.prototype.reset = function () {
    this._match = {};
    this._slots = {};
    this._aggs = {};
    this._limit = 0;
    this._sort = [];
    this._group = null;
    this._unwind = [];
};

QueryBuilder.prototype.match = function(matchobj) {
    this._match = matchobj;
    return this;
};

QueryBuilder.prototype.agg = function(slot, agg, path) {
    var doc = {};
    doc[agg] = path;
    this._aggs[slot] = doc;

    return this;
};

QueryBuilder.prototype.limit = function(n) {
    this._limit = n;
    return this;
};

QueryBuilder.prototype.sort = function(slot, direction) {
    this._sort = [slot, direction];
    return this;
};

QueryBuilder.prototype.group = function(slot, fields) {
    if (!(fields instanceof Array)) {
        fields = [ fields ];
    } 
    this._group = fields;
    return this;
};

QueryBuilder.prototype._requireSchema = function(callback, args) {
    var _this = this;
    function wait() {
        if (_this._schema !== null) {
            callback.call(_this, args);
        } else {
            setTimeout(wait, 100);
        }
    }
    wait();
};

QueryBuilder.prototype._unwinder = function(path) {
    // get value from arbitrary key
    if (typeof path !== 'string') {
        return;
    }

    // check if field needs to be unwound
    var tokens = path.split('.');

    for (var i = 1; i < tokens.length + 1; i++) {
        var f = tokens.slice(0, i).join('.');

        // check if field is array, then add to list for unwind
        if ((typeof this._schema[f] === 'object') && (this._schema[f]['$array'] === true)) {
            this._unwind.push({'$unwind': '$' + f});
        }
    }
};

QueryBuilder.prototype.end = function(callback) {

    function inner() {
        var _this = this;

        // wrap match with $match
        var match = {$match: this._match};

        // project
        // this._project = 

        // group and aggs
        if (this._group) {
            if (this._group.length > 1) {
                // compound group key
                var compound = {};
                this._group.forEach(function (f) {
                    compound[f] = '$'+f;
                });
                var group = { _id: compound };
            } else {
                // single group key
                var group = { _id: '$'+this._group[0] };
            }

            this._unwind = [];
            
            // unwind group field if necessary
            this._group.forEach( _.bind(this._unwinder, this) );

            for (slot in this._aggs) {
                if (!this._aggs.hasOwnProperty(slot)) continue;
                
                // unwind aggs automatically if necessary
                var value = this._aggs[slot][Object.keys(this._aggs[slot])[0]];
                this._unwinder.apply(this, [ value ]);

                // add aggregations to group
                group[slot] = this._aggs[slot];
            }
            group = {$group: group};
            unwind = this._unwind;
        } 

        // var sort = null;
        // if (this._sort) {
        //     // sort / limit
        //     var s = this._sort;
        //     var k = this._aggs[s[0]];
        //     sort = {};
        //     sort[k[Object.keys(k)[0]]] = k[1];
            
        var limit = [ {$limit: this._limit} ];
        // }

        // combining it all
        var pipeline = [];
        pipeline = pipeline.concat(
            match,
            // project
            unwind,
            group,
            // sort,
            limit
        ).filter(function (f) { return f != null; });

        // executing the pipeline
        this._mongoscope.aggregate("xgen.jira", pipeline, callback);
    }

    this._requireSchema(inner);
};


module.exports = QueryBuilder;
