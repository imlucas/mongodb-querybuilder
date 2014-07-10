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
        require('mongodb-schema')(docs, {flat: true}, function(err, schema) {
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
    this._sort = {};
    this._group = null;
};

QueryBuilder.prototype.match = function(matchobj) {
    this._match = matchobj;
    return this;
};

QueryBuilder.prototype.slot = function(slot, path) {
    this._slots[slot] = path;
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

QueryBuilder.prototype.sort = function(sort_doc) {
    this._sort = sort_doc;
    return this;
};

QueryBuilder.prototype.group = function(slot, fields) {
    if (fields instanceof Array) {
        // compound group key
        var compound = {};
        fields.forEach(function (f) {
            compound[f] = '$'+f;
        });
        fields = { _id: compound };
    } else {
        // single group key
        fields = { _id: '$'+fields };
    }
    this._group = fields;

    return this;
};


QueryBuilder.prototype._require_schema = function(callback, args) {
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

QueryBuilder.prototype._unwinder = function(obj) {

    // get value from arbitrary key
    var path = obj[Object.keys(obj)[0]];

    // check if field needs to be unwound
    var tokens = path.split('.');

    for (var i = 1; i < tokens.length + 1; i++) {
        var f = tokens.slice(0, i).join('.');
        
        // check if field is array, then add to list for unwind
        if (this._schema[f]['$array'] === true) {
            console.log(f, "is an array");
        }
    }


};

QueryBuilder.prototype.exec = function(callback) {
    var _this = this;

    // need to wait until schema inference completed


    // wrap match in $match + array
    match = [ {$match: this._match} ];

    // branch find vs. aggregate
    if (this._group) {

        // unwind aggs if necessary
        for (agg in this._aggs) {
            if (!this._aggs.hasOwnProperty(agg)) continue;
            this._require_schema(this._unwinder, _this._aggs[agg]);
        }

        // group / aggregate stage


    } else {
        // sanity check, if we don't group, no aggs allowed
        if (this._aggs !== {}) {
            throw Error("must specify group with agg.");
        }
    }


    // limit / sort

    // combining it all

    // executing the pipeline
};


module.exports = QueryBuilder;
