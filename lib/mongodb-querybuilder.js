var DotObject = require('dot-object'),
            _ = require('underscore');

var dotobject = new DotObject();

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

QueryBuilder.prototype.match = function(field, values) {

    if (!values) {
        delete this._match[field];
    } else {
        if (!(values instanceof Array)) {
            values = [ values ];
        }      
        this._match[field] = values;
    }
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
    if (!fields) {
        this._group = null;
        return this;
    }

    if (!(fields instanceof Array)) {
        fields = [ fields ];
    } 
    this._group = {'slot': slot, 'fields': fields };
    return this;
};

QueryBuilder.prototype.schema = function(callback) {
    function inner() {
        callback(null, this._schema);
    };
    this._requireSchema(inner);
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

        // match
        match = {};
        for (field in this._match) {
            if (!this._match.hasOwnProperty(field)) continue;

            // strings are categories
            if (this._schema[field]['$type'] === 'string') {
                if (this._match[field].length === 1) {
                    // single matches, no $in
                    match[field] = this._match[field][0];
                } else {
                    // multiple matches, use $in
                    match[field] = { $in: this._match[field] };
                }
            } else if (['number', 'date'].indexOf(this._schema[field]['$type']) !== -1) {
                // console.log("QUANTITY")
                // assume length is always 2 (min and max)
                var min = this._match[field][0],
                    max = this._match[field][1];

                match[field] = { $gte: min, $lte: max };
            }
        }
        match = {$match: match};

        // project group fields (because dot-notation is not allowed in sub docs)
        var slots = this._group['fields'].slice(0);
        for (agg in this._aggs) {
            if (!this._aggs.hasOwnProperty(agg)) continue;
            slots.push( this._aggs[agg][Object.keys(this._aggs[agg])[0]] );
        };

        // build project object
        slots = slots.filter(function (s) { return (typeof s === 'string'); });
        var project = slots.reduce(function (prev, curr) {
            prev[curr.replace(/\./g, '_')] = '$' + curr;
            return prev;
        }, {});
        project = {$project: project};

        
        // group and aggs
        if (this._group) {
            
            if (this._group['fields'].length > 1) {
                // prep for compound group key
                var compound = {};

                // build compound group key
                this._group['fields'].forEach(function (f) {
                    if (typeof f === 'string') {
                        f = f.replace(/\./g, '_');
                    }
                    compound[f] = '$' + f;
                });
                var group = { _id: compound };
            } else {
                // single group key
                var group = { _id: '$'+this._group['fields'][0] };
            }
            
            // unwind group field if necessary
            this._unwind = [];
            this._group['fields'].forEach( _.bind(this._unwinder, this) );

            // add aggs to group
            for (slot in this._aggs) {
                if (!this._aggs.hasOwnProperty(slot)) continue;

                var value = this._aggs[slot][Object.keys(this._aggs[slot])[0]];
                var fn = Object.keys(this._aggs[slot])[0];
                                
                // unwind aggs automatically if necessary
                this._unwinder.apply(this, [ value ]);

                // add aggregations to group
                console.log("AGGS", this._aggs[slot], fn, value);
                var agg = {};
                if (typeof value === 'string') {
                    value = '$' + value.replace(/\./g, '_');
                }
                agg[fn] = value;
                group[slot] = agg;
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

        // rename _id to the group slot
        var rename = Object.keys(this._aggs).reduce(function (doc, curr) {
            doc[curr] = '$' + curr;
            return doc;
        }, {});
        
        // reconstruct the nested dotted fields
        console.log("GROUP", this._group['fields']);

        var _id = this._group['fields'].reduce(function (doc, curr) {
            doc[curr] = '$_id.' + curr.replace(/\./g, '_');
            return doc;
        }, {});

        console.log("_ID before", _id);
        dotobject.object(_id);
        console.log("_ID after", _id);
        rename[this._group['slot']] = _id;

        rename['_id'] = 0;
        rename = {'$project': rename};

        // combining it all
        var pipeline = [];
        pipeline = pipeline.concat(
            match,
            unwind,
            project,
            group,
            // sort,
            limit,
            rename
        ).filter(function (f) { return f !== null; });

        console.log("PIPELINE", JSON.stringify(pipeline, null, '\t'));

        // executing the pipeline
        this._mongoscope.aggregate("xgen.jira", pipeline, callback);
    }

    this._requireSchema(inner);
};


module.exports = QueryBuilder;
