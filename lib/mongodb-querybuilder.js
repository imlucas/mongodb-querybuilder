var _ = require('underscore');

var QueryBuilder = function(options) {
    options = options || {};

    this._namespace = require('mongodb-ns')(options.namespace);
    this._mongoscope = require('mongoscope-client')(options);
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
    this._pick = {};

    this._picked = false;
    this._grouped = false;
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

QueryBuilder.prototype.pick = function(slot, path) {
    this._picked = true;
    if (!path) {
        delete this._pick[slot];
    } else {
        this._pick[slot] = path;
    }
    return this;
};

QueryBuilder.prototype.agg = function(slot, agg, path) {
    this._grouped = true;

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
    this._sort = {"slot": slot, "direction": direction};
    return this;
};

QueryBuilder.prototype.group = function(slot, fields) {
    this._grouped = true;

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

        // sanity check
        if (this._grouped && this._picked) {
            throw Error("can't use .pick() and .group() / .agg() together.");
        }

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
                // assume length is always 2 (min and max)
                var min = this._match[field][0],
                    max = this._match[field][1];

                match[field] = { $gte: min, $lte: max };
            }
        }
        match = {$match: match};
        
        // group and aggs
        if (this._group) {
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

            
            if (this._group['fields'].length > 1) {
                // prep for compound group key
                var compound = {};

                // build compound group key
                this._group['fields'].forEach(function (f) {
                    f = f.replace(/\./g, '_');
                    compound[f] = '$' + f;
                });
                var group = { _id: compound };
            } else {
                // single group key
                var group = { _id: '$'+this._group['fields'][0].replace(/\./g, '_') };
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
                var agg = {};
                if (typeof value === 'string') {
                    value = '$' + value.replace(/\./g, '_');
                }
                agg[fn] = value;
                group[slot] = agg;
            }
            group = {$group: group};

            // rename _id to the group slot
            var rename = Object.keys(this._aggs).reduce(function (doc, curr) {
                doc[curr] = '$' + curr;
                return doc;
            }, {});

            rename[this._group['slot']] = '$_id';
            rename['_id'] = 0;
            rename = {'$project': rename};

        } else {

            project = {};
            group = null;
            rename = null;

            // project picked values
            for (slot in this._pick) {
                if (!this._pick.hasOwnProperty(slot)) continue;

                console.log("PICK", slot);
                var value = this._pick[slot];

                // unwind if necessary
                this._unwinder.apply(this, [ value ]);

                // then project
                project[slot] = '$' + value;
            }
            project = {$project: project};
            console.log("PROJECT", project);
        }

        // unwind
        unwind = this._unwind;

        // sort
        var sort = null;
        if (this._sort) {
            sort = {};
            sort[this._sort['slot']] = this._sort['direction'];
        }
        sort = {$sort: sort};
         
        // limit   
        var limit = [ {$limit: this._limit} ];


        // === assembling the pipeline ===
        var pipeline = [];
        pipeline = pipeline.concat(
            match,
            unwind,
            project,
            group,
            sort,
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
