## Query Builder input and output

The query builder turns that:

```js
builder
    .match("fields.reporter.name", ["thomasr", "ramon.fernandez", "spencer"])
    .match("fields.components.name", ["Security", "Sharding"])
    .match("changelog.total", [10, 50])
    .group("x-axis", ["fields.fixVersions.name", "fields.status"])
        .agg("y-axis", "$sum", 1)
        .agg("size", "$avg", "changelog.total")
        .agg("_ids", "$push", "_id")
    .limit(5);
```

into that: 

```json
[
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
                "$gte": 10,
                "$lte": 50
            }
        }
    },
    {
        "$unwind": "$fields.fixVersions"
    },
    {
        "$project": {
            "fields_fixVersions_name": "$fields.fixVersions.name",
            "fields_status": "$fields.status",
            "changelog_total": "$changelog.total",
            "_id": "$_id"
        }
    },
    {
        "$group": {
            "_id": {
                "fields_fixVersions_name": "$fields_fixVersions_name",
                "fields_status": "$fields_status"
            },
            "y-axis": {
                "$sum": 1
            },
            "size": {
                "$avg": "$changelog_total"
            },
            "_ids": {
                "$push": "$_id"
            }
        }
    },
    {
        "$limit": 5
    },
    {
        "$project": {
            "y-axis": "$y-axis",
            "size": "$size",
            "_ids": "$_ids",
            "x-axis": "$_id",
            "_id": 0
        }
    }
]
```


Which one would you rather write?

The result of that aggregation pipeline looks like this:

```json
[
    {
        "y-axis": 1,
        "size": 10,
        "_ids": [
            "SERVER-9027"
        ],
        "x-axis": {
            "fields_fixVersions_name": "2.4.2",
            "fields_status": "Closed"
        }
    },
    {
        "y-axis": 1,
        "size": 46,
        "_ids": [
            "SERVER-4237"
        ],
        "x-axis": {
            "fields_fixVersions_name": "2.4.0-rc1",
            "fields_status": "Closed"
        }
    },
    {
        "y-axis": 1,
        "size": 11,
        "_ids": [
            "SERVER-13698"
        ],
        "x-axis": {
            "fields_fixVersions_name": "2.7.3",
            "fields_status": "Open"
        }
    },
    {
        "y-axis": 4,
        "size": 14.5,
        "_ids": [
            "SERVER-6591",
            "SERVER-7472",
            "SERVER-7493",
            "SERVER-7500"
        ],
        "x-axis": {
            "fields_fixVersions_name": "2.3.1",
            "fields_status": "Closed"
        }
    },
    {
        "y-axis": 1,
        "size": 11,
        "_ids": [
            "SERVER-13441"
        ],
        "x-axis": {
            "fields_fixVersions_name": "2.7.0",
            "fields_status": "Closed"
        }
    }
]
```
