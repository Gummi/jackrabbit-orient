jackrabbit-orientdb persistence manager
=======================================

stores a jackabbit model and structure in a human readable form to an orientdb backend

this is a more or less experimental persistence manager for jackrabbit. It still fails with multithreaded apps.




instead of storing the nodes in serialized form, as normal jackrabbit pms do,
it utilizes the hybrid-schema feature of orientdb to store the nodes in a human readable form to the database

child relations are created as edges, so the the jcr tree structure is reflected in the orient grpah

room for improvement may be in the way the references get stored.

following is an simple example how the nodes get stored in the db

```json
{
  "result": [{
      "@type": "d", "@rid": "#10:0", "@version": 19, "@class": "TestBundle",
"uuid": "3f6aed81-7832-4a15-8914-1bb45b7f92fc", "primaryType":{
    "@type": "d", "@version": 0,
  "local": "test",
  "uri": ""
  },
"parentuuid": "cafebabe-cafe-babe-cafe-babecafebabe",
"modCount": 0,
"mixinTypes": [],
  "properties": [{
      "@type": "d", "@version": 0,
    "multiValued": false,
      "values": [{
          "@type": "d", "@version": 0,
        "type": 9
        }]
    }],
"sharedSet": [],
  "out": [
  "#9:12"],
  "@fieldTypes": "modCount=s"
    }, {
      "@type": "d", "@rid": "#10:1", "@version": 17, "@class": "TestBundle",
"uuid": "dbb614c8-a319-455a-bd11-1cc8bf1d1dcd", "primaryType":{
    "@type": "d", "@version": 0,
  "local": "test",
  "uri": ""
  },
"parentuuid": "3f6aed81-7832-4a15-8914-1bb45b7f92fc",
"modCount": 0,
"mixinTypes": [],
"properties": [],
"sharedSet": [],
  "in": [
  "#9:12"],
  "@fieldTypes": "modCount=s"
    }
  ]
}
```
