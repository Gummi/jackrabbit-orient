jackrabbit-orientdb persistence manager
=======================================

stores a jackabbit model and structure in a human readable form to an orientdb backend

this is a more or less experimental persistence manager for jackrabbit. 
it can be run with the included jackrabbit performance tests.

it does not yet pass all of the jcr compliance tests. but most of them :)

instead of storing the nodes in serialized form, as normal jackrabbit pms do,
it utilizes the hybrid-schema feature of orientdb to store the nodes in a human readable form to the database

child relations are not created as edges, as this would need a two phase write in the persistence manager.


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
"out": ["#9:12"],
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

some performance results
========================
ReadPropertyTest                       min     10%     50%     90%     max

derby                                     46      46      47      49      55

orient                                    44      44      46      48      61

ConcurrentReadWriteTest                min     10%     50%     90%     max

derby                                     12      13      29     355    1362

orient                                    11      12      13     276     570

SetPropertyTest    

derby                                    212     213     246     423     505

orient                                   307     322     403     562     585

UpdateManyChildNodesTest               min     10%     50%     90%     max

derby                                      9      10      11      19      76

orient                                    57      58      89     105     339

BigFileReadTest                        min     10%     50%     90%     max

derby                                     82      85      86      96     118

orient                                    80      81      82      85     367
