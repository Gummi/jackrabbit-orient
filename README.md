jackrabbit-orientdb persistence manager
=======================================

stores a jackabbit model and structure in a human readable form to an orientdb backend

this is a more or less experimental persistence manager for jackrabbit.

instead of storing the nodes in serialized form, as normal jackrabbit pms do,
it utilizes the hybrid-schema feature of orientdb to store the nodes in a human readable form to the database

child relations are created as edges, so the the jcr tree structure is reflected in the orient grpah

room for improvement may be in the way the references get stored.

