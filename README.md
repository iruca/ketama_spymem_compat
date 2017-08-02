# ketama_spymem_compat
Ketama hash ring implementation compatible with spymemcached


## Usage:

```
from ketama import KetamaHashRing

>>> from ketama import KetamaHashRing
>>> ketama_nodes = KetamaHashRing(["example.com", "yahoo.com", "google.com"], 11211)
>>> print ketama_nodes.get_node_for_key( "cachekey1" )
yahoo.com
>>> print ketama_nodes.get_node_for_key( "cachekey2" )
example.com
>>>
```

## Description

Some are using Ketama algorithm of Spymemcached library to balance the load to memcached nodes.
We don't want to use Java or other compiler languages to test memcached's load-balancing function or to just check caches are correctly set, and I believe there is no python library which implements spymemcached-compatible Ketama algorithm other than this.

Using this python utility, you can easily find caches set by spymemcached from a bunch of memcached nodes....



Pull Requests welcome....
