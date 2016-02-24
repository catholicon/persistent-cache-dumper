Tool to dump [Jackrabbit Oak persistent cache][1] entries in text format.
Its based on [code][2] by Thomas

```
$ java -jar persistent-cache-dumper.jar cache-0.data
map,length,key
node,157,/jcr:system/jcr:versionStorage/56/d9@br1523fec1483-0-1
node,155,/jcr:system/jcr:versionStorage/56/d9@r1523fec53bb-3-1
node,157,/jcr:system/jcr:versionStorage/56/da@br1523fec1483-0-1
node,155,/jcr:system/jcr:versionStorage/56/da@r1523fec53bb-3-1
... 
```
 
[1]: http://jackrabbit.apache.org/oak/docs/nodestore/persistent-cache.html
[2]: https://issues.apache.org/jira/browse/OAK-2819