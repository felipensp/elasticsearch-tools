# elasticsearch-tools
Elasticsearch tools for local gateway one


## es-copy

Copy entire document to another document in same host or remote:

`./es-copy.py --index index/doc1 --to index/doc2 --input localhost:9200 --output 10.0.0.20:9200`
