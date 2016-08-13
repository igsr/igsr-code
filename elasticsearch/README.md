This is a list of command lines we know are being used. We should be cautious about changing the mapping it it effects these commands:

Ensembl is using this query behind their tools:

    curl http://www.1000genomes.org/api/beta/file/_search -d '{"query":{"constant_score":{"filter":{"bool":{"must":[{"term":{"dataCollections":"1000 Genomes phase 3 release"}},{"term":{"dataType":"variants"}}]}}}}, "size":-1, "_source":["url"]}'
