Programming Assignment #4 - Wikipedia Analysis with Apache Spark
================================================================
Evan Lin - elin13
Malina Jiang - malinaj

TopLinked
---------
The transformations for the TopLinked program were quite straightforward. After reading in the articles from the Wikipedia XML, we flattened all articles' link sets, grouped by article name, and used mapValues to map each article name to the number of times it was linked to. Afterwards, we used reduce to reduce the set of articles to the one which was referenced the greatest number of times.

PageRank
--------