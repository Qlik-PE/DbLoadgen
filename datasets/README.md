# Provided Datasets

These are some sample datasets and workloads that can be used as is or simply as examples
when creating your own workloads.

## Dataset: Baseball

This dataset contains 27 tables and approximately 500,000 rows of historical baseball 
statistics. The raw data is interesting from a downstream perspective in that it is 
well-suited for analytics when crafting an end-to-end demonstration.

The sample data used in this dataset comes from the 
[Sean Lahman Baseball Archive](http://www.seanlahman.com/baseball-archive/statistics){:target="_blank"},
Copyright 1996-2018 by Sean Lahman. It is licensed for use under a
[Creative Commons Attribution-ShareAlike 3.0 Unported License](http://creativecommons.org/licenses/by-sa/3.0/){:target="_blank"} 
([legalcode](https://creativecommons.org/licenses/by-sa/3.0/legalcode){:target="_blank"}).



## Dataset: sap

This dataset is "SAP-like" in structure and contains some sample data. It contains 27 tables, 
some of which are rather wide (over 100 columns).


## Dataset: test

This is a simple dataset that is useful for smoke testing connections. Its simplicity  also 
makes it useful for testing code changes against h2. This dataset is not located here, but rather
stored as a resource in the "common" module.
