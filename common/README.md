# Module: DbLoadgen Common

This module contains the code that is common to both the DbLoadgen server and the DbLoadgen
command line interface. This includes all of the logic for interfacing with
databases (schema and table creation, SQL generation, etc.), 
column initializers, and workload management (thread management, 
table initialization, transaction load generation, etc.).
