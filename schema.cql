CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION =
    { 'class' : 'NetworkTopologyStrategy',
        'replication_factor' : 3 };
USE demo;