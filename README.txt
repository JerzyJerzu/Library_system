This project was done for the Big Data university course. The aim was to create a application for book reservations in library, that will use a distributed datbase.
The requirements:
- At least 3-4 node working in project.
- Application can work on every node in cluster
- In project you have to be able to Make reservation, update reservation, see reservation, see reservation and who made it, reservation should have id.
- Stress Test 1: The client makes the same request very quickly min (10000 times).
- Stress Test 2: Two or more clients make the possible requests randomly (10000 times).
- Stress Test 3: Immediate occupancy of all seats/reservations by 2 clients.
To run the application locally and test how it works you need to have docker installed, and set up the containers acting as a nodes. For that you cna use following commands:
Docker network create -d bridge cassandraNet
docker run --name library_db_1 --network cassandraNet -p  127.0.1.1:9042:9042 cassandra
docker run --name library_db_2 --network cassandraNet -e CASSANDRA_SEEDS=library_db_1 -p  127.0.1.2:9042:9042 cassandra
docker run --name library_db_3 --network cassandraNet -e CASSANDRA_SEEDS=library_db_1 -p  127.0.1.3:9042:9042 cassandra
docker exec -it library_db_1 cqlsh
CREATE KEYSPACE library_project WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2};
