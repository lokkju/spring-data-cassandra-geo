create table GeoCassandraTableInstance (s2_hash_key bigint, s2_cell_id bigint, data text, PRIMARY KEY(s2_hash_key,s2_cell_id));
insert into GeoCassandraTableInstance (s2_hash_key, s2_cell_id, data) values(-852015, -8520151733974859771, 'test data 1'  );
insert into GeoCassandraTableInstance (s2_hash_key, s2_cell_id, data) values(-852015, -8520151733974859775, 'test data 2'  );
insert into GeoCassandraTableInstance (s2_hash_key, s2_cell_id, data) values(-852015, -8520151733974859779, 'test data 3'  );