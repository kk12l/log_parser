CREATE TABLE message (
	created TIMESTAMP(0) NOT NULL,
	id VARCHAR(1000) NOT NULL,
	int_id CHAR(16) NOT NULL,
	str VARCHAR(10000) NOT NULL,
	status BOOLEAN,
	CONSTRAINT message_id_pk PRIMARY KEY(id)
);
CREATE INDEX message_created_idx ON message (created);
CREATE INDEX message_int_id_idx ON message (int_id);
CREATE TABLE log (
	created TIMESTAMP(0) NOT NULL,
	int_id CHAR(16) NOT NULL,
	str VARCHAR(10000),
	address VARCHAR(1000)
);
CREATE INDEX log_address_idx ON log(address) USING hash;
