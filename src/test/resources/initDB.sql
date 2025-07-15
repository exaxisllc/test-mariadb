CREATE TABLE IF NOT EXISTS test_objects (
id    int(11)       primary key,
name  varchar(255)  NULL
)
ENGINE = 'InnoDB';

INSERT INTO test_objects (id, name) values (1, "record 1");
INSERT INTO test_objects (id, name) values (2, "record 2");

COMMIT;
