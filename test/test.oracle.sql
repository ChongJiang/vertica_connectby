
SET SERVEROUTPUT ON SIZE 1000000;
SET LINESIZE 328;
set pagesize 50000;

COLUMN id FORMAT 99
COLUMN parent_id FORMAT 99
COLUMN name FORMAT A10
COLUMN path_level FORMAT 99
COLUMN id_root FORMAT 99
COLUMN name_root FORMAT A10
COLUMN id_path FORMAT A15
COLUMN name_path FORMAT A30


create table company (
  id NUMBER 
  , parent_id NUMBER 
  , name varchar(20)
);

insert into company values (1, 0, 'Patrick1');
insert into company values (12, 1, 'Jim1');
insert into company values (13, 1, 'Sandy1');
insert into company values (14, 13, 'Brian1');
insert into company values (15, 13, 'Otto1');
insert into company values (2, 0, 'Patrick2');
insert into company values (22, 2, 'Jim2');
insert into company values (23, 2, 'Sandy2');
insert into company values (24, 23, 'Brian2');
insert into company values (25, 23, 'Otto2');

commit;

select id, parent_id, name
  , LEVEL as path_level
  , CONNECT_BY_ROOT id AS id_root
  , SYS_CONNECT_BY_PATH(id, '|') AS id_path
  , CONNECT_BY_ROOT name AS name_root
  , SYS_CONNECT_BY_PATH(name, '|') AS name_path
from company c
  CONNECT BY PRIOR c.id = c.parent_id 
  START WITH c.parent_id = 0
order by id
;

drop table company;
