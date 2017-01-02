/*****************************
 * Vertica Analytic Database
 *
 * connectby User Defined Functions
 *
 * Copyright Vertica, 2013
 */



\o /dev/null

create table item (
  id int 
  , parent_id int 
  , name varchar(20)
);

insert into item values (1, 0, 'Patrick1');
insert into item values (12, 1, 'Jim1');
insert into item values (13, 1, 'Sandy1');
insert into item values (131, 13, 'Brian1');
insert into item values (132, 13, 'Otto1');
insert into item values (2, 0, 'Patrick2');
insert into item values (22, 2, 'Jim2');
insert into item values (23, 2, 'Sandy2');
insert into item values (231, 23, 'Brian2');
insert into item values (232, 23, 'Otto2');

commit;

\o

select connectby(parent_id, id, name) over () from item;

select *
  from (select connectby(parent_id, id, name) over () as (level, parent_id, id, name, name_root, name_path)
          from (select (case when id=13 then NULL else parent_id end) as parent_id, id, name 
  	              from item) t0
    ) t1
  	where t1.name_root ='';
\q

select connectby(parent_id, id, name using parameters showlevel=false, showparentid=false, showid=false, showname=false, shownameroot=false) over () from item;


select connectby(parent_id, id, name using parameters startid=131, separator='|') over () as (level, parent_id, id, name, name_root, name_path) from item;


select connectby(parent_id, id, name using parameters startid=131, endid=13, separator='|') over () as (level, parent_id, id, name, name_root, name_path) from item;

---- connect by for oracle 
--select id, parent_id, name
--  , LEVEL as path_level
--  , CONNECT_BY_ROOT id AS id_root
--  , SYS_CONNECT_BY_PATH(id, '|') AS id_path
--  , CONNECT_BY_ROOT name AS name_root
--  , SYS_CONNECT_BY_PATH(name, '|') AS name_path
--from item c
--  CONNECT BY PRIOR c.id = c.parent_id 
--  START WITH c.parent_id = 0
--;

-- connect by for vertica 
select connectby(parent_id, id, id::varchar, name using parameters separator='|') over () as (level, parent_id, id, id2, id_root, id_path, name, name_root, name_path) from item;

drop table if exists item cascade;
