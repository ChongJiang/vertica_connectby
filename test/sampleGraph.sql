

\o /dev/null

drop table if exists item cascade;

create table item (
  id int 
  , parent_id int 
  , name varchar(20)
);

--insert into item values (1, 0, 'Patrick1');
--insert into item values (12, 1, 'Jim1');
--insert into item values (13, 1, 'Sandy1');
--insert into item values (131, 13, 'Brian1');
--insert into item values (132, 13, 'Otto1');
--
--insert into item values (2, 0, 'Patrick2');
--insert into item values (22, 2, 'Jim2');
--insert into item values (23, 2, 'Sandy2');
--insert into item values (231, 23, 'Brian2');
--insert into item values (232, 23, 'Otto2');

-- insert into item values (3, 0, 'Patrick3');
--insert into item values (32, 3, 'Jim3');
--insert into item values (33, 3, 'Sandy3');
--insert into item values (331, 33, 'Brian3');
--insert into item values (332, 33, 'Otto3');

insert into item values (31, 3, 'Jim3');
insert into item values (42, 4, 'Sandy4');
insert into item values (53, 5, 'Brian5');

commit;

\o


select connectby(parent_id, id, name) over () from item;


drop table if exists item cascade;
