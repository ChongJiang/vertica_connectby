drop TABLE if exists dim_base_kind cascade;

CREATE TABLE dim_base_kind (
    base_type character varying(20),
    base_kind character varying(20),
    kind_code int
);

insert into dim_base_kind values('2', '1', 1);
insert into dim_base_kind values('2', '2', 2);
insert into dim_base_kind values('2', '3', 3);
insert into dim_base_kind values('2', '4', 4);

commit;

/*
----------------------
  1 | 1
  2 | 1,2
  3 | 1,2,3
  4 | 1,2,3,4
  5 | 2
  6 | 2,3
  7 | 2,3,4
  8 | 3
  9 | 3,4
 10 | 4
------------
*/

select connectby(parent_id, id, name using parameters separator=',',showlevel=true, showparentid=false, showid=true, showname=false, shownameroot=true) over () as (level, id, name_root, name_path)
from (
  select t1.id, t1.parent_id, t1.name
  from (SELECT base_kind as name, kind_code as id, lag(kind_code) over(order by kind_code) as parent_id
          FROM DIM_BASE_KIND
          WHERE BASE_TYPE = '2') t1
) t
;

/*
 level | id | name_root | name_path 
-------+----+-----------+-----------
     2 |  1 |           | ,,1
     3 |  2 |           | ,,1,2
     4 |  3 |           | ,,1,2,3
     5 |  4 |           | ,,1,2,3,4

----------------------------------
*/

  select t1.id as t1_id, t1.parent_id as t1_parent_id, t2.id as t2_id, t2.parent_id as t2_parent_id, 
    nvl(t1.parent_id,0)*10000+ t2.id as id, 
    case when t1.parent_id=t2.parent_id then 
      null
    else
      nvl(t1.parent_id,0)*10000+ t2.parent_id
    end as parent_id, 
    t2.name
  from (SELECT base_kind as name, kind_code as id, lag(kind_code) over(order by kind_code) as parent_id
          FROM DIM_BASE_KIND
          WHERE BASE_TYPE = '2') t1,
        (SELECT base_kind as name, kind_code as id, lag(kind_code) over(order by kind_code) as parent_id
          FROM DIM_BASE_KIND
          WHERE BASE_TYPE = '2') t2
         where t1.id<=t2.id
  order by 1,3
;

/*
 t1_id | t1_parent_id | t2_id | t2_parent_id |  id   | parent_id | name 
-------+--------------+-------+--------------+-------+-----------+------
     1 |              |     1 |              |     1 |           | 1
     1 |              |     2 |            1 |     2 |         1 | 2
     1 |              |     3 |            2 |     3 |         2 | 3
     1 |              |     4 |            3 |     4 |         3 | 4
     2 |            1 |     2 |            1 | 10002 |           | 2
     2 |            1 |     3 |            2 | 10003 |     10002 | 3
     2 |            1 |     4 |            3 | 10004 |     10003 | 4
     3 |            2 |     3 |            2 | 20003 |           | 3
     3 |            2 |     4 |            3 | 20004 |     20003 | 4
     4 |            3 |     4 |            3 | 30004 |           | 4
(10 rows)
*/

select row_number() over (order by id) as id, substr(name, 3, length(name)) as name
from (select connectby(parent_id, id, name using parameters separator=',', showlevel=true, showparentid=false, showid=true, showname=false, shownameroot=true) over () as (level, id, name_root, name)
      from (
        select /* t1.id as t1_id, t1.parent_id as t1_parent_id, t2.id as t2_id, t2.parent_id as t2_parent_id, */
          nvl(t1.parent_id,0)*10000+ t2.id as id, 
          case when t1.parent_id=t2.parent_id then 
            null
          else
            nvl(t1.parent_id,0)*10000+ t2.parent_id
          end as parent_id, 
          t2.name
        from (SELECT base_kind as name, kind_code as id, lag(kind_code) over(order by kind_code) as parent_id
                FROM DIM_BASE_KIND
                WHERE BASE_TYPE = '2') t1,
              (SELECT base_kind as name, kind_code as id, lag(kind_code) over(order by kind_code) as parent_id
                FROM DIM_BASE_KIND
                WHERE BASE_TYPE = '2') t2
               where t1.id<=t2.id
      ) t3
) t
;

/*
 id |  name   
----+---------
  1 | 1
  2 | 1,2
  3 | 1,2,3
  4 | 1,2,3,4
  5 | 2
  6 | 2,3
  7 | 2,3,4
  8 | 3
  9 | 3,4
 10 | 4

-------------------------------
*/

drop TABLE if exists dim_base_kind cascade;
