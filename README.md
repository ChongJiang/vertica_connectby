<html lang="zn_CN"> <head> <meta charset='utf-8'> <title>Hierarch query function for Vertica</title> </head> <body>

Hierarch query function for Vertica
==========
This is a Vertica User Defined Functions (UDF) for hierarch query function, similar with ConnectBy statement of Oracle.

Syntax:
----------

CONNECTBY ( parentid, id, column1 [, columnN] [using parameters ...] ) over(...)

Columns:

 * parentid: column for parent id, must be int type. Mandatory. parentid of top level can be any not existing value, such as 0 or NULL
  - TODO: parent_id 不存在，但 label[0].count(parent_id)后，在 label[0] 就存在了？
 * id: column for id, must be int type. Mandatory.
 * column1: column name1 for building path, must be varchar/char type. Mandatory.
 * columnN: column nameN for building path, must be varchar/char type. Optional.

Parameters:

 * startid: id of the most bottom item. Optional paramter, default value is vint_null(0x8000000000000000LL), meams including all items.
 * endid: id of the most top item. Optional paramter, default value is vint_null(0x8000000000000000LL), meams stopping when parent_id equals 0 or null.
 * maxsize: maximum output size. Optional paramter, default value is 64000. 
 * separator: separator string for concatenating. Optional paramter, default value is '/'.
 * showlevel: switch for display level colum. Optional paramter, default true. 
 * showparentid: switch for display showparentid colum. Optional paramter, default true. 
 * showid: switch for display id colum. Optional paramter, default true. 
 * showname: switch for display name colum. Optional paramter, default true. 
 * shownameroot: switch for display root of name colum. Optional paramter, default true. 


Return:
 * hierarch query result. 

Examples:
----------

<code><pre>
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
</code></pre>
<code><pre>
	select connectby(parent_id, id, name) over () from item;
	 level | parent_id | id  |   name   | name_root |       name_path        
	-------+-----------+-----+----------+-----------+------------------------
	     1 |         0 |   1 | Patrick1 | Patrick1  | Patrick1
	     1 |         0 |   2 | Patrick2 | Patrick2  | Patrick2
	     2 |         1 |  12 | Jim1     | Patrick1  | Patrick1/Jim1
	     2 |         1 |  13 | Sandy1   | Patrick1  | Patrick1/Sandy1
	     2 |         2 |  22 | Jim2     | Patrick2  | Patrick2/Jim2
	     2 |         2 |  23 | Sandy2   | Patrick2  | Patrick2/Sandy2
	     3 |        13 | 131 | Brian1   | Patrick1  | Patrick1/Sandy1/Brian1
	     3 |        13 | 132 | Otto1    | Patrick1  | Patrick1/Sandy1/Otto1
	     3 |        23 | 231 | Brian2   | Patrick2  | Patrick2/Sandy2/Brian2
	     3 |        23 | 232 | Otto2    | Patrick2  | Patrick2/Sandy2/Otto2
	(10 rows)
</code></pre>
<code><pre>
	select connectby(parent_id, id, name using parameters showlevel=false, showparentid=false, showid=false, showname=false, shownameroot=false) over () from item;
	       name_path        
	------------------------
	 Patrick1
	 Patrick2
	 Patrick1/Jim1
	 Patrick1/Sandy1
	 Patrick2/Jim2
	 Patrick2/Sandy2
	 Patrick1/Sandy1/Brian1
	 Patrick1/Sandy1/Otto1
	 Patrick2/Sandy2/Brian2
	 Patrick2/Sandy2/Otto2
	(10 rows)
</code></pre>
	select connectby(parent_id, id, name using parameters startid=131, separator='|') over () as (level, parent_id, id, name, name_root, name_path) from item;
	 level | parent_id | id  |   name   | name_root |       name_path        
	-------+-----------+-----+----------+-----------+------------------------
	     1 |         0 |   1 | Patrick1 | Patrick1  | Patrick1
	     2 |         1 |  13 | Sandy1   | Patrick1  | Patrick1|Sandy1
	     3 |        13 | 131 | Brian1   | Patrick1  | Patrick1|Sandy1|Brian1
	(3 rows)
<code><pre>
	select connectby(parent_id, id, name using parameters startid=131, endid=13, separator='|') over () as (level, parent_id, id, name, name_root, name_path) from item;
	 level | parent_id | id  |  name  | name_root |   name_path   
	-------+-----------+-----+--------+-----------+---------------
	     1 |         1 |  13 | Sandy1 |           | Sandy1
	     2 |        13 | 131 | Brian1 | Brian1    | Sandy1|Brian1
</code></pre>
<code><pre>
	select connectby(parent_id, id, id::varchar, name using parameters separator='|') over () as (level, parent_id, id, id2, id_root, id_path, name, name_root, name_path) from item;
	 level | parent_id | id  | id2 | id_root | id_path  |   name   | name_root | name_path 
	-------+-----------+-----+-----+---------+----------+----------+-----------+-----------
	     1 |         0 |   1 | 1   | 1       | Patrick1 | Patrick1 | 1         | 
	     1 |         0 |   2 | 2   | 2       | Patrick2 | Patrick2 | 2         | 
	     2 |         1 |  12 | 12  | 1       | Jim1     | Patrick1 | 1|12      | 
	     2 |         1 |  13 | 13  | 1       | Sandy1   | Patrick1 | 1|13      | 
	     2 |         2 |  22 | 22  | 2       | Jim2     | Patrick2 | 2|22      | 
	     2 |         2 |  23 | 23  | 2       | Sandy2   | Patrick2 | 2|23      | 
	     3 |        13 | 131 | 131 | 1       | Brian1   | Patrick1 | 1|13|131  | 
	     3 |        13 | 132 | 132 | 1       | Otto1    | Patrick1 | 1|13|132  | 
	     3 |        23 | 231 | 231 | 2       | Brian2   | Patrick2 | 2|23|231  | 
	     3 |        23 | 232 | 232 | 2       | Otto2    | Patrick2 | 2|23|232  | 
	(10 rows)
</code></pre>

Install, test and uninstall:
----------
Befoe build and install, g++ should be available(yum -y groupinstall "Development tools" && yum -y groupinstall "Additional Development" can help on this).

 * Build: make
 * Install: make install
 * Test: make run
 * Uninstall make uninstall

</body> </html>



