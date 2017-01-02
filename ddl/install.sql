/*****************************
 * Vertica Analytic Database
 *
 * connectby User Defined Functions
 *
 * Copyright Vertica, 2013
 */

-- Step 1: Create LIBRARY 
\set libfile '\''`pwd`'/lib/connectby.so\'';
CREATE LIBRARY connectby AS :libfile;

-- Step 2: Create cube/rollup Factory
\set tmpfile '/tmp/connectbyinstall.sql'
\! cat /dev/null > :tmpfile

\t
\o :tmpfile
select 'CREATE TRANSFORM FUNCTION connectby AS LANGUAGE ''C++'' NAME '''||obj_name||''' LIBRARY connectby;' from user_library_manifest where lib_name='connectby' and obj_name like 'ConnectByFactory%';

\o
\t

\i :tmpfile
\! rm -f :tmpfile
