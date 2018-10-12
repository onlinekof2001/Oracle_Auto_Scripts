set feedback off
col param1 noprint new_value username

set verify off
set term off
select upper('&1') param1 from dual;

col param2 noprint new_value upper_username
select upper('&&username.') param2 from dual;

set term on
set head off
set line 160
set long 999
set pages 99
select username from dba_users where username='&&upper_username.';
