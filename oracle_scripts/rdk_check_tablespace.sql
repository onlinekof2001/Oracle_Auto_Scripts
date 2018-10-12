set feedback off
col param1 noprint new_value tbsname

set verify off
set term off
select upper('&1') param1 from dual;
col param2 noprint new_value current_date
select to_char(sysdate,'YYYYMMDDHH24') param2 from dual;
set term on

prompt TBSNAME       => &&tbsname.
set trim off
set head off
set wrap off
set line 160
set long 6999
set pages 999
COL TABLESPACE_NAME FOR A25
COL FILE_ID for 9999
COL free_rate for 999999
COL FILE_NAME FOR A80
spool /tmp/EXTTBS/&&tbsname._&&current_date..list
SELECT tablespace_name,file_id,
       round(100 * (sum_max - sum_alloc + nvl(sum_free, 0)) / sum_max,2) AS free_rate,
       round((sum_max - sum_alloc + nvl(sum_free, 0)) / 1024 / 1024,2) AS free_size,
       round((sum_alloc - nvl(sum_free, 0)) / 1024 / 1024,0) as actual_used,
       round(sum_max / 1024 / 1024,0) as max_size,
       file_name
  FROM (SELECT tablespace_name,
               file_id,
               sum(bytes) AS sum_alloc,
               sum(decode(maxbytes, 0, bytes, maxbytes)) AS sum_max,
               file_name
          FROM dba_data_files WHERE tablespace_name = '&&tbsname.'
         GROUP BY tablespace_name, file_name, file_id),
       (SELECT tablespace_name AS fs_ts_name,
               file_id as file_ts_id,
               sum(bytes) AS sum_free
          FROM dba_free_space
         GROUP BY tablespace_name, file_id)
WHERE file_id = file_ts_id(+)
order by 2, 3;
spool off
quit;
