prompt ORACLE_DB/rdk_rel_blocks_session.sql
set line 180 head off feedback off pagesize 0 verify off
col param1 noprint new_value threshold
col param2 noprint new_value dryr

set verify off
select to_number('&1') param1 from dual;
select lower('&2') param2 from dual;

prompt BLOCKS_TIME => &&threshold.(s)


set serveroutput on
DECLARE
dryrun varchar2(10);
CURSOR SESSION_LIST IS
select ws.sid waiting_sess,ws.serial# waiting_seri,ws.blocking_session blocking_sess,bs.serial# blocking_seri,
bs.username blocking_user,bs.status,ws.username,
ws.sql_id,to_char(wqs.sql_fulltext) waiting_query,round(ws.seconds_in_wait/60,1),bs.wait_class,ws.event
 from v$session bs 
 join v$session ws on ws.blocking_session = bs.sid
 join v$sql wqs on wqs.sql_id = ws.sql_id
where ws.seconds_in_wait >= &&threshold.;
ROW_CURS SESSION_LIST%ROWTYPE;
KILL_SES VARCHAR2(2000);
QUERY_SES VARCHAR2(4000);
BEGIN
    dryrun := '&&dryr.';
    OPEN SESSION_LIST;
    LOOP
    FETCH SESSION_LIST INTO ROW_CURS;
    EXIT WHEN SESSION_LIST%NOTFOUND;
        IF (ROW_CURS.STATUS = 'INACTIVE' AND ROW_CURS.WAIT_CLASS = 'Idle') THEN
            KILL_SES := 'ALTER SYSTEM KILL SESSION '''||ROW_CURS.BLOCKING_SESS||','||ROW_CURS.BLOCKING_SERI||'''';
            QUERY_SES := 'BLOCKING_SESSION: '||ROW_CURS.BLOCKING_SESS||', WITH USER '||ROW_CURS.BLOCKING_USER||';'||chr(10)||'BLOCKED_SESSION: '||ROW_CURS.WAITING_SESS||', WITH BLOCKED USER '||ROW_CURS.USERNAME||';'||chr(10)||'BLOCKED QUERY: '||ROW_CURS.WAITING_QUERY||';';     
            IF (dryrun = 'false') THEN
                dbms_output.put_line('--not executed ');
                dbms_output.put_line( KILL_SES ||chr(10)|| QUERY_SES);
            ELSE
                BEGIN
                    execute immediate(KILL_SES);
                END;
                dbms_output.put_line('--executed ');
                dbms_output.put_line( KILL_SES || ';');
            END IF;
        END IF;
    END LOOP;
    CLOSE SESSION_LIST;
END;
/
