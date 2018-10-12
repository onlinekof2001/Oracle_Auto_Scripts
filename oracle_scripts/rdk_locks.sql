rem ********************************************************************************
rem ********************************************************************************
rem
rem NAME
rem   LOCKS.SQL
rem
rem FUNCTION
rem   description FR
rem   Shows session waiting on locks... 
rem
rem PARAMETERS
rem   no
rem
rem MODIFIED
rem		Bastien CATTEAU - ALL4IT - 03/2006 - v1.0
rem 
rem ********************************************************************************
rem ********************************************************************************

set lines 400

set wrap on
set trim on

col usr heading 'user' for A15
col b heading 'type' for A8
col sid heading 'sid' for A3
col ltype for A2
col machine for A17 trunc
col program for A12 trunc
col lmode for A15
col text for A50 trunc
col kill for a60
col obj heading 'obj' for A120
set head on
set echo off
set pagesize 500


select   s.inst_id,(lpad(' ',DECODE(request,0,0,5))||S.username) "USR",
          decode(request,0,'BLOQUEUR','WAITEUR') "B",
          to_char(L.SID) "SID",
          id1,
          id2,
	  S.machine ,
	  S.program ,
	  L.TYPE "LTYPE",
          substr(decode(LMODE,0,'0 : NONE_____',2,'2 : Row Share',4,'4 : Share__',6,'6 : Exclusive',to_char(lmode)),1,15) "LMODE",
          --REQUEST ,
         -- txt.SQL_TEXT "text",
          l.ctime/60,
		  'alter system kill session '''||s.sid||','||serial#||''';' kill,
		  'SELECT * FROM ' ||o.owner ||'.'||o.object_name ||' WHERE rowid =  DBMS_ROWID.ROWID_CREATE(1, '||s.ROW_WAIT_OBJ#||', '||s.ROW_WAIT_FILE# ||', '||s.ROW_WAIT_BLOCK#||', '||s.ROW_WAIT_ROW#||');' obj
		  from gv$lock L,gv$session S
		  left outer join dba_objects o on object_id=ROW_WAIT_OBJ#
		  --, V$SQLAREA TXT
          WHERE L.sid = s.sid and L.inst_id=s.inst_id
         -- and S.SQL_HASH_VALUE = TXT.HASH_VALUE(+)
         -- and S.SQL_ADDRESS = txt.ADDRESS(+)
          AND id1 IN (SELECT id1 FROM gV$LOCK WHERE lmode = 0)
order by l.ctime/60 desc
/

clear columns


exit
