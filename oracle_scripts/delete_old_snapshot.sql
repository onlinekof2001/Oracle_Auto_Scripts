set head off
set line 260
set long 9999
set pages 999
spool /tmp/delete_old_snapshot_tmp.sql
with t1 as (select 1 as id, min(snap_id) min_snap_repository from dba_hist_snapshot)
,t2 as (select 1 as id, min(snap_id) min_snap_id_ash from sys.WRH$_ACTIVE_SESSION_HISTORY) select 'exec DBMS_WORKLOAD_REPOSITORY.DROP_SNAPSHOT_RANGE(''' ||min_snap_id_ash||''','''||min_snap_repository||''');' from t1,t2 where t1.id=t2.id;
spool off
