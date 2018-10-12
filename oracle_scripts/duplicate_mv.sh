#!/bin/bash
################################################################################
# SCRIPT:     duplicate_mv.sh
# SYNOPSIS:   create a new materialized view to replace a big one which takes forever to be refreshed
# USAGE:      duplicate_mv.sh -m SCHEMA -v MVIEW [-a] [-j] [-p SYS_PASSWORD -d TNS_ENTRY]
# USE CASE:   this script can be use to recreate a mview on the the intermediate/slave nodes of
#             a master when a new column has been added to a mview (with -a), or to recreate a mview
#             on a slave node when the mview refresh is too long compared to its recreation
# PARAMETERS:
#  mandatory:
#   -m SCHEMA: the schema owner of the mview
#   -v MVIEW: the mview to replace
#  rundeck mandatory:
#   -p SYS_PASSWORD: sys password of the database
#   -d TNS_ENTRY: database to connect to
#  optional:
#   -a: use 'select *' as the mview query. usefull when you want to recreate the mview 
#       after the columns of the mviews have changed on the master
#   -j: don't reactivate the refresh job at the end (usefull if there's more than one mview to recreate)
# INFOS:
#  This script must be launched on the server where the database resides with the environment variables set.
#
#  This script create a new mview to replace a big one which takes forever to be refreshed.
#  It adds a number at the end of the new mview name.
#  It create a public synonym before droping the old mview, this way the mview content is always accessible.
#  Once the old mview is dropped, it's remplaced with a synonym and the public synonym is dropped.
#
#  Actions performed (with example values to recreate PRIX_UNIT_CESSION_MAG mview in MD0000):
#   1. Check enough free space in MD0000 snap_data tablespace.
#   2. Create new mview in MD0000 and perform a refresh.
#   3. Add indexes and count them.
#   4. Compute stats on PRIX_UNIT_CESSION_MAG2.
#   5. Add grants.
#   6. Add mview log.
#   7. Stop refresh job MD0000.J_MD0000.
#   8. Disable refresh job MD0000.J_MD0000.
#   9. Update refresh groups.
#   10. Drop mview and replace it by a synonym.
#   11. Enable refresh job.
#
#  Example with parameters to recreate PRIX_UNIT_CESSION_MAG mview in MD0000:
#   ./duplicate_mv.sh -m MD0000 -v PRIX_UNIT_CESSION_MAG
#
#  Example with parameters to recreate ZONE_DEVISE mview in MD0000 because a new column has been added:
#   ./duplicate_mv.sh -m MD0000 -v ZONE_DEVISE -a
#
#  If an error occures during a step the script quits and display the command to
#  execute to resume at that step once the error has been fixed.
################################################################################

function die {
    # display an error message, the command to restart at the error step then dies
    error "$1"
    echo "" >&2

    if [ "$CUR_STEP" != "0" ]; then
	TXT=""
        [ ${DEBUG} -eq 0 ] && TXT="${TXT} -z"
	[ ${PAUSE} -eq 0 ] && TXT="${TXT} -y"
	[ ${SKIP_ENABLE_JOB} -eq 0 ] && TXT="${TXT} -d"
	[ ${USE_SELECT_STAR} -eq 0 ] && TXT="${TXT} -a"
	[ ${BATCH_MODE} -eq 0 ] && TXT="${TXT} -b"
        echo "Fix the issue then to restart the script at the failed step use these parameters:" >&2
        echo "./duplicate_mv.sh -m ${SCHEMA} -v ${MVIEW} -s ${CUR_STEP} ${TXT}" >&2
    fi

    exit 1
}

function get_parameters {
    # parse command line arguments
    # -v MVIEW
    export MVIEW=""
    # -m SCHEMA
    export SCHEMA=""
    # -s STEP
    export RESUME_STEP=1
    # -y
    export PAUSE=1
    # -z
    export DEBUG=1
    # -j
    export SKIP_ENABLE_JOB=1
    # -a
    export USE_SELECT_STAR=1
    # -b
    export BATCH_MODE=1
    # -p SYS_PASSWORD
    export SYS_PASSWORD=''
    # -d TNS_ENTRY
    export TNS_ENTRY=''

    while getopts ":m:v:s:yzjabp:d:" opt "$@"; do
        case $opt in
	    v)
		export MVIEW=$(echo ${OPTARG} | tr '[:lower:]' '[:upper:]')
		info "Use mview ${MVIEW}"
		;;
            m)
                export SCHEMA=$(echo ${OPTARG} | tr '[:lower:]' '[:upper:]')
                info "Use schema ${SCHEMA}"
                ;;
            s)
                export RESUME_STEP="$OPTARG"
                info "Resuming at step ""$RESUME_STEP"
                ;;
	    y)
		export PAUSE=0
		info "Pausing on"
		;;
            z)
                export DEBUG=0
                info "Debug on"
                ;;
	    j)
		export SKIP_ENABLE_JOB=0
		info "Don't enable refresh job at the end"
		;;
	    a)
		export USE_SELECT_STAR=0
		info "Use 'select *' for the mview query"
		;;
	    b)
		export BATCH_MODE=0
		info "Launch in batch mode, don't pause"
		;;
            p)
                export SYS_PASSWORD="${OPTARG}"
                info "Use sys password xxxxxxxx"
                ;;
            d)
                export TNS_ENTRY="${OPTARG}"
                info "Use tns entry ${TNS_ENTRY}"
                ;;
            \?)
                warning "Invalid option: -""$OPTARG"
                ;;
            :)
                warning "Option -""$OPTARG"" requires an argument"
                ;;
        esac
    done
}

# Main
export CUR_STEP=0

# load lib
. $(pwd)/lib_dup_ora.sh || die "Can't load lib lib_dup_ora.sh"

get_parameters "$@"
shift $IGNORE_PARAMS

# check that the vars are set
if( ! check_env_vars "SCHEMA" "MVIEW"); then
    usage "$0"
    die "Missing mandatory params"
fi

if( ! check_env_vars "ORACLE_HOME" "ORACLE_SID"); then
    usage "$0"
    die "Oracle env not set (ORACLE_HOME and ORACLE_SID)"
fi

export NUMBER=0
if ( echo ${MVIEW} | grep -q -E '[1-9]$' ); then
    # the mview is already a copy with a number at the end of its name,
    # create the new mview with the number incremented
    NUMBER=$(echo ${MVIEW} | sed -e 's+.*\([1-9]\)$+\1+')
    if [ ${NUMBER} -eq 9 ]; then
	NUMBER=2
    else
	let NUMBER=${NUMBER}+1
    fi
    NEW_MVIEW=$(echo "${MVIEW}" | sed -e 's+[1-9]$++')${NUMBER}
else
    # add a 2 at the end of the mview name
    NUMBER=2
    NEW_MVIEW=${MVIEW}${NUMBER}
fi

# display what we're going to do
info "Actions to perform:"
export CUR_STEP=1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Check enough free space in ${SCHEMA} snap_data tablespace"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Create new mview in ${SCHEMA} and perform a refresh"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add indexes and count them"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Compute stats on ${NEW_MVIEW}"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add grants"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add mview log"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Stop refresh job ${SCHEMA}.J_${SCHEMA}"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Disable refresh job ${SCHEMA}.J_${SCHEMA}"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Update refresh groups"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Drop mview and replace it by a synonym"
fi
if [ ${SKIP_ENABLE_JOB} -eq 1 ]; then
    let CUR_STEP=${CUR_STEP}+1
    if(check_step ${CUR_STEP} ${RESUME_STEP}); then
	info "${CUR_STEP}. Enable refresh job"
    fi
else
    info "Don't enable refresh job at the end"
fi
echo ""

# wait for user confirmation if launched from a TTY
if [ ${BATCH_MODE} -eq 1 ]; then
    if(tty -s); then
	press_any_key
    fi
fi

mview_exists "${SCHEMA}" "${MVIEW}" || die "The mview ${SCHEMA}.${MVIEW} doesn't exist"

export CUR_STEP=1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Check enough free space in ${SCHEMA} snap_data tablespace"

    DATA_TBS=$(get_tbs_name "${SCHEMA}" "SNAP_DATA") || die "Can't get tbs DATA name"
    DATA_TBS_FREE=$(get_tbs_free_space ${DATA_TBS}) || die "Can't get tbs ${DATA_TBS} free space"
    MVIEW_SIZE=$(get_mview_size ${SCHEMA} ${MVIEW}) || die "Can't get mview ${MVIEW} size"

    info "free space: ${DATA_TBS_FREE}"
    info "mview size: ${MVIEW_SIZE}"

    if [ ${MVIEW_SIZE} -gt ${DATA_TBS_FREE} ]; then
	die "Not enough free space in tbs ${DATA_TBS}"
    fi

    info "OK::enough free space in ${DATA_TBS}"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Create new mview in ${SCHEMA} and perform a refresh"

    if [ ${USE_SELECT_STAR} -eq 0 ]; then
	typeset SQL=$(add_sql_header "select 'select * from '||rtrim('${MVIEW}', '123456789')||master_link from dba_mviews where mview_name = '${MVIEW}' and owner = '${SCHEMA}';")
	typeset QUERY=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't get mview ${MVIEW} query: ${QUERY}"
    else
	# get the query of the mview in SCHEMA
	typeset SQL=$(add_pretty_sql_header "select query from dba_mviews where mview_name = '${MVIEW}' and owner = '${SCHEMA}';")
	typeset QUERY=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't get mview ${MVIEW} query: ${QUERY}"
	# some query have from in lowercase
	QUERY=$(echo ${QUERY} | sed -e 's+from+FROM+')
	# remove owner of the remote mview in the query (check if the from is on the same
	# line as the @ or if the query has a line return just after the from)
	if ( echo "${QUERY}" | grep -E -q 'FROM.*@' ); then
	    QUERY=$(echo "${QUERY}" | sed -e 's+FROM [^\.@]*\.+FROM +')
	else
	    QUERY=$(echo "${QUERY}" | sed -e '/@/s+^\([^\.@]*\)\.\([^@]*\)+\2+')
	fi
	# if query > 2300 char, put it in two lines because sqlplus line limit is 2499
	QUERY_LENGTH=$(echo "${QUERY}" | wc -c | sed -e 's+ ++g')
	if [ ${QUERY_LENGTH} -gt 2300 ]; then
	    info "query length > 2300"
	    let COUNT=$QUERY_LENGTH/2
	    while [ "${QUERY:$COUNT:1}" != ' ' ]; do
		let COUNT=$COUNT+1
	    done
	    let COUNT_1=$COUNT+1
	    let REMAIN=$QUERY_LENGTH-$COUNT
	    QUERY="${QUERY:0:${COUNT}}
${QUERY:${COUNT_1}:${REMAIN}}"
	fi
    fi

    DATA_TBS=$(get_tbs_name "${SCHEMA}" "SNAP_DATA") || die "Can't get tbs DATA name"
    INDEX_TBS=$(get_tbs_name "${SCHEMA}" "SNAP_INDEX") || die "Can't get tbs INDEX name"

    typeset SQL=$(add_sql_header "alter session set REMOTE_DEPENDENCIES_MODE=SIGNATURE;
grant create table to ${SCHEMA};
CREATE MATERIALIZED VIEW ${SCHEMA}.${NEW_MVIEW}
TABLESPACE ${DATA_TBS} BUILD IMMEDIATE
USING INDEX TABLESPACE ${INDEX_TBS} refresh force on demand as
${QUERY};")

    info "Mview ddl:
${SQL}"
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't create mview ${NEW_MVIEW}"
    info "mview created"

    rename_pk_index "${SCHEMA}" "${NEW_MVIEW}"

    typeset SQL=$(add_sql_header "exec dbms_mview.refresh('${SCHEMA}.${NEW_MVIEW}', '?');")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't refresh mview ${NEW_MVIEW}"
    info "mview refreshed"

    info "OK::mview created and refreshed"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add indexes and count them"

    INDEX_TBS=$(get_tbs_name "${SCHEMA}" "SNAP_INDEX") || die "Can't get tbs INDEX name"

    # get indexes ddl
    typeset SQL=$(add_sql_header "SELECT SQL FROM 
(SELECT  table_name z, index_name y, -1 x,
        'create '||RTRIM(DECODE(uniqueness,'UNIQUE','UNIQUE',NULL))||' index ${SCHEMA}.'
        || rtrim(index_name, '123456789') || '${NUMBER}' SQL
FROM    dba_indexes
WHERE   table_owner = '${SCHEMA}' AND table_name = '${MVIEW}'
and index_name not like 'PK%' and index_name not like 'SYS%' and index_name not like '%PK' and index_name not like '%PKK' and index_name not like '%\_PK\_%' escape '\' and index_name not in (select index_name from dba_indexes where table_owner = '${SCHEMA}' and table_name = '${NEW_MVIEW}')
UNION
SELECT  table_name z, index_name y, 0 x,
        'on ${SCHEMA}.'|| rtrim(table_name, '123456789') || '${NUMBER} (' SQL
FROM    dba_indexes
WHERE   table_owner = '${SCHEMA}' AND table_name = '${MVIEW}'
and index_name not like 'PK%' and index_name not like 'SYS%' and index_name not like '%PK' and index_name not like '%PKK' and index_name not like '%\_PK\_%' escape '\' and index_name not in (select index_name from dba_indexes where table_owner = '${SCHEMA}' and table_name = '${NEW_MVIEW}')
UNION
SELECT  table_name z, index_name y, column_position x,
        RTRIM(DECODE(column_position,1,NULL,','))|| RTRIM(column_name) SQL
FROM    dba_ind_columns
WHERE   table_owner = '${SCHEMA}' AND table_name = '${MVIEW}'
and index_name not like 'PK%' and index_name not like 'SYS%' and index_name not like '%PK' and index_name not like '%PKK' and index_name not like '%\_PK\_%' escape '\' and index_name not in (select index_name from dba_indexes where table_owner = '${SCHEMA}' and table_name = '${NEW_MVIEW}')
UNION
SELECT  table_name z, index_name y, 999999 x,
        ') nologging TABLESPACE ${INDEX_TBS} PARALLEL '||DEGREE||';' SQL
FROM    dba_indexes
WHERE   table_owner = '${SCHEMA}' AND table_name = '${MVIEW}'
and index_name not like 'PK%' and index_name not like 'SYS%' and index_name not like '%PK' and index_name not like '%PKK' and index_name not like '%\_PK\_%' escape '\' and index_name not in (select index_name from dba_indexes where table_owner = '${SCHEMA}' and table_name = '${NEW_MVIEW}')
ORDER BY 1,2,3);")
    typeset INDEXES_DDL=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't get indexes ddl for ${MVIEW}: ${INDEXES_DDL}"
    if [ -n "${INDEXES_DDL}" ]; then
	INDEXES_DDL=$(add_sql_header "${INDEXES_DDL}")
	# create indexes
	info "indexes ddl:
${INDEXES_DDL}"
	sqlplus_sysdba_check_errors "${INDEXES_DDL}" ${DEBUG} || die "Can't create indexes for ${MVIEW}"
	info "indexes created for ${NEW_MVIEW}"
    else
	info "no indexes to create for ${NEW_MVIEW}"
    fi

    # count number of indexes for old and new mview to check that they are the same
    INDEX_COUNT_MVIEW=$(get_mview_indexes_count "${SCHEMA}" "${MVIEW}") || die "Can't get mview ${MVIEW} index count"
    INDEX_COUNT_NEW_MVIEW=$(get_mview_indexes_count "${SCHEMA}" "${NEW_MVIEW}") || die "Can't get mview ${NEW_MVIEW} index count"
    if [ ${INDEX_COUNT_MVIEW} -ne ${INDEX_COUNT_NEW_MVIEW} ]; then
	die "The old and new mviews don't have the same number of indexes"
    else
	info "mview ${NEW_MVIEW} has the same number of indexes as before: ${INDEX_COUNT_NEW_MVIEW}"
    fi

    info "OK::indexes added to mview"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. compute stats on ${NEW_MVIEW}"

    typeset SQL=$(add_sql_header "exec dbms_stats.gather_table_stats(ownname => '${SCHEMA}', tabname => '${NEW_MVIEW}');")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't compute stats on new mview"

    info "OK::stats computed"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add grants"

    typeset SQL=$(add_sql_header "select 'grant select on ${SCHEMA}.${NEW_MVIEW} to '||grantee||';' from dba_tab_privs where owner = '${SCHEMA}' and table_name = '${MVIEW}';")
    typeset GRANTS=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't generate grants: ${GRANTS}"
    info "Grants: 
${GRANTS}"
    GRANTS=$(add_sql_header "${GRANTS}")
    sqlplus_sysdba_check_errors "${GRANTS}" ${DEBUG} || die "Can't grant select on new mview"

    info "OK::Grant select granted"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add mview log"

    typeset SQL=$(add_sql_header "select 'OK' from dba_mview_logs where log_owner = '${SCHEMA}' and master = '${MVIEW}';")
    RESULT=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't check if mview log is present"
    if [ "${RESULT}" = 'OK' ]; then
	typeset DATA_TBS=$(get_tbs_name "${SCHEMA}" "SLOG_DATA") || die "Can't get tbs SLOG_DATA name"
	typeset SQL=$(add_sql_header "create materialized view log on ${SCHEMA}.${NEW_MVIEW} tablespace ${DATA_TBS};")
	sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't add mview log"
	info "OK::mview log created"

	# add select grant to mlog
	typeset SQL=$(add_sql_header "select 'grant select on '||l.log_owner||'.'||l.log_table||' to '||p.grantee||';' from dba_tab_privs p, dba_mview_logs l where p.owner = '${SCHEMA}' and p.table_name = '${NEW_MVIEW}' and p.owner = l.log_owner and p.table_name = l.master;")
	typeset GRANTS=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't generate grants: ${GRANTS}"
	info "Grants: 
${GRANTS}"
	GRANTS=$(add_sql_header "${GRANTS}")
	sqlplus_sysdba_check_errors "${GRANTS}" ${DEBUG} || die "Can't grant select on mview log"
    else
	info "OK::no mview log on ${MVIEW}"
    fi

    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Stop refresh job ${SCHEMA}.J_${SCHEMA}"

    STATE='RUNNING'
    while [ ${STATE} = 'RUNNING' ]; do
	typeset SQL=$(add_sql_header "select trim(state) from dba_scheduler_jobs where owner = '${SCHEMA}' and job_name = 'J_${SCHEMA}';")
	STATE=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't get refresh job state: ${STATE}"

	if [ "${STATE}" = 'RUNNING' ]; then
	    info "job running, let stop it"

	    SQL=$(add_sql_header "exec dbms_scheduler.stop_job(job_name => '${SCHEMA}.J_${SCHEMA}', force => TRUE);")
	    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't stop refresh group"

	    # wait 10s to let the job end
	    sleep 10
	fi
    done

    info "OK::job stopped"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Disable refresh job ${SCHEMA}.J_${SCHEMA}"

    SQL=$(add_sql_header "exec dbms_scheduler.disable('${SCHEMA}.J_${SCHEMA}');")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't disable refresh group"

    info "OK::refresh job disabled"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Update refresh groups"

    typeset SQL=$(add_sql_header "select rname from dba_refresh_children where owner = '${SCHEMA}' and name = '${MVIEW}';")
    REFRESH_GROUP=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't get mview refresh group: ${REFRESH_GROUP}"

    typeset SQL=$(add_sql_header "exec dbms_refresh.add(name => '${SCHEMA}.${REFRESH_GROUP}', list => '${SCHEMA}.${NEW_MVIEW}');
exec dbms_refresh.subtract(name => '${SCHEMA}.${REFRESH_GROUP}', list => '${SCHEMA}.${MVIEW}');
commit;")
    info "query:
${SQL}"
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't update refresh group"

    info "OK::refresh group updated"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Drop mview and replace it with a synonym"

    # create a public synonym before dropping the mview to be transparent
    typeset SQL=$(add_sql_header "create or replace public synonym ${MVIEW} for ${SCHEMA}.${NEW_MVIEW};
grant select on ${SCHEMA}.${NEW_MVIEW} to public;")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't create public symonym"
    info "public synonym created"

    # drop mview
    typeset SQL=$(add_sql_header "drop materialized view ${SCHEMA}.${MVIEW};")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't drop mview ${MVIEW}"
    info "mview dropped"

    # create synonym, if MVIEW has already a number at its end, remove it
    typeset SQL=$(add_sql_header "create or replace synonym ${SCHEMA}."$(echo "${MVIEW}" | sed -e 's+[1-9]$++')" for ${SCHEMA}.${NEW_MVIEW};")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't create synonym ${MVIEW}"
    info "synonym created"

    # drop public synonym
    typeset SQL=$(add_sql_header "drop public synonym ${MVIEW};")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't drop public synonym ${MVIEW}"
    info "public synonym dropped"

    info "OK::mview dropped and synonym created"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

if [ ${SKIP_ENABLE_JOB} -eq 1 ]; then
    let CUR_STEP=${CUR_STEP}+1
    if(check_step ${CUR_STEP} ${RESUME_STEP}); then
	info "${CUR_STEP}. Enable refresh job"
	
	SQL=$(add_sql_header "exec dbms_scheduler.enable('${SCHEMA}.J_${SCHEMA}');")
	sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't enable refresh group"
	
	info "OK::refresh job enabled"
	echo ""
	[ ${PAUSE} -eq 0 ] && press_any_key
    fi
fi

info "Successfully ending"
