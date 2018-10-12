#!/bin/bash
################################################################################
# SCRIPT:     duplicate_mv.sh
# SYNOPSIS:   create a new materialized view to replace a big one which takes forever to be refreshed
# USAGE:      duplicate_mv.sh -m SCHEMA -p PASSWORD -v MVIEW [-s STEP] [-y] [-z] [-d]
# PARAMETERS:
#  mandatory:
#	-r ServerTnsEntry   
#   -m SCHEMA: the schema owner of the mview
#   -p PASSWORD: the schema password
#   -v MVIEW: the mview to replace
#  optional:
#   -s STEP: the step to restart at
#   -y: pause between step
#   -z: debug mode
#   -d: don't reactivate the refresh job at the end
# INFOS:
#  This script must be launched on the server where the database resides with the environment variables set.
#
#  This script create a new mview to replace a big one which takes forever to be refreshed.
#  It adds a number at the end of the new mview name.
#  It create a public synonym before droping the old mview, this way the mview content is always accessible.
#  Once the old mview is dropped, it's remplaced with a synonym and the public synonym is dropped.
#
#  Actions performed (with example values to recreate PRIX_UNIT_CESSION_MAG mview in MD0000):
#   1. Check enough free space in tablespace MD0000_SNAP_DATA.
#   2. Create new mview in MD0000 and perform a fast refresh.
#   3. Add indexes and count them
#   4. Compute stats on PRIX_UNIT_CESSION_MAG2.
#   5. Add grants.
#   6. Add mview log.
#   9. Update refresh groups.
#   10. Drop mview and replace it by a synonym.
#
#  Example with parameters to recreate PRIX_UNIT_CESSION_MAG mview in MD0000:
#   ./duplicate_mv.sh -m MD0000 -p XXXXXX -v PRIX_UNIT_CESSION_MAG
#
#  If an error occures during a step the script quits and display the command to
#  execute to resume at that step once the error has been fixed.
################################################################################

function usage {
    # display usage
    grep -E '^# USAGE:' "$1"
}

function timestamp {
    # display the current time
    date +'%b %d %H:%M:%S'
}

function info {
    # display on stdout
    echo "$(timestamp) INFO: ${*}."
}

function warning {
    # display on stdout
    echo "$(timestamp) WARNING: ${*}."
}

function error {
    # display on stderr
    echo "$(timestamp) ERROR: ${*}." >&2
}

function check_env_vars {
    # check if given parameters are environment variables
    typeset RET=0
    while [ $# -ne 0 ]; do
        if [ -z "`eval echo \"$\"\"$1\"`" ]; then
            error "$1"" variable not set"
            RET=1
        fi
        shift
    done

    return $RET
}

function die {
    # display an error message, the command to restart at the error step then dies
    error "$1"
    echo "" >&2

    if [ "$CUR_STEP" != "0" ]; then
        [ ${DEBUG} -eq 0 ] && DEBUG_TXT="-z"
        [ ${PAUSE} -eq 0 ] && PAUSE_TXT="-y"
        [ ${SKIP_ENABLE_JOB} -eq 0 ] && SKIP_TXT="-d"
        echo "Fix the issue then to restart the script at the failed step use these parameters:" >&2
        echo "./duplicate_mv.sh -m ${SCHEMA} -p ${PASSWORD} -v ${MVIEW} -s ${CUR_STEP} ${DEBUG_TXT} ${PAUSE_TXT} ${SKIP_TXT}" >&2
    fi

    exit 1
}

function press_any_key {
    # wait for the user to press Enter
    info "Press Enter to continue or Ctrl-C to cancel..."
    read
}

function check_step {
    # return 0 if STEP >= RESUME_STEP
    typeset STEP="$1"
    typeset RESUME_STEP="$2"

    [ ${STEP} -ge ${RESUME_STEP} ]
}

function get_parameters {
    # parse command line arguments
    # set the IGNORE_PARAMS which count the number of params handled
    # by getopts and to skip in the caller
    # -v MVIEW
    export MVIEW=""
    # -m SCHEMA
    export SCHEMA=""
    # -p PASSWORD
    export PASSWORD=""
    # -s STEP
    export RESUME_STEP=1
    # -y
    export PAUSE=1
    # -z
    export DEBUG=1
    # -d
    export SKIP_ENABLE_JOB=1
	# -r
	export SERVER_TNS_ENTRY=""
	

    while getopts ":m:p:v:s:yzdr:" opt "$@"; do
        case $opt in
            v)
                export MVIEW=$(echo ${OPTARG} | tr '[:lower:]' '[:upper:]')
                info "Use mview ${MVIEW}"
                ;;
			r)
                export SERVER_TNS_ENTRY=$(echo ${OPTARG} | tr '[:lower:]' '[:upper:]')
                info "Use server_tns_entry ${SERVER_TNS_ENTRY}"
                ;;				
            m)
                export SCHEMA=$(echo ${OPTARG} | tr '[:lower:]' '[:upper:]')
                info "Use schema ${SCHEMA}"
                ;;
            p)
                export PASSWORD="$OPTARG"
                info "Use schema password ${PASSWORD}"
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
            d)
                export SKIP_ENABLE_JOB=0
                info "Don't enable refresh job at the end"
                ;;
            \?)
                warning "Invalid option: -""$OPTARG"
                ;;
            :)
                warning "Option -""$OPTARG"" requires an argument"
                ;;
        esac
    done

    export IGNORE_PARAMS=$(($OPTIND -1))
}

function get_tbs_name {
    typeset MASTER="$1"
    typeset TYPE="$2"
    typeset SQL=$(add_sql_header "select tablespace_name from dba_tablespaces where tablespace_name like '${MASTER}%${TYPE}';")
    typeset RC=0

    RESULT=$(sqlplus_sysdba_check_errors "$SQL")
    RC=$?

    echo "$RESULT"

    return $RC
}

function get_tbs_free_space {
    # returns the tbs free space in Bytes
    typeset TBS="$1"

    typeset SQL=$(add_sql_header "SELECT to_char(df.maxbytes - (df.bytes - fs.bytes))
FROM (SELECT tablespace_name, SUM(bytes) bytes
    FROM dba_free_space
    where tablespace_name = '${TBS}'
    GROUP BY tablespace_name) fs,
   (SELECT tablespace_name, SUM(bytes) bytes, SUM(GREATEST(maxbytes, bytes)) maxbytes
    FROM dba_data_files
    where tablespace_name = '${TBS}'
    GROUP BY tablespace_name) df
WHERE fs.tablespace_name(+) = df.tablespace_name;")
    typeset RC=0

    RESULT=$(sqlplus_sysdba_check_errors "${SQL}")
    RC=$?

    echo "${RESULT}"

    return ${RC}
}

function get_mview_size {
    # return the mview size in bytes
    typeset SCHEMA="${1}"
    typeset MVIEW="${2}"

    typeset SQL=$(add_sql_header "select to_char(sum(bytes)) from dba_segments where segment_name = '${MVIEW}' and owner = '${SCHEMA}';")

    RESULT=$(sqlplus_sysdba_check_errors "${SQL}")
    RC=$?

    echo "${RESULT}"

    return ${RC}
}

function get_mview_indexes_count {
    # return the number of indexes for an mview
    typeset SCHEMA="${1}"
    typeset MVIEW="${2}"

    typeset SQL=$(add_sql_header "select to_char(count(*)) from dba_indexes where table_name = '${MVIEW}' and owner = '${SCHEMA}';")

    RESULT=$(sqlplus_sysdba_check_errors "${SQL}")
    RC=$?

    echo "${RESULT}"

    return ${RC}
}

function add_sql_header {
    # set params to sql request to format its output
    typeset SQL="$1"

    echo 'set head off feedback off pagesize 0
set trimspool off linesize 512 echo off'"
$SQL"
}

function add_pretty_sql_header {
    # set params to format the output for dbms_metadata and longs
    typeset SQL="$1"

    echo "set head off feedback off pagesize 0
set trimspool off linesize 4096 echo off
set long 200000000 longchunksize 2000000
exec dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'SQLTERMINATOR', true);
exec dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'PRETTY', true);
$SQL"
}

function sqlplus_sysdba {
    # execute a request on the instance connected as sys, do not check the output for errors
    # ORACLE_SID must be set
    typeset SQL="$1"

    # check if SQL query or SQL script file
    SQL_FILE=$(mktemp -u)'.sql'
    if [ -f "$SQL" ]; then
        cp "$SQL" "$SQL_FILE"
    else
        echo "$SQL" > "$SQL_FILE"
    fi
    echo "exit;" >> "$SQL_FILE"

    /usr/bin/sqlplus64 -S -L /@${SERVER_TNS_ENTRY}_oraexploit @$SQL_FILE
    typeset RC=$?

    rm -f "$SQL_FILE"

    return $RC
}

function sqlplus_sysdba_check_errors {
    # execute a request on the instance connected as sys, check the output for ORA-
    typeset SQL="$1"
    typeset DEBUG="$2"

    typeset RC=0
    typeset LOG_FILE=$(mktemp)
    if [ -n "${DEBUG}" ] && [ ${DEBUG} -eq 0 ]; then
        info "DEBUG::SQL=${SQL}"
        touch "${LOG_FILE}"
    else
        sqlplus_sysdba "${SQL}" 2>&1 | tee "${LOG_FILE}"
    fi

    # check sqlplus_sysdba exit code with bash specific PIPESTATUS variable
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        error "SQL*Plus exit code != 0"
        RC=1
    fi

    grep -q 'ORA-' "$LOG_FILE"
    if [ $? -eq 0 ]; then
        error "'ORA-' detected while executing sql request"
        RC=1
    fi

    rm -f "$LOG_FILE"

    return $RC
}

function sqlplus_user {
    # execute a request on the instance connected as a user, do not check the output for errors
    # ORACLE_SID must be set
    typeset USER="$1"
    typeset PASSWORD="$2"
    typeset SQL="$3"

    # check if SQL query or SQL script file
    SQL_FILE=$(mktemp -u)'.sql'
    if [ -f "${SQL}" ]; then
        cp "${SQL}" "${SQL_FILE}"
    else
        echo "$SQL" > "${SQL_FILE}"
    fi
    echo "exit;" >> "${SQL_FILE}"

    /usr/bin/sqlplus64 -S -L "${USER}/${PASSWORD}"@${SERVER_TNS_ENTRY} @${SQL_FILE}
    typeset RC=$?

    rm -f "${SQL_FILE}"

    return ${RC}
}

function sqlplus_user_check_errors {
    # execute a request on the instance connected as a user, check the output for ORA-
    typeset USER="$1"
    typeset PASSWORD="$2"
    typeset SQL="$3"
    typeset DEBUG="$4"

    typeset RC=0
    typeset LOG_FILE=$(mktemp)
    if [ -n "${DEBUG}" ] && [ ${DEBUG} -eq 0 ]; then
        info "DEBUG::SQL=${SQL}"
        touch ${LOG_FILE}
    else
        sqlplus_user "${USER}" "${PASSWORD}" "${SQL}" 2>&1 | tee "${LOG_FILE}"
    fi

    # check sqlplus_sysdba exit code with bash specific PIPESTATUS variable
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        error "SQL*Plus exit code != 0"
        RC=1
    fi

    grep -q 'ORA-' "${LOG_FILE}"
    if [ $? -eq 0 ]; then
        error "'ORA-' detected while executing sql request"
        RC=1
    fi

    rm -f "$LOG_FILE"

    return $RC
}

# Main

get_parameters "$@"
shift $IGNORE_PARAMS

# check that the vars are set
if( ! check_env_vars "ORACLE_HOME" "SCHEMA" "PASSWORD" "MVIEW" "SERVER_TNS_ENTRY"); then
    usage "$0"
    die "Missing mandatory params"
fi

export SNAP_DATA_TBS="${SCHEMA}_SNAP_DATA"
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
    info "${CUR_STEP}. Check enough free space in tablespace ${SNAP_DATA_TBS}"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Create new mview in ${SCHEMA} and perform a fast refresh"
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
    info "${CUR_STEP}. Update refresh groups"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Drop mview and replace it by a synonym"
fi
echo ""


export CUR_STEP=1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Check enough free space in tablespace ${SNAP_DATA_TBS}"

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
    info "${CUR_STEP}. Create new mview in ${SCHEMA} and perform a fast refresh"

    # get the query of the mview in SCHEMA
    typeset SQL=$(add_pretty_sql_header "select query from dba_mviews where mview_name = '${MVIEW}' and owner = '${SCHEMA}';")
    typeset QUERY=$(sqlplus_sysdba_check_errors "$SQL") || die "Can't get mview ${MVIEW} query: ${QUERY}"
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

    DATA_TBS=$(get_tbs_name "${SCHEMA}" "SNAP_DATA") || die "Can't get tbs DATA name"
    INDEX_TBS=$(get_tbs_name "${SCHEMA}" "SNAP_INDEX") || die "Can't get tbs INDEX name"

    typeset SQL=$(add_sql_header "CREATE MATERIALIZED VIEW ${SCHEMA}.${NEW_MVIEW}
TABLESPACE ${DATA_TBS} BUILD IMMEDIATE
USING INDEX TABLESPACE ${INDEX_TBS} refresh force on demand as
${QUERY};")
    info "Mview ddl:
${SQL}"

	typeset SQL2=$(add_pretty_sql_header "alter user ${SCHEMA} account unlock;")
	sqlplus_sysdba_check_errors "${SQL2}" || die "Can't unlock the user ${SCHEMA}"
	
    sqlplus_user_check_errors "${SCHEMA}" "${PASSWORD}" "${SQL}" ${DEBUG} || die "Can't create mview ${NEW_MVIEW}"
    info "mview created"

	typeset SQL=$(add_pretty_sql_header "alter user ${SCHEMA} account lock;")
	sqlplus_sysdba_check_errors "${SQL}" || die "Can't relock the user ${SCHEMA}"

    typeset SQL=$(add_sql_header "exec dbms_mview.refresh('${SCHEMA}.${NEW_MVIEW}', 'f');")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't fast refresh mview ${NEW_MVIEW}"
    info "mview fast refreshed"

    info "OK::mview created and fast refreshed"
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
        ')'  || CHR(10)
        ||'unrecoverable ' || CHR(10)
        ||'STORAGE('                            || CHR(10)
        ||'INITIAL '     || initial_extent      || CHR(10)
        ||'NEXT '        || next_extent         || CHR(10)
        ||'MINEXTENTS ' || '1' || CHR(10)
        ||'MAXEXTENTS ' || max_extents  || CHR(10)
        ||'PCTINCREASE '|| '0'  ||')'   || CHR(10)
        ||'INITRANS '   || ini_trans         || CHR(10)
        ||'MAXTRANS '   || max_trans         || CHR(10)
        ||'PCTFREE '    || '0' || CHR(10)
        ||'TABLESPACE ${INDEX_TBS} PARALLEL (DEGREE ' || DEGREE || ') ' || CHR(10)
        ||';'||CHR(10) SQL
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
       info "Grants:${GRANTS}"
       GRANTS=$(add_sql_header "${GRANTS}")
       sqlplus_sysdba_check_errors "${GRANTS}" ${DEBUG} || die "Can't grant select on mview log"
       info "OK::mview log created"
    else
       info "OK::no mview log on ${MVIEW}"
    fi

    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Update refresh groups"

	typeset SQL=$(add_sql_header "select rname from dba_refresh_children where name = '${MVIEW}' and owner='${SCHEMA}';")
    REFRESH_GROUP=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't get mview refresh group:
${REFRESH_GROUP}"
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

info "Successfully ending"
