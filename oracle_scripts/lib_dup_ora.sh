#!/bin/bash
# helper functions for mviews creation/duplication

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
    # display an error on stdout
    echo "$(timestamp) ERROR: ${*}."
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

function get_tbs_name {
    typeset MASTER="$1"
    typeset TYPE="$2"
    typeset SQL=$(add_sql_header "select tablespace_name from dba_tablespaces where tablespace_name like '${MASTER}\_%${TYPE}' escape '\';")
    typeset RC=0

    RESULT=$(sqlplus_sysdba_check_errors "$SQL")
    RC=$?

    echo "$RESULT"

    return $RC
}

function get_master_db_link {
    typeset SCHEMA="$1"

    typeset SQL=$(add_sql_header "select distinct master_link from dba_mviews where owner = '${SCHEMA}' and  master_link is not null;")
    typeset RC=0

    RESULT=$(sqlplus_sysdba_check_errors "$SQL")
    RC=$?

    if [ ${RC} -eq 0 ]; then
	# check if more than one db link found
	NB_DB_LINKS=$(echo "${RESULT}" | wc -l)
	if [ ${NB_DB_LINKS} -ne 1 ]; then
	    error "More than one db link found:"
	    echo "${RESULT}"
	    return 1
	else
	    # found one db link
	    echo "${RESULT}"
	    return 0
	fi
    else
	echo "$RESULT"
	return ${RC}
    fi
}

function get_syn_users {
    typeset SCHEMA="$1"
    typeset SQL=$(add_sql_header "select distinct owner from dba_synonyms where table_owner = '${SCHEMA}' and owner != table_owner order by 1;")

    sqlplus_sysdba_check_errors "${SQL}"
}

function get_grantees {
    typeset SCHEMA="$1"
    typeset SQL=$(add_sql_header "select distinct tp.grantee from dba_tab_privs tp join dba_users u on tp.grantee = u.username where owner = '${SCHEMA}' union select distinct tp.grantee from dba_tab_privs tp join dba_roles r on tp.grantee = r.role where owner = '${SCHEMA}' order by 1;")

    sqlplus_sysdba_check_errors "${SQL}"
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

function get_tbs_size {
    # returns the tbs size on disk in Bytes
    typeset TBS="$1"
    typeset SQL=$(add_sql_header "select to_char(sum(bytes)) from dba_data_files where tablespace_name = upper('${TBS}');")
    typeset RC=0

    RESULT=$(sqlplus_sysdba_check_errors "$SQL")
    RC=$?

    echo "$RESULT"

    return $RC
}

function create_tbs {
    # create a tablespace, if size > 32G, create multiple datafiles in the tbs
    typeset TBS_NAME="$1"
    typeset TBS_SIZE="$2"
    typeset FS="$3"

    typeset DATAFILE_TEMPLATE="${FS}/${ORACLE_SID}/$(echo ${TBS_NAME} | tr '[:upper:]' '[:lower:]').data"

    let MAX_SIZE=32767*1024*1024
    typeset DATAFILE_COUNT=1
    typeset SQL=$(add_sql_header "create tablespace ${TBS_NAME} datafile '${DATAFILE_TEMPLATE}${DATAFILE_COUNT}' size 1G autoextend on next 256M maxsize unlimited;")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || return 1
    let TBS_SIZE=${TBS_SIZE}-${MAX_SIZE}

    while [ ${TBS_SIZE} -gt 0 ]; do
	let DATAFILE_COUNT=${DATAFILE_COUNT}+1
	typeset SQL=$(add_sql_header "alter tablespace ${TBS_NAME} add datafile '${DATAFILE_TEMPLATE}${DATAFILE_COUNT}' size 1G autoextend on next 256M maxsize unlimited;")
	sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || return 1
	let TBS_SIZE=${TBS_SIZE}-${MAX_SIZE}
    done

    return 0
}

function mview_exists {
    typeset SCHEMA="${1}"
    typeset MVIEW="${2}"

    typeset SQL=$(add_sql_header "select 'OK' from dba_mviews where owner = '${SCHEMA}' and mview_name = '${MVIEW}';")

    RESULT=$(sqlplus_sysdba_check_errors "${SQL}")
    if [ $? -eq 0 ]; then
	[ "${RESULT}" = 'OK' ]
    else
	die "Error while checking if the mview exists:
${RESULT}"
    fi
}

function get_mview_size {
    # return the mview size in bytes
    typeset SCHEMA="${1}"
    typeset MVIEW="${2}"

    typeset SQL=$(add_sql_header "select to_char(nvl(sum(bytes), 0)) from dba_segments where segment_name = '${MVIEW}' and owner = '${SCHEMA}';")

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

function get_refresh_jobs {
    # return the job_name of the refresh jobs of SCHEMA
    typeset SCHEMA="${1}"

    typeset SQL=$(add_sql_header "select distinct(rname) from dba_refresh_children where owner = '${SCHEMA}';")
    R_GROUPS=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't get ${SCHEMA} refresh groups: ${R_GROUPS}"

    typeset SQL="select job_name from dba_scheduler_jobs where owner = '${SCHEMA}' and (1=2 "
    for GROUP in ${R_GROUPS}; do
	SQL="${SQL} or job_action like '%${GROUP}%'"
    done
    SQL="${SQL} );"
    SQL=$(add_sql_header "${SQL}")

    sqlplus_sysdba_check_errors "${SQL}" || die "Can't get ${SCHEMA} jobs: ${JOBS}"
}

function get_job_state {
    # return the state of the job (RUNNING, ...)
    typeset JOB="${1}"

    typeset SQL=$(add_sql_header "select trim(state) from dba_scheduler_jobs where owner = '${MASTER_SCHEMA}' and job_name = '${JOB}';")
    sqlplus_sysdba_check_errors "${SQL}" || die "Can't get refresh job ${JOB} state"
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

    SQL_FILE=$(mktemp -u)'.sql'

    # there can be parameters to the SQL script
    typeset SCRIPT=$(echo "$SQL" | cut -d ' ' -f 1)
    typeset PARAMS=$(echo "$SQL" | cut -d ' ' -f 2,3,4,5,6,7,8,9)

    # check if SQL query or SQL script file
    if [ -f "${SCRIPT}" ]; then
	cp "${SCRIPT}" "${SQL_FILE}"
    else
	echo "${SQL}" > "${SQL_FILE}"
	PARAMS=""
    fi
    echo "exit;" >> "${SQL_FILE}"

    if [ -n "${SYS_PASSWORD}" -a -n "${TNS_ENTRY}" ]; then
	sqlplus -S -L "sys/${SYS_PASSWORD}@${TNS_ENTRY} as sysdba" @${SQL_FILE} ${PARAMS}
    else
	sqlplus -S -L '/ as sysdba' @${SQL_FILE} ${PARAMS}
    fi
    typeset RC=$?

    rm -f "$SQL_FILE"

    return $RC
}

function sqlplus_sysdba_check_errors {
    # execute a request on the instance connected as sys, check the output for ORA-
    typeset SQL="$1"
    typeset DEBUG="$2"

    # display the sql requests to be executed on stderr
    info "SQL to execute as sysdba:" >&2
    echo "${SQL}" | sed -e 's+^+  +' >&2

    typeset RC=0
    typeset LOG_FILE=$(mktemp)
    if [ -n "${DEBUG}" ] && [ ${DEBUG} -eq 0 ]; then
	touch "${LOG_FILE}"
    else
	sqlplus_sysdba "${SQL}" 2>&1 | tee "${LOG_FILE}"
    fi

    # check sqlplus_sysdba exit code with bash specific PIPESTATUS variable
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
	error "SQL*Plus exit code != 0"
	RC=1
    fi

    grep -q -E 'ORA-|SP2-' "$LOG_FILE"
    if [ $? -eq 0 ]; then
        error "'ORA-' or 'SP2-' detected while executing sql request"
        RC=1
    fi

    rm -f "$LOG_FILE"

    return $RC
}

function sqlplus_user {
    # execute a request on the instance connected as a user, do not check the output for errors
    # ORACLE_SID must be set
    typeset USER="$1"
    typeset SQL="$2"

    # check if SQL query or SQL script file
    SQL_FILE=$(mktemp -u)'.sql'
    if [ -f "${SQL}" ]; then
	cp "${SQL}" "${SQL_FILE}"
    else
	echo "$SQL" > "${SQL_FILE}"
    fi
    echo "exit;" >> "${SQL_FILE}"

    sqlplus -S -L "${USER}/${USER}" @${SQL_FILE}
    typeset RC=$?

    rm -f "${SQL_FILE}"

    return ${RC}
}

function sqlplus_user_check_errors {
    # execute a request on the instance connected as a user, check the output for ORA-
    typeset USER="$1"
    typeset SQL="$2"
    typeset DEBUG="$3"
    
    # display the sql requests to be executed on stderr
    info "SQL to execute as ${USER}:" >&2
    echo "${SQL}" | sed -e 's+^+  +' >&2

    typeset RC=0
    typeset LOG_FILE=$(mktemp)
    if [ -n "${DEBUG}" ] && [ ${DEBUG} -eq 0 ]; then
	touch ${LOG_FILE}
    else
	sqlplus_user "${USER}" "${SQL}" 2>&1 | tee "${LOG_FILE}"
    fi

    # check sqlplus_sysdba exit code with bash specific PIPESTATUS variable
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
	error "SQL*Plus exit code != 0"
	RC=1
    fi

    grep -q -E 'ORA-|SP2-' "${LOG_FILE}"
    if [ $? -eq 0 ]; then
        error "'ORA-' or 'SP2-' detected while executing sql request"
        RC=1
    fi

    rm -f "$LOG_FILE"

    return $RC
}

function is_mview_discarded {
    # some mviews have been discontinued, for now we only have a masterdatas one
    typeset MVIEW="$1"

    [ ${MVIEW} = 'PARAMETRES_MASTERDATAS' ]
}

function get_fixed_mview {
    # mviews which have been recreated have a number at their end, remove it
    typeset MVIEW="$1"

    echo "${MVIEW}" | sed -e 's+[1-9]$++'
}

function is_mview_fast_refreshed {
    typeset SCHEMA="$1"
    typeset MVIEW="$2"

    typeset SQL=$(add_sql_header "select last_refresh_type from dba_mviews where owner = '${SCHEMA}' and mview_name = '${MVIEW}';")

    RESULT=$(sqlplus_sysdba_check_errors "${SQL}")

    [ "${RESULT}" = 'FAST' ]
}

function are_mlogs_in_schema {
    typeset SCHEMA="${1}"
    typeset SQL=$(add_sql_header "select count(*) from dba_mview_logs where log_owner = '${SCHEMA}';")

    RESULT=$(sqlplus_sysdba_check_errors "${SQL}")

    [ ${RESULT} -gt 0 ]
}
