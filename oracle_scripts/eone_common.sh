#!/bin/bash

function timestamp {
    # display the current time
    date +'%b %d %H:%M:%S'
}

function info {
    # display an info
    echo "$(timestamp) INFO: ${*}."
}

function warning {
    # display a warning
    echo "$(timestamp) WARNING: ${*}."
}

function error {
    # display an error
    echo "$(timestamp) ERROR: ${*}."
}

function die {
    # display an error message, then dies
    error "${*}"
    exit 1
}

function add_sql_header_feedback {
    # set params to sql request to format its output
    typeset SQL="${1}"

    echo "set head off feedback on pagesize 0
set trimspool on linesize 512 echo on
${SQL}"
}

function add_sql_header_display {
    # set params to sql request to format its output
    typeset SQL="$1"

    echo "set feedback on pagesize 9999
set trimspool on linesize 512 echo off
${SQL}"
}

function add_sql_pretty {
    # set params to sql request to format its output
    typeset SQL="${1}"

    echo "set long 200000000 longchunksize 2000000 pagesize 0 linesize 1000 feedback off verify off trimspool on
column ddl format a1000

begin
   dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'SQLTERMINATOR', true);
   dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'PRETTY', true);
end;
/

${SQL}"
}

function add_sql_header {
    # set params to sql request to format its output
    typeset SQL="${1}"

    echo "set head off feedback off pagesize 0
set trimspool on linesize 512 echo off
${SQL}"
}

function sqlplus_query {
    # execute a request on the instance connected with the dba user, do not check the output for errors
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

    sqlplus -S -L "${RD_OPTION_30_DBA_USER}/\"${RD_OPTION_40_DBA_PWD}\"@${RD_OPTION_20_TNS_SERVER}" @${SQL_FILE} ${PARAMS}
    typeset RC=$?

    rm -f "$SQL_FILE"

    return $RC
}

function sqlplus_check_errors {
    # execute a request on the instance connected with dba account, check the output for ORA-
    typeset SQL="$1"
    typeset IGNORE="$2"

    typeset RC=0
    typeset LOG_FILE=$(mktemp)
    sqlplus_query "${SQL}" 2>&1 | tee "${LOG_FILE}"

    # check sqlplus_query exit code with bash specific PIPESTATUS variable
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
	error "SQL*Plus exit code != 0"
	RC=1
    fi

    grep -E 'ORA-|SP2-' "$LOG_FILE" |
    if [ -n "${IGNORE}" ]; then
        grep -E -v "${IGNORE}";
    else
        cat <&0;
    fi |
    grep -q -E 'ORA-|SP2-'
    if [ $? -eq 0 ]; then
        error "'ORA-' or 'SP2-' detected while executing sql request"
        RC=1
    fi

    rm -f "$LOG_FILE"

    return $RC
}

function get_prefix {
    typeset SERVER="${1}"

    # depending on the env (prod or dev) the prefix for the users is not the same
    case ${SERVER} in
        'xxxx1odb08'|'xxxx1odb01') echo 'PD___';;
        'xxxz1odb08'|'xxxy1odb51') echo 'PP%';;
        'xxxx1odb18'|'cafiy1odb71') echo '%';;
        *) die "Unknow server: [${SERVER}]";;
    esac
}

function get_env {
    typeset SERVER="${1}"

    case ${SERVER} in
        'xxxx1odb08'|'xxxx1odb01') echo 'a';;
        'xxxz1odb08'|'xxxz1odb51') echo 'z';;
        'xxxx1odb18'|'xxxx1odb71') echo 'd';;
        *) die "Unknow server: [${SERVER}]";;
    esac
}

function is_aws {
    typeset SERVER="${1}"

    case ${SERVER} in
        'xxxx1odb08'|'xxxz1odb08'|'xxxx1odb18') echo 1;;
        'cafiy1odb71'|'xxxy1odb51'|'xxxx1odb01') echo 0;;
        *) die "Unknow server: [${SERVER}]";;
    esac
}

function get_cm {
    # xxxx1odb08: PDCMDTA PDCMCTL
    # xxxz1odb08: PPDTA PPCTL
    # xxxx1odb18: CRPDTA CRPCTL
    # xxxx1odb01: PDCMDTA PDCMCTL
    # xxxy1odb51: PP1DTA PP1CTL
    # cafiy1odb71: CRPDTA CRPCTL
    typeset SERVER="${1}"
    # DTA or CTL
    typeset TYPE="${2}"

    case ${SERVER} in
        'xxxx1odb08'|'xxxx1odb01') echo 'PDCM'${TYPE};;
        'xxxz1odb08') echo 'PP'${TYPE};;
        'xxxy1odb51') echo 'PP1'${TYPE};;
        'xxxx1odb18'|'cafiy1odb71') echo 'CRP'${TYPE};;
        *) die "Unknow server: [${SERVER}]";;
    esac
}

function generate_password {
    # we need at least one number in the password
    echo $(cat /dev/urandom | tr -dc 'A-Z0-9' | fold -w 7 | head -1)$(cat /dev/urandom | tr -dc '0-9' | fold -w 1 | head -1)
}

function get_random_id {
    # get a 8 char/number id
    cat /dev/urandom | tr -dc 'A-Z0-9' | fold -w 8 | head -1
}

# set oracle env
export ORACLE_HOME=/usr/lib/oracle/12.1/client64
export TNS_ADMIN=${ORACLE_HOME}/network/admin
export LD_LIBRARY_PATH=${ORACLE_HOME}/lib
export PATH=${ORACLE_HOME}/bin:${PATH}
