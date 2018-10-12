#!/bin/bash
# how to call it for eone:
# /RUNDECK/rundeck_scripts/ORACLE_DB/generate_export_lst.sh '/mnt/nas_eone' "${option.20_Tns_Server}" 'jde01_cafiz2odb08' "${option.30_Source_Schema}" "${option.40_Destination_Schema}" "${option.200_Dryrun}"

NAS="$1"
SRC_TNS_SERVER="$2"
DEST_TNS_SERVER="$3"
SRC_SCHEMA="$4"
DEST_SCHEMA="$5"
DRY_RUN="$6"

if [ -z "${NAS}" -o -z "${SRC_TNS_SERVER}" -o -z "${DEST_TNS_SERVER}" -o -z "${SRC_SCHEMA}" -o -z "${DEST_SCHEMA}" -o -z "${DRY_RUN}" ]; then
    echo "ERROR: missing parameters"
    exit 1
fi

SRC_SID=$(echo "${SRC_TNS_SERVER}" | cut -d '_' -f 1)
SRC_SERVER=$(echo "${SRC_TNS_SERVER}" | cut -d '_' -f 2)

DEST_SID=$(echo "${DEST_TNS_SERVER}" | cut -d '_' -f 1)
DEST_SERVER=$(echo "${DEST_TNS_SERVER}" | cut -d '_' -f 2)

NEW_LINE="${SRC_SCHEMA}:${DEST_SCHEMA} ${DEST_SERVER} ${DEST_SID}"
LST_FILE="${NAS}/export_prod_${SRC_SERVER}_${SRC_SID}.lst"


if [ "${DRY_RUN}" = 'true' ]; then
    echo "Dry run, nothing done."
    echo "The line we would have added to ${LST_FILE}: \"${NEW_LINE}\""
    if [ -f "${LST_FILE}" ]; then
	echo "The existing content of ${LST_FILE}:"
	cat ${LST_FILE}
    else
	echo "No existing lst file ${LST_FILE}."
    fi
else
    if [ -f "${LST_FILE}" ]; then
	# check if line to add is not already present in the existing .lst file
	grep -q -i "${NEW_LINE}" "${LST_FILE}"
	if [ $? -eq 0 ]; then
	    echo "Line already present in the .lst file on the NAS, nothing done."
	else
	    echo "Adding new line \"${NEW_LINE}\" to lst file."
	    echo "${NEW_LINE}" >> ${LST_FILE}
	fi
    else
	echo "Creating lst file with new line \"${NEW_LINE}\"."
	echo "${NEW_LINE}" > ${LST_FILE}
    fi

    echo "${LST_FILE} file new content:"
    cat ${LST_FILE}
fi
