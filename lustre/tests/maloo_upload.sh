#!/bin/sh

source ~/.maloorc
FILENAME=$1
echo Uploading $FILENAME to $MALOO_URL
if [ -d $FILENAME ] ; then
	pushd $FILENAME
	tar czf - * | curl -F "user_id=${MALOO_USER_ID}" -F "upload=@-" -F "user_upload_token=${MALOO_UPLOAD_TOKEN}" ${MALOO_URL} > /dev/null
	popd
else
	curl -F "user_id=${MALOO_USER_ID}" -F "upload=@${FILENAME}" -F "user_upload_token=${MALOO_UPLOAD_TOKEN}" ${MALOO_URL} > /dev/null
fi
echo Complete.
