#!/bin/bash
# Adds Whamcloud copyright notices to files modified by Whamcloud.
# Does not add copyright notices to files that are missing them
#
# Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.
# Copyright (c) 2012, Intel Corporation.
#

TMP=${TMP:-/tmp}
TMPFILE=$(mktemp $TMP/updatecopy.XXXXXX)
EXCFILE=$(mktemp $TMP/excludecopy.XXXXXX)
NOW=$(date +%Y)
DIRS=${DIRS:-"build ldiskfs libcfs lnet lustre snmp lustre-iokit"}
EXCLUDE=${EXCLUDE:-'e3a7c58aebafce40323db54bf6056029e5af4a70\n
65701b4a30efdb695776bcf690a2b3cabc928da1'}

ORACOPY1="Copyright.*Oracle.*"
ORACOPY2="Use is subject to license terms."
WHAMCOPY=${WHAMCOPY:-"Copyright.*Whamcloud, Inc."}
INTCOPY=${INTCOPY:-"Copyright.*Intel Corporation."}
AUTHOR=${AUTHOR:-".*@whamcloud.com"}
START=${START:-"2010-06-01"}
ECHOE=${ECHOE:-"echo -e"}
[ "$($ECHOE foo)" = "-e foo" ] && ECHOE=echo

echo -e $EXCLUDE > $EXCFILE

git ls-files $DIRS | grep -v ${0##*/} | while read FILE; do
	NEEDCOPY=false
	# Pick only files that have changed since START
	git log --follow --since=$START --author=$AUTHOR \
		--pretty=format:"%ci %H" $FILE | grep -v -f $EXCFILE \
		| cut -d- -f1 > $TMPFILE

	# Skip files not modified by $AUTHOR
	[ -s "$TMPFILE" ] || continue

	OLDCOPY="$(egrep "$ORACOPY1|$ORACOPY2|$WHAMCOPY|$INTCOPY" \
						 $FILE|tail -n1|tr / .)"
	if [ -z "$OLDCOPY" ]; then
		case $FILE in
			*.[ch]) echo "$FILE: ** NO COPYRIGHT **" && continue ;;
			*) continue ;;
		esac
	fi 

	OLDCOPY="$(egrep "$WHAMCOPY|$INTCOPY" $FILE|tail -n1|tr / .)"
	if [ -z "$(echo "$OLDCOPY" | grep -e "$INTCOPY" -e "$WHAMCOPY")" ];
	then
		NEEDCOPY=true
		OLDCOPY="$(egrep "$ORACOPY1|$ORACOPY2|$WHAMCOPY|$INTCOPY" \
                                                 $FILE|tail -n1|tr / .)"
	elif [ ! -z "$(echo "$OLDCOPY" | grep -e "$INTCOPY")" ]; then
		# Skip files that already have a copyright for this year
		echo "$OLDCOPY" | grep -q "$NOW" && continue
	fi

	# Get commit dates
	NEWYEAR=$(head -1 $TMPFILE)
	OLDYEAR=$(tail -1 $TMPFILE)

	[ $NEWYEAR == $OLDYEAR ] && YEAR="$NEWYEAR" || YEAR="$OLDYEAR, $NEWYEAR"
	COMMENT=$(echo "$OLDCOPY" | sed -e "s/^\( *[^A-Z]*\) [A-Z].*/\1/")
	NEWCOPY=$(sed -e "s/^/$COMMENT /" -e "s/\.\*/ (c) $YEAR, /" <<<$INTCOPY)

	# '.\"' as a COMMENT isn't escaped correctly
	if [ "$COMMENT" == ".\\\"" ]; then # add " to fix vim
		echo "$FILE: *** EDIT MANUALLY ***"
		continue
	fi
	# If copyright is unchanged (excluding whitespace), we're done
	[ "$OLDCOPY" == "$NEWCOPY" ] && continue

	[ ! -w $FILE ] && echo "Unable to write to $FILE" && exit 1

	echo "$FILE: Copyright $YEAR"
	if $NEEDCOPY; then
		# Add a new copyright line after the existing copyright.
		# Using a temporary file is ugly, but I couldn't get
		# newlines into the substitution pattern for some reason,
		# and this is not a performance-critical process.
		$ECHOE "${COMMENT}\n${NEWCOPY}" > $TMPFILE
		sed -e "/$OLDCOPY/r $TMPFILE" $FILE > $FILE.tmp
	else
		# Replace the old copyright line with a new copyright
		sed -e "s/.*$INTCOPY/$NEWCOPY/" -e "s/.*$WHAMCOPY/$NEWCOPY/" \
							$FILE > $FILE.tmp
	fi
	[ -s $FILE.tmp ] && cp $FILE.tmp $FILE
	rm $FILE.tmp
done
rm $TMPFILE
