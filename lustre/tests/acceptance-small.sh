#!/bin/bash
DEFAULT_SUITES=$(cat test-groups/regression)
for SUB in $DEFAULT_SUITES; do
	ENV=$(echo $SUB | tr "[:lower:]-" "[:upper:]_")
	[ "$(eval echo \$$ENV)" = "no" ] && continue
	SUITES="$SUITES $SUB"
done
sh auster -r -v -f ${NAME:-lustre} $SUITES
