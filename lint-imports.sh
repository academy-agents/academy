#!/bin/bash -ex
for m in $(find academy -name \*.py | sed 's%/%.%'g | sed 's/^\(.*\)\.py/\1/' ); do
  echo Checking clean import of $m

  # an import failure will cause python to exit with a unix-level failure,
  # which (because of bash -e) will cause the script to also fail with a
  # unix level failure, visible (for example) to enclosing tox.
  python -c "import $m"

done
exit 0
