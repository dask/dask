export PYTEST_OPTIONS="--verbose -r s --timeout-method=thread --timeout=300 --durations=20"
if [[ $RUNSLOW != false ]]; then
    export PYTEST_OPTIONS="$PYTEST_OPTIONS --runslow"
fi

# On OS X builders, the default open files limit is too small (256)
if [[ $TRAVIS_OS_NAME == osx ]]; then
    ulimit -n 8192
fi

echo "--"
echo "-- Soft limits"
echo "--"
ulimit -a -S

echo "--"
echo "-- Hard limits"
echo "--"
ulimit -a -H

if [[ $COVERAGE == true ]]; then
    coverage run $(which py.test) distributed -m "not avoid_travis" $PYTEST_OPTIONS;
else
    py.test -m "not avoid_travis" distributed $PYTEST_OPTIONS;
fi;
