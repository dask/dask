export PYTEST_OPTIONS="--verbose -r s --timeout-method=thread --timeout=300 --durations=20"
if [[ $RUNSLOW != false ]]; then
    export PYTEST_OPTIONS="$PYTEST_OPTIONS --runslow"
fi

if [[ $HDFS == true ]]; then
    py.test distributed/tests/test_hdfs.py $PYTEST_OPTIONS
    if [ $? -ne 0 ]; then
        # Diagnose test error
        echo "--"
        echo "-- HDFS namenode log follows"
        echo "--"
        docker exec -it $(docker ps -q) bash -c "tail -n50 /usr/local/hadoop/logs/hadoop-root-namenode-hdfs-container.log"
        (exit 1)
    fi
elif [[ $COVERAGE == true ]]; then
    coverage run $(which py.test) distributed -m "not avoid_travis" $PYTEST_OPTIONS;
else
    py.test -m "not avoid_travis" distributed $PYTEST_OPTIONS;
fi;
