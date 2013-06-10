private boolean verbose = false;

private String YCSB_PATH = '.';
private String RESULTS_PATH = '.';
// comma separated list of hosts
private String HOSTS='127.0.0.1'

private String YCSB_HOSTS_OPTIONS = " -p $HOSTS "
private String YCSB_FAILFAST_OPTIONS = ' -p cassandra.operationretries=2 -p cassandra.connectionretries=2 ';


private String[] CREATION_FILES = {};
private String[] WORKLOADS = {};
private String[] DRIVERS = {};

for(String creationFileSuffix : CREATION_FILES){
    for(String workload : WORKLOADS){
        for(String driver : DRIVERS){
            bench("${workload}_${driver}_${creationFileSuffix}",workload,driver,creationFileSuffix);
        }
    }
}

public void bench(String executionName, String workloadName, String driver, String creationFileSuffix){
    cleanDatabase();
    importSchemaDatabase(creationFileSuffix);
    clearCaches();
    loadYCSB(executionName, workloadName, driver);
    runYCSB(executionName, workloadName, driver);
}


/*         NODETOOL           */
public void clearCaches(){
    def process = executeCommand("$CASSANDRA_HOME/bin/nodetool invalidatekeycache ");
    process = executeCommand("$CASSANDRA_HOME/bin/nodetool invalidaterowcache ");
}

/*         CQLSH              */
public void cleanDatabase(){
    def process = executeCQLSH('drop-keyspace.cql');
}

public void importSchemaDatabase(String creationFileSuffix){
    def process = executeCQLSH("creation${creationFileSuffix}.cql");
}

public void executeCQLSH(String file){
    def process = executeCommand("$CASSANDRA_HOME/bin/cqlsh.sh -f $file");
}

/*         YCSB               */
public void runYCSB(String executionName, String workloadName, String driver){
   def process = executeYCSB('run', executionName, workloadName, driver);
}

public void loadYCSB(String executionName, String workloadName, String driver){
    def process = executeYCSB('load', executionName, workloadName, driver);
}


public def executeYCSB(String ycsbCommand, String executionName, String workloadName, String driver){
    String timestamp = date.format('yyyy-MM-dd-hh-mm');
    def command = "$YCSB_PATH/bin/ycsb $ycsbCommand $driver -P $YCSB_PATH/$workloadName $YCSB_HOSTS_OPTIONS $YCSB_FAILFAST_OPTIONS > ${RESULTS_PATH}/${executionName}_${ycsbCommand}_${timestamp}.txt");

    return executeCommand(command, true);
}

protected def executeCommand(String commandLabel, boolean discardOutput) {
    def processOutput = [:]
    if (verbose) {
        printOut.println '[DEBUG] executing command \'' + commandLabelToPrint + '\''
    }
    def process = commandLabel.execute()

    def outBuffer = new ByteArrayOutputStream()
    def errBuffer = new ByteArrayOutputStream()

    if(discardOutput){
        if (verbose){
            process.consumeProcessOutput(printOut, printOut)
        } else {
            // discard output, see http://groovy.codehaus.org/groovy-jdk/java/lang/Process.html#consumeProcessOutput()
            process.consumeProcessOutput();
        }
    }else{
        process.consumeProcessOutput(outBuffer, errBuffer)
    }

    process.waitFor()

    if (verbose) {
        if(!discardOutput){
            printOut.println outBuffer.toString()
            printOut.println errBuffer.toString()
        }
        printOut.println '[DEBUG] exit value : ' + process.exitValue()
    }
    processOutput.exitValue = process.exitValue()
    processOutput.inText = outBuffer.toString()
    processOutput.inErrText = errBuffer.toString()

    return processOutput
}