
verbose = true;

CASSANDRA_HOME='/work/tools/cassandra/current'
YCSB_PATH = '/work/sources/ycsb/YCSB';
WORKLOAD_PATH = "$YCSB_PATH/workloads";
RESULTS_PATH = '.';
// comma separated list of hosts
HOSTS='127.0.0.1'

YCSB_HOSTS_OPTIONS = " -p hosts=$HOSTS -p cassandra.compression=true "
YCSB_FAILFAST_OPTIONS = ' -p cassandra.operationretries=2 -p cassandra.connectionretries=2 ';

CREATION_FILES = ['','index'];
WORKLOADS = ["workloada","workloadb","workloadc","workloadd","workloadf","workloadlongread","workloadlonginsert","workloadlongrw"];
DRIVERS = ["cassandra-10","cassandra-12-ps"];

for(String creationFileSuffix : CREATION_FILES){
    for(String workload : WORKLOADS){
        for(String driver : DRIVERS){
            bench("${workload}_${driver}_${creationFileSuffix}",workload,driver,creationFileSuffix);
        }
    }
}

def void bench(String executionName, String workloadName, String driver, String creationFileSuffix){
    cleanDatabase();
    importSchemaDatabase(creationFileSuffix);
    clearCaches();
    loadYCSB(executionName, workloadName, driver);
    runYCSB(executionName, workloadName, driver);
}


/*         NODETOOL           */
def void clearCaches(){
    def process = executeCommand("${CASSANDRA_HOME}/bin/nodetool invalidatekeycache ",true, null);
    process = executeCommand("${CASSANDRA_HOME}/bin/nodetool invalidaterowcache ", true, null);
}

/*         CQLSH              */
def void cleanDatabase(){
    def process = executeCQLSH('drop-keyspace.cql');
}

def void importSchemaDatabase(String creationFileSuffix){
    def process = executeCQLSH("creation${creationFileSuffix}.cql");
}

def void executeCQLSH(String file){
    def process = executeCommand("$CASSANDRA_HOME/bin/cqlsh -f $file", true, null);
}

/*         YCSB               */
def void runYCSB(String executionName, String workloadName, String driver){
   def process = executeYCSB('run', executionName, workloadName, driver);
}

def void loadYCSB(String executionName, String workloadName, String driver){
    def process = executeYCSB('load', executionName, workloadName, driver);
}


def executeYCSB(String ycsbCommand, String executionName, String workloadName, String driver){
    String timestamp = new Date().format('yyyy-MM-dd-hh-mm');
    def command = "${YCSB_PATH}/bin/ycsb ${ycsbCommand} ${driver} -P ${WORKLOAD_PATH}/${workloadName} ${YCSB_HOSTS_OPTIONS} ${YCSB_FAILFAST_OPTIONS}"
    //>> ${RESULTS_PATH}/${executionName}_${ycsbCommand}_${timestamp}.txt";

    return executeCommand(command, false, "${RESULTS_PATH}/${executionName}_${ycsbCommand}_${timestamp}.out");
}


def executeCommand(String commandLabel) {
    executeCommand(commandLabel, true, null)
}
def executeCommand(String commandLabel, boolean discardOutput, String outputFile) {
    def processOutput = [:]
    if (verbose) {
        println '[DEBUG] executing command \'' + commandLabel + '\''
    }
    def process = commandLabel.execute()


    if(discardOutput){
        if (verbose){
            process.consumeProcessOutput(System.out, System.err)
        } else {
            // discard output, see http://groovy.codehaus.org/groovy-jdk/java/lang/Process.html#consumeProcessOutput()
            process.consumeProcessOutput();
        }
    }else{
        def outBuffer = new FileOutputStream(outputFile)
        def errBuffer = new FileOutputStream(outputFile+'-err')
        process.consumeProcessOutput(outBuffer, errBuffer)
    }

    process.waitFor()

    if (verbose) {
        if(!discardOutput){
            //println outBuffer.toString()
            //println errBuffer.toString()
        }
        println '[DEBUG] exit value : ' + process.exitValue()
    }
    processOutput.exitValue = process.exitValue()
    //processOutput.inText = outBuffer.toString()
    //processOutput.inErrText = errBuffer.toString()

    return processOutput
}
