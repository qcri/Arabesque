#!/usr/bin/python2.7
# Hi
import sys
import subprocess
import time

################ CONFIG VARIABLES ########

# workloads = ['citeseer', 'mico', 'patent', 'youtube', 'livej', 'orkut']

#### configurations contain tuple of [workloads], [queriesAnchored], [queriesUnanchored], [outliers_pcts], numServers, numPartitionsPerServer, outputActive

configurations = [
    (['citeseer'], [1,2,3,4,5,6,7,8], [5,6,7], ['0.001'], 1, 8, True)
]

local = True 

fastNeighbors = 'true'
#outputBinary = 'true'
outputBinary = 'false'

#minMatches = '100'
minMatches = '0'

doRun = True
doVerify = True

################ DON'T TOUCH THESE VARIABLES ########

results = {'citeseer': [19, 14, 12, 0, 0, 0, 0, 0],
           'mico': [51980, 144511, 100925, 197303, 27638, 56987, 90815, 30862],
           'patent': [31343, 17320, 904287, 904789, 4570, 1098, 2552, 2208],
           'youtube': [1086315, 2726538, 11600303, 2897261, 352490, 437620, 531482, 221375]}

# !!!!NOTE !!!!: no counter name can be a superstring of another name

counters = ['processed', 'generated', 'output', 'matched', 'cross_edges', 'comput_time']

histograms = ['trees_expanded', 'embeddings_expanded', 'embeddings_expanded_pertree',
              'times_matched', 'trees_matched', 'embeddings_matched_pertree', 'max_embeddings_matched_pt',
              'edges_matched_pertree', 'max_edges_matched_pt', 'cost_matched_pertree', 'max_cost_matched_pt',
              'size_matched_pertree', 'max_size_matched_pt', 'trees_output']

timesPerTree = ['expanding', 'matching', 'output', 'sending', 'splitting', 'estimating', 'queueing']

arrays = ['time_cost', 'time_embeddings', 'time_edges']

series = ['metrics_pertree']

numServers = 0
numPartitionsPerServer = 0
outputActive = True


################ PREPARE EXPERIMENTS ################

def writeClusterYaml(numServers, numPartitionsPerServer, outputActive):
    clusterFile = open('cluster-spark.yaml', 'w')

    if local:
        clusterFile.write('spark_master: local[*]\n')
        clusterFile.write('worker_memory: 10g\n')
        clusterFile.write('driver_memory: 8g\n')
        clusterFile.write('num_workers: 1\n')
        clusterFile.write('num_compute_threads: 6\n')
    else:
        clusterFile.write('spark_master: yarn-client\n')
        clusterFile.write('worker_memory: 200g\n')
        clusterFile.write('driver_memory: 200g\n')
        clusterFile.write('num_workers: ' + str(numServers) + '\n')
        clusterFile.write('num_compute_threads: ' + str(numPartitionsPerServer) + '\n')

    clusterFile.write('max_result_size: 5g\n')
    clusterFile.close()


def writeAppYaml(workload, queryId, outliers_pct, minMatches, anchored):
    yamlfile = open('search-configs.yaml', 'w')
    yamlfile.write('search_input_graph_path: ' + workload + '.unsafe.graph\n')
    yamlfile.write('search_output_path: output-search\n')
    yamlfile.write('search_injective: true\n')
    # yamlfile.write('arabesque.graph.edge_labelled: false\n')
    # yamlfile.write('search_multigraph: false\n')
    yamlfile.write('search_outliers_pct: ' + outliers_pct + '\n')
    yamlfile.write('search_outliers_min_matches: ' + minMatches + '\n')
    yamlfile.write('search_fastNeighbors: ' + fastNeighbors + '\n')
    yamlfile.write('search_write_in_binary: ' + outputBinary + '\n')

    if anchored:
        yamlfile.write('search_query_graph_path: Q' + str(queryId) + '-' + workload + '\n')
    else:
        yamlfile.write('search_query_graph_path: Q' + str(queryId) + 'u\n')

    if outputActive:
        yamlfile.write('search_output_active: true\n')
    else:
        yamlfile.write('search_output_active: false\n')

    if workload == 'citeseer':
        yamlfile.write('search_num_vertices: 3312\n')
        yamlfile.write('search_num_edges: 9072\n')
        yamlfile.write('search_num_labels: 6\n')

    if workload == 'mico':
        yamlfile.write('search_num_vertices: 100000\n')
        yamlfile.write('search_num_edges: 2160312\n')
        yamlfile.write('search_num_labels: 29\n')

    if workload == 'patent':
        yamlfile.write('search_num_vertices: 2745761\n')
        yamlfile.write('search_num_edges: 27930818\n')
        yamlfile.write('search_num_labels: 37\n')

    if workload == 'youtube':
        yamlfile.write('search_num_vertices: 4589876\n')
        yamlfile.write('search_num_edges: 87937596\n')
        yamlfile.write('search_num_labels: 108\n')

    if workload == 'star':
        yamlfile.write('search_num_vertices: 10000\n')
        yamlfile.write('search_num_edges: 39996\n')
        yamlfile.write('search_num_labels: 1\n')

    if workload == 'livej':
        yamlfile.write('search_num_vertices: 4846609\n')
        yamlfile.write('search_num_edges: 85702474\n')
        yamlfile.write('search_num_labels: 1\n')

    if workload == 'orkut':
        yamlfile.write('search_num_vertices: 3072441\n')
        yamlfile.write('search_num_edges: 234369798\n')
        yamlfile.write('search_num_labels: 1\n')

    yamlfile.write('log_level: fatal\n')

    yamlfile.close()

################ RUN EXPERIMENTS ################


def run (workload,queryId,outliers_pct,minMatches,anchored):
    query = 'Q' + str(queryId)
    if not anchored:
        query += 'u'
    print ("\n############## running workload " + workload + " on query " + query + " with outlier_pct " + outliers_pct + " ##############\n")
    subprocess.call(["hdfs", "dfs", "-rm", "output-search/*"])
    writeAppYaml(workload,queryId,outliers_pct,minMatches,anchored)
    print ('Arabesque is running workload ' + workload  + ' query ' + query + ' outliers_pct ' + outliers_pct + ' ...')
    p = subprocess.Popen(['./run_qfrag_spark.sh cluster-spark.yaml search-configs.yaml'], stderr=subprocess.PIPE, shell=True)
    outChars = p.stderr.read()
    outLines = charsToLines(outChars, '')
    for line in outLines:
        print line
    return outLines

def charsToLines(charsArray, startChar):
    lines = []
    currLine = ""
    copying = False
    for char in charsArray:
        if copying:
            if char == '\n':
                lines.append(currLine)
                currLine = ""
                copying = False
            else:
                currLine += char
        elif (startChar == '' or char == startChar) and not copying:
            copying = True
            currLine += char
    return lines

################ VERIFY RESULTS ################

def verifyAnchored(workload, queryId, outliers_pct):
    # NOTE for unanchored, it does not make sense to do this check as there could be different permutations
    query = 'Q' + str(queryId)
    searchOut = open(
        'output-search/search-' + workload + '-' + query + '-outliers_pct=' + str(outliers_pct) + '-numServers=' + str(
            numServers) \
        + '-partPerServer=' + str(numPartitionsPerServer) + '-outputActive=' + str(outputActive) + '.txt', 'r')
    numResults = sum(1 for line in searchOut)
    searchOut.close()
    expectedResults = results[workload][queryId - 1]
    if numResults != expectedResults:
        #        outcome = open ('failed-' + workload + '-' + query + '-' + outliers_pct + '.txt', 'w')
        #        outcome.write('Problem with workload ' + workload + ' and query Q' + str(queryId))
        #        outcome.write('Expected ' + str(expectedResults) + ' results and got ' + str(numResults))
        #        outcome.close()
        print ('Problem with workload ' + workload + ', query Q' + str(queryId) + ' and outliers_pct ' + str(outliers_pct))
        print ('Expected ' + str(expectedResults) + ' results and got ' + str(numResults))
        print ('Type Y to continue')
        choice = raw_input()
        if choice != 'Y':
            sys.exit()
        else:
            return False
    else:
        print ('Workload ' + workload + ' and query Q' + str(queryId) + ' give right number of results')
        return True


def verify(workload, queryId, outliers_pct, anchored):
    query = 'Q' + str(queryId)
    if not anchored:
        query += 'u'
    searchOut = open(
        'results/search-' + workload + '-' + query + '-outliers_pct=' + str(outliers_pct) + '-numServers=' + str(
            numServers) \
        + '-partPerServer=' + str(numPartitionsPerServer) + '-outputActive=' + str(outputActive) + '.txt', 'w')
    subprocess.call(["hdfs", "dfs", "-cat", "output-search/*"], stdout=searchOut)
    searchOut.close()

    searchOut = open(
        'results/search-' + workload + '-' + query + '-outliers_pct=' + str(outliers_pct) + '-numServers=' + str(
            numServers) \
        + '-partPerServer=' + str(numPartitionsPerServer) + '-outputActive=' + str(outputActive) + '.txt', 'r')
    search = set()
    for line in searchOut:
        words = line.split()
        words.sort(key=int)
        string = ""
        for word in words:
            string += word
            string += ' '
        search.add(string)
    searchOut.close()
    search = sorted(search)

    if anchored:
        truthOut = open('ground-truth/sorted-clean-' + workload + '-q' + str(queryId) + '.txt', 'r')
    else:
        truthOut = open('ground-truth/sorted-clean-' + workload + '-unanchored-q' + str(queryId) + '.txt', 'r')
    #    truth = set()
    #    for line in truthOut:
    #        words = line.split()
    #        words.sort(key=int)
    #        string = ""
    #        for word in words:
    #            string += word
    #            string += ' '
    #        truth.add(string)
    #    truthOut.close()
    #    truth = sorted(truth)

    for i in range(len(search)):
        truth = truthOut.readline()
        if truth == '':
            print ('Problem with workload ' + workload + ', query ' + query + ' and outliers_pct ' + str(outliers_pct))
            print ('Search produced a superset of the ground truth results')
            print ('Example of additional incorrect result ' + repr(search[i].strip(' \t\n\r')))
            print ('Type Y to continue')
            choice = raw_input()
            if choice == 'Y':
                return
            else:
                sys.exit()

        if search[i].strip(' \t\n\r') != truth.strip(' \t\n\r'):
            print ('Problem with workload ' + workload + ', query ' + query + ' and outliers_pct ' + str(outliers_pct))
            print ('Either search missed true result ' + repr(truth.strip(' \t\n\r')))
            print ('or it found false result ' + repr(search[i].strip(' \t\n\r')))
            print ('Type Y to continue')
            choice = raw_input()
            if choice == 'Y':
                return
            else:
                sys.exit()

    truth = truthOut.readline()
    if truth != '':
        print ('Problem with workload ' + workload + ', query ' + query + ' and outliers_pct ' + str(outliers_pct))
        print ('Search produced a subset of the ground truth results')
        print ('Example of missing true result ' + repr(truth))
        print ('Type Y to continue')
        choice = raw_input()
        if choice == 'Y':
            return
        else:
            sys.exit()

    print ('Workload ' + workload + ' and query ' + query + ' give correct canonical results! Checked ' + str(
        len(search)) + ' results')

################ MAIN ################

def main():
    for configuration in configurations:
        global workloads
        workloads = configuration[0]
        global queriesAnchored
        queriesAnchored = configuration[1]
        global queriesUnanchored
        queriesUnanchored = configuration[2]
        global outliers_pcts
        outliers_pcts = configuration[3]
        global numServers
        numServers = configuration[4]
        global numPartitionsPerServer
        numPartitionsPerServer = configuration[5]
        global outputActive
        outputActive = configuration[6]

        writeClusterYaml(numServers, numPartitionsPerServer, outputActive)

        for workload in workloads:
            for queryId in queriesAnchored:
                for outliers_pct in outliers_pcts:
                    if doRun:
                        run(workload, queryId, outliers_pct, minMatches, True)
                    if doVerify and outputActive and outputBinary == 'false':
                        print ('Running verification for workload ' + workload + ' query Q' + str(
                            queryId) + ' outliers_pct ' + outliers_pct)
                        #                    verifyAnchored(workload,i,outliers_pct)
                        verify(workload, queryId, outliers_pct, True)
                    else:
                        print ('Skipping verification for workload ' + workload + ' query Q' + str(
                            queryId) + ' outliers_pct ' + outliers_pct)

        for workload in workloads:
            for queryId in queriesUnanchored:
                for outliers_pct in outliers_pcts:
                    if doRun:
                        run(workload, queryId, outliers_pct, minMatches, False)
                    if doVerify and outputActive and outputBinary == 'false':
                        print ('Running verification for workload ' + workload + ' query Q' + str(
                            queryId) + 'u outliers_pct ' + outliers_pct)
                        if workload == 'citeseer':
                            verify('citeseer', queryId, outliers_pct, False)
                    else:
                        print ('Skipping verification for workload ' + workload + ' query Q' + str(
                            queryId) + 'u outliers_pct ' + outliers_pct)

main()
