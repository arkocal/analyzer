import argparse
import time
import subprocess
import os
from prettytable import PrettyTable

analyzer_path = os.path.dirname(__file__) + '/../../goblint'

def analyses():
    files = ['tests/incremental/00-basic/00-local.c']
    anas = ["expRelation", "base", "threadid", "threadflag", "threadreturn", "escape", "mutexEvents", "mutex", "access", "race", "mallocWrapper", "mhp", "assert", "pthreadMutexType"]
    for f in files:
        for a in anas:
            conf = open(os.path.dirname(__file__) + "/restart_timing.conf", "w")
            conf.write('{ "ana": { "activated": [ "' + a + '" ]} }')
            conf.close()
            start = time.perf_counter()
            subprocess.run([analyzer_path, '--conf', conf.name, a, f])
            end = time.perf_counter()
            print(end - start)

def overhead():
    file = "../bench/coreutils/cksum_comb.c"
    conf = "conf/examples/large-program.json"
    total_runs = 10

    runs = [""]
    for i in range(1,11):
        runs.append("run" + str(i))
    runs.append("avg")
    t = PrettyTable(runs)

    total_time = 0
    row = ["Single run without restart"]
    for i in range(total_runs):
        start = time.perf_counter()
        subprocess.run([analyzer_path, '--conf', conf, file], capture_output=True)
        end = time.perf_counter()
        row.append(end - start)
        total_time += end - start
    avg = total_time/total_runs
    row.append(avg)
    print("Single run average time without restart: " + str(avg))
    t.add_row(row)

    total_time = 0
    row = ["Single run with restart"]
    for i in range(total_runs):
        restart_path = os.path.dirname(__file__) + "/restart.py"
        start = time.perf_counter()
        subprocess.run(['python3', restart_path, '--conf', conf, '-f', file], capture_output=True)
        end = time.perf_counter()
        row.append(end - start)
        total_time += end - start
    avg = total_time/total_runs
    row.append(avg)
    print("Single run average time with restart: " + str(total_time/10))
    t.add_row(row)

    # This runs three times because large-program.conf generates an error
    total_time = 0
    row = ["Three runs with restart"]
    for i in range(total_runs):
        restart_path = os.path.dirname(__file__) + "/restart.py"
        start = time.perf_counter()
        subprocess.run(['python3', restart_path, '--conf', conf, conf, conf, '-f', file], capture_output=True)
        end = time.perf_counter()
        row.append(end - start)
        total_time += end - start
    avg = total_time/total_runs
    row.append(avg)
    print("Three runs average time with restart: " + str(total_time/10))
    t.add_row(row)

    outfile = open(os.path.dirname(__file__) + "/overhead_timings", "w")
    outfile.write(t.get_string())
    outfile.close()


def main ():
    parser = argparse.ArgumentParser(description='Time goblint restart functionality.')
    parser.add_argument('--analyses', action='store_true', help='Measure single analysis runtimes.')
    parser.add_argument('--overhead', action='store_true', help='Measure overhead time of restart functionality.')
    args = parser.parse_args()

    if args.analyses:
        analyses()
    if args.overhead:
        overhead()

if __name__ == "__main__":
    main()

