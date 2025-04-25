import argparse
import time
import subprocess
import os
from prettytable import PrettyTable
import matplotlib.pyplot as plt
import numpy as np

analyzer_path = os.path.dirname(__file__) + '/../../goblint'

def analyses():
    files = ['../bench/coreutils/cksum_comb.c']
    anas = ['"base", "mallocWrapper", "mutex", "mutexEvents", "access"', "expRelation", "threadid", "threadflag", "threadreturn",
            "escape", "race", "mhp", "assert", "pthreadMutexType","var_eq","symb_locks","region","thread","threadJoins"]
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
    total_runs = 100

    runs = [""]
    for i in range(1, total_runs + 1):
        runs.append("run" + str(i))
    runs.append("avg")
    runs.append("var")
    t = PrettyTable(runs)

    fig, axs = plt.subplots()
    box_data = []

    row = ["Single run without restart"]
    for i in range(total_runs):
        start = time.perf_counter()
        subprocess.run([analyzer_path, '--conf', conf, file], capture_output=True)
        end = time.perf_counter()
        row.append(end - start)
    box_data.append(row[1:])
    avg = np.average(row[1:])
    var = np.var(row[1:])
    row.append(avg)
    row.append(var)
    print("Single run average time without restart: " + str(avg))
    t.add_row(row)

    row = ["Single run with restart"]
    for i in range(total_runs):
        restart_path = os.path.dirname(__file__) + "/restart.py"
        start = time.perf_counter()
        subprocess.run(['python3', restart_path, '--conf', conf, '-f', file], capture_output=True)
        end = time.perf_counter()
        row.append(end - start)
    box_data.append(row[1:])
    avg = np.average(row[1:])
    var = np.var(row[1:])
    row.append(avg)
    row.append(var)
    print("Single run average time with restart: " + str(avg))
    t.add_row(row)

    axs.boxplot(box_data)
    axs.set_xticklabels( ('Without restart', 'With restart') )
    axs.set_ylabel('Runtime in seconds')
    axs.set_title('Single run analysis of cksum_comb.c')
    plt.savefig(os.path.dirname(__file__) + "/overhead_box.png")

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

