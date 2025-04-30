import argparse
import string
import time
import subprocess
import os

from blinker import Namespace
import restart
from prettytable import PrettyTable
import matplotlib.pyplot as plt
import numpy as np
import yaml


analyzer_path = os.path.dirname(__file__) + '/../../goblint'
restart_path = os.path.dirname(__file__) + "/restart.py"
svbench_path = os.path.dirname(__file__) + '/../../../sv-benchmarks'
bench_path = os.path.dirname(__file__) + '/../../../bench'


def test():
    x,y,z = np.genfromtxt(os.path.dirname(__file__) + '/data/test.txt', delimiter=',', unpack=True, dtype=None)
    x2,y2,z2 = np.genfromtxt(os.path.dirname(__file__) + '/data/test2.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_xticks(x, y)
    ax.bar(x-0.1, z, width=0.2, color='b', align='center')
    ax.bar(x2+0.1, z2, width=0.2, color='g', align='center')
    plt.savefig(os.path.dirname(__file__) + "/test.png")

def restart_svcomp_graph():
    return

# svcomp no-data-race
def autotune_timings(property):
    outfile = open(os.path.dirname(__file__) + '/data/autotune_' + property + '_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/" + property + ".prp" and "expected_verdict" in p:
                    args = argparse.Namespace(conf=[os.path.dirname(__file__) + '/start.json'],file=svbench_path + '/c/goblint-regression/' + spec['input_files'],spec=svbench_path + '/c/properties/' + property + '.prp',runtime=120,timeout=60,a_limit=2,verbose=False,autotune=True,architecture=None)
                    setattr(args,"witness.yaml.validate",None)
                    setattr(args,"witness.yaml.unassume",None)
                    start = time.perf_counter()
                    first, firstTime, output = restart.loop(args, timing=True)
                    end = time.perf_counter()

                    result = "empty"
                    if "SV-COMP result: unknown" in output.stdout:
                        result = "unknown"
                    if "SV-COMP result: true" in output.stdout:
                        result = "True"
                    if "SV-COMP result: false" in output.stdout:
                        result = "False"
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + ',' + str(first) + ',' + str(firstTime) + ',' + str(result) + ',' + str(p["expected_verdict"]) + '\n')
                    i += 1


def conflist_timings(property):
    outfile = open(os.path.dirname(__file__) + '/data/conflist_' + property + '_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/" + property + ".prp" and "expected_verdict" in p:
                    args = argparse.Namespace(conf=[os.path.dirname(__file__) + '/start.json',os.path.dirname(__file__) + '/../../conf/svcomp.json'],file=svbench_path + '/c/goblint-regression/' + spec['input_files'],spec=svbench_path + '/c/properties/' + property + '.prp',runtime=120,timeout=60,a_limit=2,verbose=False,autotune=False,architecture=None)
                    setattr(args,"witness.yaml.validate",None)
                    setattr(args,"witness.yaml.unassume",None)
                    start = time.perf_counter()
                    first, firstTime, output = restart.loop(args, timing=True)
                    end = time.perf_counter()

                    result = "empty"
                    if "SV-COMP result: unknown" in output.stdout:
                        result = "unknown"
                    if "SV-COMP result: true" in output.stdout:
                        result = "True"
                    if "SV-COMP result: false" in output.stdout:
                        result = "False"
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + ',' + str(first) + ',' + str(firstTime) + ',' + str(result) + ',' + str(p["expected_verdict"]) + '\n')
                    i += 1


def goblint_timings(property):
    outfile = open(os.path.dirname(__file__) + '/data/goblint_' + property + '_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/" + property + ".prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    output = subprocess.run([analyzer_path, '--conf', os.path.dirname(__file__) + '/../../conf/svcomp.json', '--set', 'restart.enabled', 'true', '--set', 'ana.specification', svbench_path + '/c/properties/no-data-race.prp', svbench_path + '/c/goblint-regression/' + spec['input_files']], capture_output=True, text=True)
                    end = time.perf_counter()
                    result = "empty"
                    if "SV-COMP result: unknown" in output.stdout:
                        result = "unknown"
                    if "SV-COMP result: true" in output.stdout:
                        result = "True"
                    if "SV-COMP result: false" in output.stdout:
                        result = "False"
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + ',' + str(result) + ',' + str(p["expected_verdict"]) + '\n')
                    i += 1


def analyses():
    files = os.listdir(bench_path + '/coreutils')
    anas = ['base", "mallocWrapper", "mutex", "mutexEvents", "access', "expRelation", "threadflag", "threadreturn",
            "escape", 'base", "mallocWrapper", "mutex", "mutexEvents", "access", "race', "mhp", "assert", "pthreadMutexType","var_eq","symb_locks","region",'thread","threadJoins", "threadid']
    for a in anas:
        i = 1
        outfile = open(os.path.dirname(__file__) + '/data/analysis_timing' + str(anas.index(a)) + '.txt', 'w')
        outfile.write(a + '\n')
        for f in files:
            if f.endswith('.c'):
                conf = open(os.path.dirname(__file__) + "/restart_timing.conf", "w")
                conf.write('{ "ana": { "activated": [ "' + a + '" ]} }')
                conf.close()
                start = time.perf_counter()
                subprocess.run([analyzer_path, '--conf', conf.name, bench_path + '/coreutils/' + f])
                end = time.perf_counter()
                outfile.write(str(i) + ',' + f + ',' + str(end - start) + '\n')
        outfile.close()

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
    parser.add_argument('--autotune_timings', action='store_true', help='test.')
    parser.add_argument('--conflist_timings', action='store_true', help='test.')
    parser.add_argument('--goblint_timings', action='store_true', help='test.')
    parser.add_argument('--test', action='store_true', help='test.')
    args = parser.parse_args()

    if args.analyses:
        analyses()
    if args.overhead:
        overhead()
    if args.test:
        test()
    if args.conflist_timings:
        #conflist_timings("no-data-race")
        conflist_timings("no-overflow")
        conflist_timings("unreach-call")
    if args.autotune_timings:
        autotune_timings("no-data-race")
        autotune_timings("no-overflow")
        autotune_timings("unreach-call")
    if args.goblint_timings:
        goblint_timings("no-data-race")
        goblint_timings("no-overflow")
        goblint_timings("unreach-call")

if __name__ == "__main__":
    main()

