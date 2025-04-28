import argparse
import string
import time
import subprocess
import os
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
def autotune_svcomp_datarace_timings():
    outfile = open(os.path.dirname(__file__) + '/data/Autotune_svcomp_datarace_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/no-data-race.prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    first, firstrunTime = restart.loop([os.path.dirname(__file__) + '/start.json'], svbench_path + '/c/goblint-regression/' + spec['input_files'], svbench_path + '/c/properties/no-data-race.prp', True, 120, 60, 2, False, timing=True)
                    end = time.perf_counter()
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + ',' + str(first) + ',' + str(firstrunTime) + '\n')
                    i += 1


def conflist_svcomp_datarace_timings():
    outfile = open(os.path.dirname(__file__) + '/data/conflist_svcomp_datarace_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/no-data-race.prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    first, firstrunTime = restart.loop([os.path.dirname(__file__) + '/start.json',os.path.dirname(__file__) + '/../../conf/svcomp.json'], svbench_path + '/c/goblint-regression/' + spec['input_files'], svbench_path + '/c/properties/no-data-race.prp', False, 120, 60, 2, False, timing=True)
                    end = time.perf_counter()
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + ',' + str(first) + ',' + str(firstrunTime) + '\n')
                    i += 1


def goblint_svcomp_datarace_timings():
    outfile = open(os.path.dirname(__file__) + '/data/goblint_svcomp_datarace_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/no-data-race.prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    subprocess.run([analyzer_path, '--conf', os.path.dirname(__file__) + '/../../conf/svcomp.json', '--set', 'restart.enabled', 'true', '--set', 'ana.specification', svbench_path + '/c/properties/no-data-race.prp', svbench_path + '/c/goblint-regression/' + spec['input_files']])
                    end = time.perf_counter()
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + '\n')
                    i += 1


# svcomp no-overflow
def autotune_svcomp_nooverflow_timings():
    outfile = open(os.path.dirname(__file__) + '/data/Autotune_svcomp_nooverflow_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/no-overflow.prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    first, firstrunTime = restart.loop([os.path.dirname(__file__) + '/start.json'], svbench_path + '/c/goblint-regression/' + spec['input_files'], svbench_path + '/c/properties/no-overflow.prp', True, 120, 60, 2, False, timing=True)
                    end = time.perf_counter()
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + ',' + str(first) + ',' + str(firstrunTime) + '\n')
                    i += 1


def conflist_svcomp_nooverflow_timings():
    outfile = open(os.path.dirname(__file__) + '/data/conflist_svcomp_nooverflow_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/no-overflow.prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    first, firstrunTime = restart.loop([os.path.dirname(__file__) + '/start.json',os.path.dirname(__file__) + '/../../conf/svcomp.json'], svbench_path + '/c/goblint-regression/' + spec['input_files'], svbench_path + '/c/properties/no-overflow.prp', False, 120, 60, 2, False, timing=True)
                    end = time.perf_counter()
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + ',' + str(first) + ',' + str(firstrunTime) + '\n')
                    i += 1


def goblint_svcomp_nooverflow_timings():
    outfile = open(os.path.dirname(__file__) + '/data/goblint_svcomp_nooverflow_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/no-overflow.prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    subprocess.run([analyzer_path, '--conf', os.path.dirname(__file__) + '/../../conf/svcomp.json', '--set', 'restart.enabled', 'true', '--set', 'ana.specification', svbench_path + '/c/properties/no-overflow.prp', svbench_path + '/c/goblint-regression/' + spec['input_files']])
                    end = time.perf_counter()
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + '\n')
                    i += 1

# svcomp unreach-call
def autotune_svcomp_unreachcall_timings():
    outfile = open(os.path.dirname(__file__) + '/data/Autotune_svcomp_unreachcall_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/unreach-call.prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    first, firstrunTime = restart.loop([os.path.dirname(__file__) + '/start.json'], svbench_path + '/c/goblint-regression/' + spec['input_files'], svbench_path + '/c/properties/unreach-call.prp', True, 120, 60, 2, False, timing=True)
                    end = time.perf_counter()
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + ',' + str(first) + ',' + str(firstrunTime) + '\n')
                    i += 1


def conflist_svcomp_unreachcall_timings():
    outfile = open(os.path.dirname(__file__) + '/data/conflist_svcomp_unreachcall_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/unreach-call.prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    first, firstrunTime = restart.loop([os.path.dirname(__file__) + '/start.json',os.path.dirname(__file__) + '/../../conf/svcomp.json'], svbench_path + '/c/goblint-regression/' + spec['input_files'], svbench_path + '/c/properties/unreach-call.prp', False, 120, 60, 2, False, timing=True)
                    end = time.perf_counter()
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + ',' + str(first) + ',' + str(firstrunTime) + '\n')
                    i += 1


def goblint_svcomp_unreachcall_timings():
    outfile = open(os.path.dirname(__file__) + '/data/goblint_svcomp_unreachcall_timings.txt', 'w')
    i = 1
    for file in os.listdir(svbench_path + '/c/goblint-regression'):
        if file.endswith('.yml'):
            spec = yaml.safe_load(open(svbench_path + '/c/goblint-regression/' + file, 'r'))
            for p in spec["properties"]:
                if p["property_file"] == "../properties/unreach-call.prp" and "expected_verdict" in p:
                    start = time.perf_counter()
                    subprocess.run([analyzer_path, '--conf', os.path.dirname(__file__) + '/../../conf/svcomp.json', '--set', 'restart.enabled', 'true', '--set', 'ana.specification', svbench_path + '/c/properties/unreach-call.prp', svbench_path + '/c/goblint-regression/' + spec['input_files']])
                    end = time.perf_counter()
                    outfile.write(str(i) + ',' + spec['input_files'] + ',' + str(end - start) + '\n')
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
    parser.add_argument('--autotune_svcomp_datarace_timings', action='store_true', help='test.')
    parser.add_argument('--conflist_svcomp_datarace_timings', action='store_true', help='test.')
    parser.add_argument('--goblint_svcomp_datarace_timings', action='store_true', help='test.')
    parser.add_argument('--autotune_svcomp_nooverflow_timings', action='store_true', help='test.')
    parser.add_argument('--conflist_svcomp_nooverflow_timings', action='store_true', help='test.')
    parser.add_argument('--goblint_svcomp_nooverflow_timings', action='store_true', help='test.')
    parser.add_argument('--autotune_svcomp_unreachcall_timings', action='store_true', help='test.')
    parser.add_argument('--conflist_svcomp_unreachcall_timings', action='store_true', help='test.')
    parser.add_argument('--goblint_svcomp_unreachcall_timings', action='store_true', help='test.')
    parser.add_argument('--test', action='store_true', help='test.')
    args = parser.parse_args()

    if args.analyses:
        analyses()
    if args.overhead:
        overhead()
    if args.test:
        test()
    if args.autotune_svcomp_datarace_timings:
        autotune_svcomp_datarace_timings()
    if args.conflist_svcomp_datarace_timings:
        conflist_svcomp_datarace_timings()
    if args.goblint_svcomp_datarace_timings:
        goblint_svcomp_datarace_timings()
    if args.autotune_svcomp_nooverflow_timings:
        autotune_svcomp_nooverflow_timings()
    if args.conflist_svcomp_nooverflow_timings:
        conflist_svcomp_nooverflow_timings()
    if args.goblint_svcomp_nooverflow_timings:
        goblint_svcomp_nooverflow_timings()
    if args.autotune_svcomp_unreachcall_timings:
        autotune_svcomp_unreachcall_timings()
    if args.conflist_svcomp_unreachcall_timings:
        conflist_svcomp_unreachcall_timings()
    if args.goblint_svcomp_unreachcall_timings:
        goblint_svcomp_unreachcall_timings()

if __name__ == "__main__":
    main()

