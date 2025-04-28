import argparse
import subprocess
import os
import signal
import time


analyzer_path = os.path.dirname(__file__) + '/../../goblint'

def parser():
    parser = argparse.ArgumentParser(description='Run goblint multiple times with different configurations until timeout.')
    parser.add_argument('-v', '--verbose', action='store_true', help='generate verbose output.')
    parser.add_argument('-t', '--timeout', type=int, default=120, help='Time until analysis timeout in seconds.')
    parser.add_argument('--runtime', type=int, default=898, help='Time limit for restart script in seconds.')
    parser.add_argument('-a', '--autotune', action='store_true', help='Auto adjust first config after every restart for LIMIT runs or RUNTIME seconds.')
    parser.add_argument('--a_limit', type=int, metavar='LIMIT', default=2, help='limit how many iterations autotune is used for.')
    parser.add_argument('--spec', metavar='property.prp', help='Path or string for a specification for SV-COMP.')
    parser.add_argument('-c','--conf', nargs='+', help='configurations to be used.')
    parser.add_argument('-f','--file', required=True, help='file to be analyzed.')

    args = parser.parse_args()
    assert args.timeout > 0, "Timeout must be a positive integer."
    print(args)
    return args


def handler(signum, frame):
    print("Restart time limit reached!")
    raise Exception("timeout")

def loop(conf, file, spec, autotune, runtime, timeout, runcount, verbose, timing=False):
    firstrun = True
    firstrunTime = 0.0
    error_message = "Error" if spec == None else "SV-COMP result: unknown"
    restart = "true"
    last_output = "empty"

    # total allowed runtime, print last results if runtime is reached
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(runtime)

    # start of restart loop
    try:
        for i in range(runcount):
            # set loop args
            if autotune and i > 0:
                c = "scripts/restart/autotune.conf"
            else:
                c = conf[i]

            if i >= runcount - 1:
                restart = "false"
            if i > 0:
                firstrun = False

            gob_args = [analyzer_path, '--set', 'restart.enabled', restart, '--set', 'restart.timeout', str(timeout), '--conf', c, file]
            if verbose: gob_args.append("-v")
            if autotune and i > 0: gob_args.extend(["--set", "restart.autotune", "true"])
            if spec != None: gob_args.extend(["--set", "ana.specification", spec])

            # call goblint
            print("Current configuration: " + c)
            if firstrun and timing:
                start = time.perf_counter()
            result = subprocess.run(gob_args, capture_output=True, text=True)
            if firstrun and timing:
                end = time.perf_counter()
                firstrunTime = end - start

            # check and print output
            if (error_message not in result.stdout):
                print(result.stdout)
                break
            else:
                last_output = result.stdout
                print("Error: Restarting")
        else:
            print(result.stdout)
    except:
        print(last_output)

    return firstrun, (firstrunTime)

def main():
    args = parser()

    runcount = args.a_limit if args.autotune else len(args.conf)

    loop(args.conf, args.file, args.spec, args.autotune, args.runtime, args.timeout, runcount, args.verbose)


if __name__ == "__main__":
    main()
