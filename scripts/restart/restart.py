import argparse
import subprocess
import os
import signal

def parser():
    parser = argparse.ArgumentParser(description='Run goblint multiple times with different configurations until timeout.')
    parser.add_argument('-v', '--verbose', action='store_true', help='generate verbose output.')
    parser.add_argument('-t', '--timeout', type=int, default=60, help='Time until analysis timeout in seconds.')
    parser.add_argument('--runtime', type=int, default=900, help='Time limit for restart script in seconds.')
    parser.add_argument('-a', '--autotune', action='store_true', help='Auto adjust first config after every restart for LIMIT runs or RUNTIME seconds.')
    parser.add_argument('--a_limit', type=int, metavar='LIMIT', help='limit how many iterations autotune is used for.')
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


def main():
    args = parser()

    restart = "true"
    last_output = "empty"
    runcount = args.a_limit if args.autotune else len(args.conf)
    error_message = "Error" if args.spec == None else "SV-COMP result: unknown"
    analyzer_path = os.path.dirname(__file__) + '/../../goblint'
    autotune_args = ["--set", "ana.autotune.enabled", "true"]

    # total allowed runtime, print last results if runtime is reached
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(args.runtime)

    # start of restart loop
    try:
        for i in range(runcount):
            # set loop args
            c = args.conf[i]
            if i >= runcount - 1:
                restart = "false"
            gob_args = [analyzer_path, '--set', 'restart.enabled', restart, '--set', 'restart.timeout', str(args.timeout), '--conf', c, args.file]
            if args.verbose: gob_args.append("-v")
            if args.autotune: gob_args.extend(["--set", "restart.autotune", "true"])
            if args.spec != None: gob_args.extend(["--set", "ana.specification", args.spec])

            # call goblint
            print("Current configuration: " + c)
            result = subprocess.run(gob_args, capture_output=True, text=True)

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


if __name__ == "__main__":
    main()
