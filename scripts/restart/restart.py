import argparse
import subprocess
import os

def parser():
    parser = argparse.ArgumentParser(description='Run goblint multiple times with different configurations.')
    parser.add_argument('-v', '--verbose', action='store_true', help='generate verbose output')
    parser.add_argument('-t', '--timeout', type=int, default=60, help='Time until analysis timeout in seconds')
    parser.add_argument( '-c','--conf', nargs='+', help='configurations to be used')
    parser.add_argument( '-f','--file', required=True, help='file to be analyzed')

    args = parser.parse_args()
    assert args.timeout > 0, "Timeout must be a positive integer."
    print(args)
    return args
  

def main():
    args = parser()

    restart = "true"
    analyzer_path = os.path.dirname(__file__) + '/../../goblint'
    for c in args.conf:
        index = args.conf.index(c)
        if len(args.conf) - index == 1:
            restart = "false"
        gob_args = [analyzer_path, '--set', 'restart.enabled', restart, '--set', 'restart.timeout', str(args.timeout), '--conf', c, args.file]
        if args.verbose: gob_args.append("-v")

        print("Current configuration: " + c)
        result = subprocess.run(gob_args, capture_output=True, text=True)

        if ("Error" not in result.stdout):
            print(result.stderr)
            print(result.stdout)
            break
        else:
            print("Error: Restarting")
    else:
        print(result.stderr)
        print(result.stdout)


if __name__ == "__main__":
    main()
