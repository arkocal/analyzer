import argparse
import subprocess
import os

def main():
    parser = argparse.ArgumentParser(description='Run goblint multiple times with different configs.')
    parser.add_argument('-v', '--verbose', action='store_true', help='generate verbose output')
    parser.add_argument('-t', '--timeout', type=int, default=60, help='Time until analysis timeout in seconds')
    parser.add_argument( '-c','--conf', nargs='+', help='config to be used')
    parser.add_argument( '-f','--file', help='file to be analyzed')

    args = parser.parse_args()
    analyzer_path = os.path.dirname(__file__) + '/../goblint'
    print(analyzer_path)
    print(args)

    assert args.timeout > 0, "Timeout must be a positive integer."
    restart = True
    # gob_args = [analyzer_path, '--set', 'restart.timeout', args.timeout, '-v', '--conf', c, args.file]
    for c in args.conf:
        index = args.conf.index(c)
        if len(args.conf) - index == 1:
            restart = False
        if args.verbose:
            result = subprocess.run([analyzer_path, '--set', 'restart.timeout', str(args.timeout), '-v', '--conf', c, args.file],
                                   capture_output=True, text=True)
        else:
            result = subprocess.run([analyzer_path, '--set', 'restart.timeout', str(args.timeout), '--conf', c, args.file],
                                    capture_output=True, text=True)
        print(result.stdout)


if __name__ == "__main__":
    main()
