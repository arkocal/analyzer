"""Run parallel solver with stealing and extract statistics"""
import subprocess
import sys
from collections import namedtuple

# Set line length to 120 for linter 
# pylint: disable=C0301

Result = namedtuple("Result", ["solver_time", "search_percentage", "thread_9_iterations",
                               "thread_8_iterations", "search_time", "thread_8_work_time",
                               "total_work_time", "thread_9_average_iteration_time",
                               "thread_8_average_iteration_time"])


def process_output(out, err):
    # Reading error as metadata and logs are printed to stderr
    err_str = err.decode("utf-8")
    error_lines = err_str.split("\n")

    # Remove lines before "[Info] Main done"
    error_lines = error_lines[error_lines.index("[Info] Main done") + 1:]

    def get_number(pattern):
        lines = [line for line in error_lines if pattern in line]
        if len(lines) >= 1:
            return float([line for line in error_lines if pattern in line][-1]
                         .split(":")[1].strip())
        else:
            return None

    solver_time = get_number("Solver time")
    search_percentage = get_number("Search percentage")
    thread_9_iterations = get_number("Thread 9 iterations")
    thread_8_iterations = get_number("Thread 8 iterations")

    search_time = solver_time * search_percentage
    thread_8_work_time = solver_time - search_time
    total_work_time = (solver_time + thread_8_work_time) if thread_8_iterations else solver_time
    thread_9_average_iteration_time = solver_time / thread_9_iterations * 1e6
    if thread_8_iterations:
        thread_8_average_iteration_time = thread_8_work_time / thread_8_iterations * 1e6
    else:
        thread_8_average_iteration_time = None

    return Result(solver_time, search_percentage, thread_9_iterations, thread_8_iterations,
                  search_time, thread_8_work_time, total_work_time, thread_9_average_iteration_time,
                  thread_8_average_iteration_time)


def run_goblint(nr_threads, input_file):
    command = "./goblint --conf myconf.json --set solvers.td3.parallel_domains"
    process = subprocess.Popen(command.split() + [str(nr_threads), input_file],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    return process_output(*process.communicate())

test_file_path = sys.argv[1]
single_thread_result = run_goblint(1, test_file_path)
double_thread_result = run_goblint(2, test_file_path)


def format_print(title, nr1, nr2):
    print(title, end="")
    difference_percent = (nr2 - nr1) / nr1 * 100
    print(f"\t[{difference_percent:+.2f}%; {nr2-nr1:+.2e}]", end="\t")
    print(f"{nr1:.2f} -> {nr2:.2f}")


format_print("Solver time      ", single_thread_result.solver_time,
             double_thread_result.solver_time)
format_print("Thread 9 iterations", single_thread_result.thread_9_iterations,
             double_thread_result.thread_9_iterations)
format_print("Thread 9 avg iter us", single_thread_result.thread_9_average_iteration_time,
             double_thread_result.thread_9_average_iteration_time)
print("Search percentage\t", double_thread_result.search_percentage, sep="")
print(f"Thread 8 iterations \t{double_thread_result.thread_8_iterations:.2f}")
print(f"Thread 8 avg iter us\t{double_thread_result.thread_8_average_iteration_time:.2f}")
format_print("Actual work     ", single_thread_result.total_work_time,
             double_thread_result.total_work_time)
format_print("Total iterations", single_thread_result.thread_9_iterations,
             double_thread_result.thread_9_iterations + double_thread_result.thread_8_iterations)










