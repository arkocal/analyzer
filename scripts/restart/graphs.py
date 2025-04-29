import matplotlib.pyplot as plt
import numpy as np
import yaml
import os

# no-data-race graphs
def average_datarace():
    x1,y1,z1, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/Autotune_svcomp_datarace_timings.txt', delimiter=',', unpack=True, dtype=None)
    x2,y2,z2, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/conflist_svcomp_datarace_timings.txt', delimiter=',', unpack=True, dtype=None)
    x3,y3,z3 = np.genfromtxt(os.path.dirname(__file__) + '/data/goblint_svcomp_datarace_timings.txt', delimiter=',', unpack=True, dtype=None)

    ax = plt.subplot(111)
    ax.set_title('Average runtime for SV-COMP no-data-race')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Used configuration')
    ax.bar('Autotune', np.average(z1), color='b', align='center')
    ax.bar('Conflist', np.average(z2), color='r', align='center')
    ax.bar('Goblint', np.average(z3), color='g', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/average_datarace.png")
    

def autotune_datarace():
    x1,y1,z1, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/Autotune_svcomp_datarace_timings.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_title('Measured runtime for autotune on SV-COMP no-data-race')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Analyzed file')
    ax.set_ylim([0,5])
    ax.bar(x1, z1, color='b', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/autotune_datarace.png")


def conflist_datarace():
    x1,y1,z1, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/conflist_svcomp_datarace_timings.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_title('Measured runtime for conflist on SV-COMP no-data-race')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Analyzed file')
    ax.set_ylim([0,5])
    ax.bar(x1, z1, color='r', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/conflist_datarace.png")
    

def goblint_datarace():
    x1,y1,z1 = np.genfromtxt(os.path.dirname(__file__) + '/data/goblint_svcomp_datarace_timings.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_title('Measured runtime for goblint on SV-COMP no-data-race')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Analyzed file')
    ax.set_ylim([0,5])
    ax.bar(x1, z1, color='g', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/goblint_datarace.png")


# no-overflow graphs
def average_nooverflow():
    x1,y1,z1, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/Autotune_svcomp_nooverflow_timings.txt', delimiter=',', unpack=True, dtype=None)
    x2,y2,z2, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/conflist_svcomp_nooverflow_timings.txt', delimiter=',', unpack=True, dtype=None)
    x3,y3,z3 = np.genfromtxt(os.path.dirname(__file__) + '/data/goblint_svcomp_nooverflow_timings.txt', delimiter=',', unpack=True, dtype=None)

    ax = plt.subplot(111)
    ax.set_title('Average runtime for SV-COMP no-overflow')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Used configuration')
    ax.bar('Autotune', np.average(z1), color='b', align='center')
    ax.bar('Conflist', np.average(z2), color='r', align='center')
    ax.bar('Goblint', np.average(z3), color='g', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/average_nooverflow.png")
    

def autotune_nooverflow():
    x1,y1,z1, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/Autotune_svcomp_nooverflow_timings.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_title('Measured runtime for autotune on SV-COMP no-overflow')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Analyzed file')
    ax.set_ylim([0,5])
    ax.bar(x1, z1, color='b', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/autotune_nooverflow.png")


def conflist_nooverflow():
    x1,y1,z1, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/conflist_svcomp_nooverflow_timings.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_title('Measured runtime for conflist on SV-COMP no-overflow')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Analyzed file')
    ax.set_ylim([0,5])
    ax.bar(x1, z1, color='r', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/conflist_nooverflow.png")
    

def goblint_nooverflow():
    x1,y1,z1 = np.genfromtxt(os.path.dirname(__file__) + '/data/goblint_svcomp_nooverflow_timings.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_title('Measured runtime for goblint on SV-COMP no-overflow')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Analyzed file')
    ax.set_ylim([0,5])
    ax.bar(x1, z1, color='g', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/goblint_nooverflow.png")

#  unreach-call graphs
def average_unreachcall():
    x1,y1,z1, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/Autotune_svcomp_unreachcall_timings.txt', delimiter=',', unpack=True, dtype=None)
    x2,y2,z2, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/conflist_svcomp_unreachcall_timings.txt', delimiter=',', unpack=True, dtype=None)
    x3,y3,z3 = np.genfromtxt(os.path.dirname(__file__) + '/data/goblint_svcomp_unreachcall_timings.txt', delimiter=',', unpack=True, dtype=None)

    ax = plt.subplot(111)
    ax.set_title('Average runtime for SV-COMP unreach-call')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Used configuration')
    ax.bar('Autotune', np.average(z1), color='b', align='center')
    ax.bar('Conflist', np.average(z2), color='r', align='center')
    ax.bar('Goblint', np.average(z3), color='g', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/average_unreachcall.png")
    

def autotune_unreachcall():
    x1,y1,z1, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/Autotune_svcomp_unreachcall_timings.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_title('Measured runtime for autotune on SV-COMP unreach-call')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Analyzed file')
    ax.set_ylim([0,5])
    ax.bar(x1, z1, color='b', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/autotune_unreachcall.png")


def conflist_unreachcall():
    x1,y1,z1, _, _ = np.genfromtxt(os.path.dirname(__file__) + '/data/conflist_svcomp_unreachcall_timings.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_title('Measured runtime for conflist on SV-COMP unreach-call')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Analyzed file')
    ax.set_ylim([0,5])
    ax.bar(x1, z1, color='r', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/conflist_unreachcall.png")
    

def goblint_unreachcall():
    x1,y1,z1 = np.genfromtxt(os.path.dirname(__file__) + '/data/goblint_svcomp_unreachcall_timings.txt', delimiter=',', unpack=True, dtype=None)
    ax = plt.subplot(111)
    ax.set_title('Measured runtime for goblint on SV-COMP unreach-call')
    ax.set_ylabel('Runtime in seconds')
    ax.set_xlabel('Analyzed file')
    ax.set_ylim([0,5])
    ax.bar(x1, z1, color='g', align='center')
    plt.savefig(os.path.dirname(__file__) + "/graphs/goblint_unreachcall.png")


def main():
    #autotune_datarace()
    #conflist_datarace()
    #goblint_datarace()
    #average_datarace()

    #autotune_nooverflow()
    #conflist_nooverflow()
    #goblint_nooverflow()
    #average_nooverflow()

    #autotune_unreachcall()
    #conflist_unreachcall()
    #goblint_unreachcall()
    #average_unreachcall()
    return

if __name__ == "__main__":
    main()