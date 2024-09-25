#!/bin/zsh

BASEPROMPT='$(git_custom_status)%{$fg[cyan]%}[%~% ]%{$reset_color%}%B$%b '
NR_THREADS=1
TESTFILE="coreutils/dd_comb.c"

unalias t

t() {
  export NR_THREADS=$1
  export RPROMPT="Threads: $NR_THREADS; Testfile: $TESTFILE"
}

i() {
  export TESTFILE=$1
  export RPROMPT="Threads: $NR_THREADS; Testfile: $TESTFILE"
}

e() {
  ./goblint --conf myconf.json --set solvers.td3.parallel_domains $NR_THREADS $TESTFILE > /dev/null
}

b() {
  hyperfine -r ${1:-1} "./goblint --conf myconf.json --set solvers.td3.parallel_domains $NR_THREADS $TESTFILE > /dev/null"
}
