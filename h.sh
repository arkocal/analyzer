#!/bin/zsh

BASEPROMPT='$(git_custom_status)%{$fg[cyan]%}[%~% ]%{$reset_color%}%B$%b '
NR_THREADS=1
TESTFILE="coreutils/dd_comb.c"

unalias t
unalias c

t() {
  export NR_THREADS=$1
  export RPROMPT="%{$fg[cyan]%}Threads: $NR_THREADS; Testfile: $TESTFILE%{$reset_color%}"
}

i() {
  export TESTFILE=$1
  export RPROMPT="%{$fg[cyan]%}Threads: $NR_THREADS; Testfile: $TESTFILE%{$reset_color%}"
}

e() {
  ./goblint --conf myconf.json --set solvers.td3.parallel_domains $NR_THREADS $TESTFILE > /dev/null
}

b() {
  hyperfine -r ${1:-1} "./goblint --conf myconf.json --set solvers.td3.parallel_domains $NR_THREADS $TESTFILE > /dev/null"
}

c() {
  hyperfine -r ${1:-1} "./goblint --conf myconf.json --set solvers.td3.parallel_domains 1 $TESTFILE > /dev/null" "./goblint --conf myconf.json --set solvers.td3.parallel_domains 2 $TESTFILE > /dev/null"
}

unalias r
r() {
  hyperfine -r ${1:-1} "./goblint --conf myconf.json --set solvers.td3.parallel_domains 2 $TESTFILE > /dev/null" "./goblint --conf myconf.json --set solvers.td3.parallel_domains 1 $TESTFILE > /dev/null"
}
