// just a few sanity checks on the asserts
#include<assert.h>

int main() {
  int success = 1;
  int silence = 1;
  int fail = 0;
  int unknown;
  // TODO: change back to assert?
  __goblint_check(success);
  __goblint_check(fail); // FAIL!
  __goblint_check(unknown == 4); // UNKNOWN!
  return 0;
  __goblint_check(silence); // NOWARN!
}
