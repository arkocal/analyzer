#include <goblint.h>

struct S1 {
  int a;
  int b;
};

struct S2 {
  int a;
  int b;
};

int main() {
	struct S1 s1a = {1, 2};
	struct S2 s1b = s1a;
	__goblint_check(s1b.a == 1);
}

