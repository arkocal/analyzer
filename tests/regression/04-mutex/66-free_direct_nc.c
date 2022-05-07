// PARAM: --disable ana.race.free
#include <pthread.h>
#include <stdlib.h>

void *t_fun(void *arg) {
  int *p = (int *) arg;
  (*p)++; // NORACE
  return NULL;
}

int main(void) {
  pthread_t id;
  int *p = malloc(sizeof(int));
  pthread_create(&id, NULL, t_fun, (void *) p);
  free(p); // NORACE
  pthread_join (id, NULL);
  return 0;
}
