#include <pthread.h>

void *t_fun(void *arg) {
  return NULL;
}

int main(void) {
  pthread_t id;
  int i = 0;
  pthread_create(&id, NULL, t_fun, (void *) &i);
  pthread_join(id, NULL);
  return 0;
}
