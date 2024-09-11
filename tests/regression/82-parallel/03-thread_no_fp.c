//PARAM: --set solver td_parallel_base
#include <pthread.h>
#include <goblint.h>
#include <float.h>

int glob = 0;
pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

void *t_fun(void *arg) {
  pthread_mutex_lock(&mtx);
  glob = 999;
  pthread_mutex_unlock(&mtx);
  return NULL;
}

int main() {
  int i;
  pthread_t id;

  pthread_create(&id, NULL, t_fun, NULL);

  i = 9;
  
  i = 9;

  glob = 10;

  i = glob;

  return 0;
}
