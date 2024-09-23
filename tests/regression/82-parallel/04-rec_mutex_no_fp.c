//PARAM: --set solver td_parallel_dist --set ana.activated[+] pthreadMutexType
#define _GNU_SOURCE
#include <pthread.h>

int g;

pthread_mutex_t m = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;


void *t_fun(void *arg) {

  pthread_mutex_lock(&m);
  g++; 
  pthread_mutex_unlock(&m);
  return NULL;
}

int main() {
  pthread_t id;
  pthread_create(&id, NULL, t_fun, NULL);

  pthread_mutex_lock(&m);
  g++; 
  pthread_mutex_unlock(&m);
  return 0;
}
