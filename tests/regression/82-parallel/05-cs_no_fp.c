// PARAM: --set "ana.activated[+]" call_string --set ana.ctx_sens "['call_string']" --enable ana.int.interval_set --set solver td_parallel_dist
#include <pthread.h>

int f(int i)
{
  if (i == 0)
  {
    return 1;
  }
  if (i > 0)
  {
    return f(i - 1);
  }
  return 11;
}


int procedure(int num_iterat)
{
  int res1 = f(num_iterat);
  return res1;
}

void *t_sens(void *arg)
{
  procedure(0);
  return NULL;
}

void *t_sens2(void *arg)
{
  procedure(9);
  return NULL;
}

void *t_sens3(void *arg)
{
  procedure(12);
  return NULL;
}

int main()
{
  pthread_t id;
  pthread_t id2;
  pthread_t id3;

  pthread_create(&id, NULL, t_sens, NULL);
  pthread_create(&id2, NULL, t_sens2, NULL);
  pthread_create(&id3, NULL, t_sens3, NULL);
  return 0;
}
