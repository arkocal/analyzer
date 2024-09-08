type 'a t = Dmutex.t * ('a Stdlib.Lazy.t)

let make_lazy expr = (Dmutex.create (), lazy expr)

let from_fun f = (Dmutex.create (), Stdlib.Lazy.from_fun f)

let force (mtx, blk) = 
  Dmutex.lock mtx; 
  let value = Stdlib.Lazy.force blk in
  Dmutex.unlock mtx;
  value
