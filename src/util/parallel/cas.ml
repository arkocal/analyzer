exception CasFailException

module CasWithStatAndException = struct
  let count_success = Atomic.make 0
  let count_failure = Atomic.make 0
  let cas key old new_ =
    if Atomic.compare_and_set key old new_ then
      Atomic.incr count_success
    else
      begin
        Atomic.incr count_failure;
        raise CasFailException
      end
end
