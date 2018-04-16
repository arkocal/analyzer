open Defaults (* CircInterval needs initialized conf *)

let domains: (module Lattice.S) list = [
  (module IntDomain.Integers);
  (module IntDomain.Flattened);
  (module IntDomain.Lifted);
  (module IntDomain.Interval32);
  (module IntDomain.Booleans);
  (module IntDomain.CircInterval);
  (module IntDomain.Trier);
  (module IntDomain.Enums);
  (module IntDomain.IntDomTuple)
]

let testsuite =
  List.map (fun d ->
      let module D = (val d: Lattice.S) in
      let module DP = DomainProperties.All (D) in
      DP.tests)
    domains
  |> List.flatten

let () =
  QCheck_runner.run_tests_main ~argv:Sys.argv testsuite