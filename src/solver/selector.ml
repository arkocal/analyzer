(** Solver, which delegates at runtime to the configured solver. *)

open Batteries
open ConstrSys
open GobConfig

(* Registered solvers. *)
let solvers : (string * (module GenericCreatingEqIncrSolver)) list ref = ref []

(** Register your solvers here!!! *)
let add_solver (name, (module Slvr: GenericEqIncrSolver)) = 
  solvers := (name, (module (PostSolver.CreatingEqIncrSolverFromEqIncrSolver (Slvr : GenericEqIncrSolver)): GenericCreatingEqIncrSolver))::!solvers

(** Register your solvers for creating eq systems here!!! *)
let add_creating_eq_solver x = solvers := x::!solvers

(** Dynamically choose the solver. *)
let choose_solver solver =
  try List.assoc solver !solvers
  with Not_found ->
    raise @@ ConfigError ("Solver '"^solver^"' not found. Abort!")

(** The solver that actually uses the implementation based of [GobConfig.get_string "solver"]. *)
module Make =
  functor (Arg: IncrSolverArg) ->
  functor (S:CreatingEqConstrSys) ->
  functor (VH:Hashtbl.S with type key = S.v) ->
  struct
    type marshal = Obj.t (* cannot use Sol.marshal because cannot unpack first-class module in applicative functor *)

    let copy_marshal (marshal: marshal) =
      let module Sol = (val choose_solver (get_string "solver") : GenericCreatingEqIncrSolver) in
      let module F = Sol (Arg) (S) (VH) in
      Obj.repr (F.copy_marshal (Obj.obj marshal))

    let relift_marshal (marshal: marshal) =
      let module Sol = (val choose_solver (get_string "solver") : GenericCreatingEqIncrSolver) in
      let module F = Sol (Arg) (S) (VH) in
      Obj.repr (F.relift_marshal (Obj.obj marshal))

    let solve xs vs (old_data: marshal option) =
      let module Sol = (val choose_solver (get_string "solver") : GenericCreatingEqIncrSolver) in
      let module F = Sol (Arg) (S) (VH) in
      let (vh, marshal) = F.solve xs vs (Option.map Obj.obj old_data) in
      (vh, Obj.repr marshal)
  end

let _ =
  let module T1 : GenericCreatingEqIncrSolver = Make in
  ()