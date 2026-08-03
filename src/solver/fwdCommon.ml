(** Update rules ("warrowing") for solver contributions, abstracted from the
    solver itself.

    Taken over from the [fwd_constrsys_new] branch, where this is the head of
    [src/solver/fwdCommon.ml]. Only the update-rule part is built here; the
    forward-solver machinery of that file (which needs [FwdGlobConstrSys]) is
    kept unbuilt under [src/solver/reference/fwdCommon.ml] for reference.

    The original [open Goblint_constraint.ConstrSys] is dropped: it is only
    needed by the parts that are not carried over. *)

module type WarrowConfig = sig
  val delay_default: int
  val gas_default: int
  val update_gas: int
end

(** Manage warrowing

      Widening will be delayed 'delay' times in each phase
      There will be at most 'gas' narrowing phases.
*)
module Warrow (L : Lattice.S) (Conf: WarrowConfig) = struct
  (** (value, delay, gas, narrowing_flag) 
        Narrowing flag denotes if the last update lead
        to a narrowing. This is required to maintain delay/gas values.
  *)
  type contribution = {
    value: L.t;
    delay: int;
    gas: int;
    update_gas: int;
    last_was_narrow: bool;
  }

  let default () = { value = L.bot (); delay = Conf.delay_default; gas = Conf.gas_default; update_gas = Conf.update_gas; last_was_narrow=false }

  let warrow contribution new_value =

    let narrow () =
      if contribution.last_was_narrow then
        { contribution with value = L.narrow contribution.value new_value }
      else (
        if contribution.gas > 0 then  
          { contribution with
            value = L.narrow contribution.value new_value;
            gas = contribution.gas - 1;
            last_was_narrow = true;
          }
        else contribution 
      )
    in

    let widen () =
      if contribution.last_was_narrow then
        { contribution with
          value = L.join contribution.value new_value;
          last_was_narrow = false;
          delay = Conf.delay_default
        }
      else if contribution.delay <= 0 then
        { contribution with value = L.widen contribution.value (L.join contribution.value new_value) }
      else
        { contribution with
          value = L.join contribution.value new_value;
          delay = contribution.delay - 1
        }
    in

    let current_value = contribution.value in
    if L.equal new_value current_value then contribution
    else if L.leq new_value current_value then narrow ()
    else if L.leq current_value new_value then widen ()
    else (
      if contribution.update_gas > 0 then { contribution with value = new_value; update_gas = contribution.update_gas - 1 }
      else widen () 
    )

end
