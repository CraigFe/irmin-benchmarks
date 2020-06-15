(* Welford algorithm for online computation of mean and variance statistics. *)
module Moments : sig
  type t

  val empty : t

  val add : t -> float -> t

  type stats = {
    count : int;
    mean: float;
    variance: float;
  }

  val pp_stats : Format.formatter -> stats -> unit

  val finalize : t -> stats
end = struct
  type t = { count : int; mean : float; m2 : float }

  let empty = { count = 0; mean = 0.; m2 = 0. }

  let add { count; mean; m2 } v =
    let count = count + 1 in
    let delta = v -. mean in
    let mean = mean +. (delta /. float_of_int count) in
    let delta2 = v -. mean in
    let m2 = m2 +. (delta *. delta2) in
    { count; mean ; m2 }

  type stats = {
    count : int;
    mean: float;
    variance: float;
  }

  let pp_stats ppf { count; mean; variance } =
    Format.fprintf ppf "%d,%f,%f" count mean variance

  let finalize { count; mean; m2 } =
    if count < 2 then raise @@ Invalid_argument "Must contain at least 2 data points";
    { count = count; mean = mean; variance = m2 /. float_of_int count; }
end

include Moments
