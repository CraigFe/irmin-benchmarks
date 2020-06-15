let ( ++ ) = Int64.add

module type S = sig
  type t

  val v : Unix.file_descr -> t

  val close : t -> unit

  val unsafe_write : t -> off:int64 -> string -> unit

  val unsafe_read : t -> off:int64 -> len:int -> bytes -> int
end

module Raw_lseek : S = struct
  type t = { fd : Unix.file_descr; mutable cursor : int64 }

  let v fd = { fd; cursor = 0L }

  let close { fd; _ } = Unix.close fd

  let really_write fd buf =
    let rec aux off len =
      let w = Unix.write fd buf off len in
      if w = 0 || w = len then () else (aux [@tailcall]) (off + w) (len - w)
    in
    (aux [@tailcall]) 0 (Bytes.length buf)

  let really_read fd len buf =
    let rec aux off len =
      let r = Unix.read fd buf off len in
      if r = 0 then off (* end of file *)
      else if r = len then off + r
      else (aux [@tailcall]) (off + r) (len - r)
    in
    (aux [@tailcall]) 0 len

  let lseek t off =
    if off = t.cursor then ()
    else
      let _ = Unix.LargeFile.lseek t.fd off Unix.SEEK_SET in
      t.cursor <- off

  let unsafe_write t ~off buf =
    lseek t off;
    let buf = Bytes.unsafe_of_string buf in
    really_write t.fd buf;
    t.cursor <- off ++ Int64.of_int (Bytes.length buf);
    ()

  let unsafe_read t ~off ~len buf =
    lseek t off;
    let n = really_read t.fd len buf in
    t.cursor <- off ++ Int64.of_int n;
    n
end

module Raw_positioned : S = struct
  type t = { fd : Unix.file_descr } [@@unboxed]

  let v fd = { fd }

  let close { fd } = Unix.close fd

  module Syscalls = Index_unix.Syscalls

  let really_write fd fd_offset buffer =
    let rec aux fd_offset buffer_offset length =
      let w = Syscalls.pwrite ~fd ~fd_offset ~buffer ~buffer_offset ~length in
      if w = 0 || w = length then ()
      else
        (aux [@tailcall])
          (fd_offset ++ Int64.of_int w)
          (buffer_offset + w) (length - w)
    in
    (aux [@tailcall]) fd_offset 0 (Bytes.length buffer)

  let really_read fd fd_offset length buffer =
    let rec aux fd_offset buffer_offset length =
      let r = Syscalls.pread ~fd ~fd_offset ~buffer ~buffer_offset ~length in
      if r = 0 then buffer_offset (* end of file *)
      else if r = length then buffer_offset + r
      else
        (aux [@tailcall])
          (fd_offset ++ Int64.of_int r)
          (buffer_offset + r) (length - r)
    in
    (aux [@tailcall]) fd_offset 0 length

  let unsafe_write t ~off buf =
    let buf = Bytes.unsafe_of_string buf in
    really_write t.fd off buf

  let unsafe_read t ~off ~len buf = really_read t.fd off len buf
end

let protect_unix_exn = function
  | Unix.Unix_error _ as e -> failwith (Printexc.to_string e)
  | e -> raise e

let ignore_enoent = function
  | Unix.Unix_error (Unix.ENOENT, _, _) -> ()
  | e -> raise e

let protect f x = try f x with e -> protect_unix_exn e

let safe f x = try f x with e -> ignore_enoent e

let mkdir dirname =
  let rec aux dir k =
    if Sys.file_exists dir && Sys.is_directory dir then k ()
    else (
      if Sys.file_exists dir then safe Unix.unlink dir;
      (aux [@tailcall]) (Filename.dirname dir) @@ fun () ->
      protect (Unix.mkdir dir) 0o755;
      k () )
  in
  aux dirname (fun () -> ())

let openfile file =
  let mode = Unix.O_RDWR in
  mkdir (Filename.dirname file);
  match Sys.file_exists file with
  | false -> Unix.openfile file Unix.[ O_CREAT; mode; O_CLOEXEC ] 0o644
  | true -> Unix.openfile file Unix.[ O_EXCL; mode; O_CLOEXEC ] 0o644

let nb_elements = 10_000_000

let element_size = 100

let random_char () = char_of_int (Random.int 256)

let random_string () = String.init element_size (fun _i -> random_char ())

let time_in_ns f =
  let clock = Mtime_clock.counter () in
  let _ = f () in
  Mtime_clock.count clock |> Mtime.Span.to_ns

module Bench (Raw : S) = struct
  let read t ~off buf = Raw.unsafe_read t ~off ~len:(Bytes.length buf) buf

  let () = Random.self_init ()

  let write get_off t =
    let rec aux stats i =
      if i = nb_elements then stats
      else
        let random = random_string () in
        let off = get_off i in
        let time = time_in_ns (fun () -> Raw.unsafe_write t ~off random) in
        aux (Moments.add stats time) (i + 1)
    in
    aux Moments.empty 0

  let read get_off t =
    let to_read = nb_elements in
    let rec aux stats n =
      if n = to_read then stats
      else
        let off = get_off n in
        let buf = Bytes.create element_size in
        let time = time_in_ns (fun () -> read t ~off buf) in
        aux (Moments.add stats time) (n + 1)
    in
    aux Moments.empty 0

  let random (_ : int) = Random.int nb_elements * element_size |> Int64.of_int

  let sequential i = i * element_size |> Int64.of_int

  let remove_pack () = try Sys.remove "store.pack" with Sys_error _ -> ()

  let with_raw f =
    let raw = Raw.v (openfile "store.pack") in
    let a = f raw in
    Raw.close raw;
    a

  let bench impl () =
    remove_pack ();

    with_raw (fun t ->
        let stats = write random t |> Moments.finalize in
        Format.printf "%s,random_writes,%a\n%!" impl Moments.pp_stats stats);

    remove_pack ();

    with_raw (fun t ->
        let stats = write sequential t |> Moments.finalize in
        Format.printf "%s,sequential_writes,%a\n%!" impl Moments.pp_stats stats);

    with_raw (fun t ->
        let stats = read random t |> Moments.finalize in
        Format.printf "%s,random_reads,%a\n%!" impl Moments.pp_stats stats);

    with_raw (fun t ->
        let stats = read sequential t |> Moments.finalize in
        Format.printf "%s,sequential_reads,%a\n%!" impl Moments.pp_stats stats)
end

module Raw_lseek_bench = Bench (Raw_lseek)
module Raw_positioned_bench = Bench (Raw_positioned)

let rec repeat n f =
  match n with
  | 0 -> ()
  | n ->
      f ();
      repeat (n - 1) f

let () =
  Format.printf "implementation,benchmark,count,mean(ns),variance(ns^2)\n";
  repeat 5 (fun () ->
      Raw_lseek_bench.bench "lseek" ();
      Raw_positioned_bench.bench "positioned" ())
