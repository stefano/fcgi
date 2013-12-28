(*
  A FCGI server implementation.
  See http://www.fastcgi.com/drupal/node/6?q=node/22 for details on the protocol.
*)

open Core.Std
open Async.Std

module Logging : sig
  type t
  type level = Quiet | Verbose | Debug

  val create: ?level:level -> ?logging_function:(string -> unit) -> unit -> t
  val set_logger: t -> unit
end

module Configuration : sig
  type t
  type connection = Passive | TcpServer of string * int

  val create: ?max_connections:int -> connection:connection -> unit -> t
end

module Request : sig
  type t
end

val start : conf:Configuration.t -> (Request.t -> string String.Map.t -> int Deferred.t)
  -> never_returns
val read : Request.t -> string String.Map.t -> string option Deferred.t
val write : Request.t -> string -> unit Deferred.t
val on_abort: Request.t -> unit Deferred.t
val don't_stream: (Request.t -> string String.Map.t -> string -> (int * string) Deferred.t)
  -> (Request.t -> string String.Map.t -> int Deferred.t)
