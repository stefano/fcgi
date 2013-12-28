(*
  "Hello World" FCGI application.
*)

open Core.Std
open Async.Std

let hello_world request env body =
  return (200, "Content-Type: text/plain\r\n\r\nHello, World!\n")

let never_returns =
  let conf = Fcgi.Configuration.create
    ~connection:(Fcgi.Configuration.TcpServer ("127.0.0.1", 4444))
    ()
  in
  Fcgi.start ~conf (Fcgi.don't_stream hello_world)
