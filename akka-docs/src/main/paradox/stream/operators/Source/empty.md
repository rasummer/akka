# empty

Complete right away without ever emitting any elements.

@ref[Source stages](../index.md#source-stages)

@@@div { .group-scala }

## Signature

@@signature [Source.scala]($akka$/akka-stream/src/main/scala/akka/stream/scaladsl/Source.scala) { #empty }

@@@

## Description

Complete right away without ever emitting any elements. Useful when you have to provide a source to
an API but there are no elements to emit.


@@@div { .callout }

**emits** never

**completes** directly

@@@

