# concat

After completion of the original upstream the elements of the given source will be emitted.

@ref[Fan-in stages](../index.md#fan-in-stages)

@@@div { .group-scala }

## Signature

@@signature [Flow.scala]($akka$/akka-stream/src/main/scala/akka/stream/scaladsl/Flow.scala) { #concat }

@@@

## Description

After completion of the original upstream the elements of the given source will be emitted.


@@@div { .callout }

**emits** when the current stream has an element available; if the current input completes, it tries the next one

**backpressures** when downstream backpressures

**completes** when all upstreams complete

@@@

