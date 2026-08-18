<?php

ini_set("display_errors", "stderr");
require dirname(__DIR__) . "/vendor/autoload.php";

$consumer = new Spiral\RoadRunner\Jobs\Consumer();

while ($task = $consumer->waitTask()) {
    try {
        $h = $task->getHeader('test')[0] ?? 'undefined';
        if ("test2" !== $h) {
            throw new RuntimeException(sprintf(
                "Expected header '%s', got '%s'",
                "test2",
                $h
            ));
        }
        $task->ack();
    } catch (\Throwable $e) {
        $task->fail($e);
    }
}
