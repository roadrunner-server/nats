<?php

/**
 * @var Goridge\RelayInterface $relay
 */

use Spiral\Goridge;
use Spiral\RoadRunner;
use Spiral\Goridge\StreamRelay;
use Spiral\RoadRunner\Jobs\Consumer;
use Spiral\RoadRunner\Jobs\Serializer\JsonSerializer;

ini_set('display_errors', 'stderr');
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
        $task->error((string)$e);
    }
}
