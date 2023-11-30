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
        $subject = $task->getHeader('x-nats-subject')[0] ?? 'undefined';
        if ('default-nats-message-subject-as-header.current-subject' !== $subject) {
            throw new RuntimeException('Subject was not found');
        }

        $task->complete();
    } catch (\Throwable $e) {
        $task->error((string)$e);
    }
}
