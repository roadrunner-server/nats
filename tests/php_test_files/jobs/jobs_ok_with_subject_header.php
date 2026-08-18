<?php

ini_set("display_errors", "stderr");
require dirname(__DIR__) . "/vendor/autoload.php";

$consumer = new Spiral\RoadRunner\Jobs\Consumer();

const EXPECTED_SUBJECT = 'default-nats-message-subject-as-header.current-subject';

while ($task = $consumer->waitTask()) {
    try {
        $subject = $task->getHeader('x-nats-subject')[0] ?? 'undefined';
        if (EXPECTED_SUBJECT !== $subject) {
            throw new RuntimeException(sprintf(
                "Expected subject '%s', got '%s'",
                EXPECTED_SUBJECT,
                $subject
            ));
        }

        $task->ack();
    } catch (\Throwable $e) {
        $task->fail($e);
    }
}
