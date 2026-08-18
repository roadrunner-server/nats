<?php

ini_set("display_errors", "stderr");
require dirname(__DIR__) . "/vendor/autoload.php";

$consumer = new Spiral\RoadRunner\Jobs\Consumer();

// nacks without asking for a redelivery, which terminates the message
while ($task = $consumer->waitTask()) {
    $task->nack("dropped");
}
