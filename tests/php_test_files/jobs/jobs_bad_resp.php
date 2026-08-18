<?php

use Spiral\Goridge\StreamRelay;
use Spiral\RoadRunner;

ini_set("display_errors", "stderr");
require dirname(__DIR__) . "/vendor/autoload.php";

$rr = new RoadRunner\Worker(new StreamRelay(\STDIN, \STDOUT));

// answers with a payload the jobs response handler cannot parse
while ($in = $rr->waitPayload()) {
    $rr->respond(new RoadRunner\Payload("foo"));
}
