#!/usr/bin/perl
use strict;
use warnings;
use Data::Dumper;
use JSON;
use ZeroMQ qw/:all/;

my $ctx = ZeroMQ::Context->new;
my $sock = $ctx->socket(ZMQ_PULL);
$sock->connect("tcp://127.0.0.1:1234");

for(;;) {
  my $d = $sock->recv()->data();
  print Dumper from_json($d), "\n";
}
