#!/usr/bin/perl
use strict;
use warnings;

`mkdir spool` unless -d "spool";

use KVSpool;
my $kv = KVSpool->new("spool");

my $h = {'day' => 'Wednesday', 'user' => 'Troy'};
$kv->write($h);

my $d = $kv->read();
print "$_: $d->{$_}\n" for keys %$d;

# test non blocking 
$kv->{blocking}=0;
$d = $kv->read();
if (not defined $d) { print "non blocking read: no data\n"; }
else { print "$_: $d->{$_}\n" for keys %$d; }

