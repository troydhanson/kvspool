use KVSpool;
use Data::Dumper;
use Text::CSV;
use Text::CSV_XS;

#my $hh = KVSpool::read("/home/adamstr1/cic/spool",1);
my $v = KVSpool->new("/home/adamstr1/cic/spool");


my $csv = Text::CSV->new({always_quote => 1});

#open FILE, ">", "text.csv";
@cols = ('keyval');
sub hashtoaref {
    my $hash = shift;
    my @columns = shift;
    my @arry;
    foreach my $col (@columns) {
        push(@arry,$hash->{$col});
    }
    return \@arry;
}

#$status = $csv->print(\*FILE,$colref);
#print Dumper $v->read(1);
#print Dumper $v->read();
#print Dumper $v->stat();



my $h = {'keyval'=>'dude'};
print Dumper hashtoaref($h,@cols);
#$v->write($h);
print "foo";
