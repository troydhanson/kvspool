package KVSpool;

use 5.010001;
use strict;
use warnings;
use Carp;
use Data::Dumper;

require Exporter;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use KVSpool ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
);

our $VERSION = '0.01';

sub AUTOLOAD {
    # This AUTOLOAD is used to 'autoload' constants from the constant()
    # XS function.

    my $constname;
    our $AUTOLOAD;
    ($constname = $AUTOLOAD) =~ s/.*:://;
    croak "&KVSpool::constant not defined" if $constname eq 'constant';
    my ($error, $val) = constant($constname);
    if ($error) { croak $error; }
    {
	no strict 'refs';
	# Fixed between 5.005_53 and 5.005_61
#XXX	if ($] >= 5.00561) {
#XXX	    *$AUTOLOAD = sub () { $val };
#XXX	}
#XXX	else {
	    *$AUTOLOAD = sub { $val };
#XXX	}
    }
    goto &$AUTOLOAD;
}

require XSLoader;
XSLoader::load('KVSpool', $VERSION);

# Preloaded methods go here.

sub new {
    my $class = shift;
    my $dir = shift;
    my $self = {
        'rsp' => 0,
        'wsp' => 0,
        'set' => 0,
        'dir' => $dir,
        'blocking' => 1,
    };
    bless $self,$class;
    return $self;
}
sub read {
    my $self = shift;
    $self->{'rsp'} = makersp($self->{'dir'}) if($self->{'rsp'} == 0);
    $self->{'set'} = makeset() if($self->{'set'} == 0);
    return kvread($self->{'rsp'},$self->{'set'},$self->{'blocking'});
}
sub write {
    my $self = shift;
    my $hash = shift;
    $self->{'wsp'} = makewsp($self->{'dir'}) if($self->{'wsp'} == 0);
    $self->{'set'} = makeset() if($self->{'set'} == 0);
    return kvwrite($self->{'wsp'},$self->{'set'},$hash);
}
sub stat {
    my $self = shift;
    return kv_stat($self->{'dir'});
}


sub DESTROY {
    my $self = shift;
    freersp($self->{'rsp'}) if($self->{'rsp'} != 0);
    freewsp($self->{'wsp'}) if($self->{'wsp'} != 0);
    freeset($self->{'set'}) if($self->{'set'} != 0) ;
}
# Autoload methods go after =cut, and are processed by the autosplit program.

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

KVSpool - Perl extension for kvspool library

=head1 SYNOPSIS

  use KVSpool;
  my $v = KVSpool->new("spool");
  my $h = {'day' => 'Wednesday', 'user' => 'Troy'};
  $v->write($h);
  # or for a spool reader:
  $h = $v->read();

=head1 DESCRIPTION

This is the Perl binding for the kvspool library.

=head2 EXPORT

None by default.

=head1 SEE ALSO

See http://tkhanson.net/kvspool

=head1 AUTHOR

Trevor Adams
Troy D. Hanson

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2011 The Johns Hopkins University/Applied Physics Laboratory

This software was developed at The Johns Hopkins University/Applied Physics
Laboratory ("JHU/APL") that is the author thereof under the "work made for
hire" provisions of the copyright law. Permission is hereby granted, free of
charge, to any person obtaining a copy of this software and associated
documentation (the "Software"), to use the Software without restriction,
including without limitation the rights to copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to permit
others to do so, subject to the following conditions:

    1. This LICENSE AND DISCLAIMER, including the copyright notice, shall be
    included in all copies of the Software, including copies of substantial
    portions of the Software;

    2. JHU/APL assumes no obligation to provide support of any kind with regard
    to the Software. This includes no obligation to provide assistance in using
    the Software nor to provide updated versions of the Software; and

    3. THE SOFTWARE AND ITS DOCUMENTATION ARE PROVIDED AS IS AND WITHOUT ANY
    EXPRESS OR IMPLIED WARRANTIES WHATSOEVER. ALL WARRANTIES INCLUDING, BUT NOT
    LIMITED TO, PERFORMANCE, MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE,
    AND NONINFRINGEMENT ARE HEREBY DISCLAIMED. USERS ASSUME THE ENTIRE RISK AND
    LIABILITY OF USING THE SOFTWARE. USERS ARE ADVISED TO TEST THE SOFTWARE
    THOROUGHLY BEFORE RELYING ON IT. IN NO EVENT SHALL THE JOHNS HOPKINS
    UNIVERSITY BE LIABLE FOR ANY DAMAGES WHATSOEVER, INCLUDING, WITHOUT
    LIMITATION, ANY LOST PROFITS, LOST SAVINGS OR OTHER INCIDENTAL OR
    CONSEQUENTIAL DAMAGES, ARISING OUT OF THE USE OR INABILITY TO USE THE
    SOFTWARE.

=cut
