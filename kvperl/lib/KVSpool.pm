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
	KV_BASE_MAX
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
	KV_BASE_MAX
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
    my $base = shift;
    my $self = {
        'rsp' => 0,
        'wsp' => 0,
        'dir' => $dir,
        'base' => $base
    };
    bless $self,$class;
    return $self;
}
sub read {
    my $self = shift;
    my $block = shift;
    if($self->{'rsp'} == 0){
        $self->{'rsp'} = makersp($self->{'dir'},$self->{'base'});
    }
    return kvread($self->{'rsp'},$block);
}
sub write {
    my $self = shift;
    my $hash = shift;
    if($self->{'wsp'} == 0){
        $self->{'wsp'} = makewsp($self->{'dir'},$self->{'base'});
    }
    return kvwrite($self->{'wsp'},$hash);
}
sub stat {
    my $self = shift;
    return kv_stat($self->{'dir'},$self->{'base'});
}


sub DESTROY {
    my $self = shift;
    freersp($self->{'rsp'}) if($self->{'rsp'} != 0);
    freewsp($self->{'wsp'}) if($self->{'wsp'} != 0);
    #$self->{'rsp'} = 0;
    #$self->{'wsp'} = 0;
}
# Autoload methods go after =cut, and are processed by the autosplit program.

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

KVSpool - Perl extension for blah blah blah

=head1 SYNOPSIS

  use KVSpool;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for KVSpool, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.

=head2 Exportable constants

  KV_BASE_MAX



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

A. U. Thor, E<lt>adamstr1@E<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by A. U. Thor

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.1 or,
at your option, any later version of Perl 5 you may have available.


=cut
