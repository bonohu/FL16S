#!/usr/bin/perl
# Script to convert the base to N. 
# Hidemasa Bono <bonohu@gmail.com> 
die "usage: $0 threshold_quality_score (example: 20)\n" if(@ARGV != 1);
my $thre = shift(@ARGV); #quality value threshold for cutoff

while(<STDIN>) { # first
     chomp;
    my $header = $_;
    my $seq = <STDIN>; # second line is sequence
    my $dummy = <STDIN>; # third line is dummy
    my $qual = <STDIN>; # fourth line is quality
    chomp($seq);
    my @seqs = split(//,$seq);
    my @quals = split(//,$qual);
    print ">$header q_threshold:$thre\n";
    for $n (@seqs) {
        $q = ord(shift(@quals)) - 33; # decode Q-value 
        if( $q >= $thre) {
            print "$n"; 
        } else {
            print "N";
        }
    }
    print "\n";
}