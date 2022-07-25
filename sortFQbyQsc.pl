#!/usr/bin/perl
# Script to sort sequences by avg Q-score
# Hidemasa Bono <bonohu@gmail.com> 

while(<STDIN>) { # first
     chomp;
    my $header = $_;
    my $qsum = 0;
    my $seq = <STDIN>; # second line is sequence
    my $dummy = <STDIN>; # third line is dummy
    my $qual = <STDIN>; # fourth line is quality
    chomp($seq);
    my @quals = split(//,$qual);
    my $len = @quals;
    for $n (@quals) {
        $qsum += ord($n) - 33; # decode Q-value 
    }
    my $qavg = $qsum / $len;
    print "$len\t$qavg\n";
}
