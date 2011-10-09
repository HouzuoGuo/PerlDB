package Filter;
use strict;
use warnings;
use diagnostics;
use Carp;
use English qw(-no_match_vars);

# Test if two strings are equal
sub equals {

    # Parameters: value1, value2
    my ( $value1, $value2 ) = @_;
    return $value1 eq $value2;
}

# Test if one scalar number is less than the another
sub less_than {
    # Parameters: value1, value2
    my ($value1, $value2) = @_;
    return scalar $value1 < scalar $value2;
}
1;
