# Some useful filter functions, to be used by some functions in RA module.
package Filter;
use strict;
use warnings;
use diagnostics;
use Carp;
use English qw(-no_match_vars);
use Util;
use List::MoreUtils qw(any);

# Test if two trimmed strings are identical
sub equals {

    # Parameters: value1, value2
    my ( $value1, $value2 ) = @_;
    return Util::trimmed($value1) eq Util::trimmed($value2);
}

# Test if one scalar number is less than the another
sub less_than {

    # Parameters: value1, value2
    my ( $value1, $value2 ) = @_;
    return scalar Util::trimmed($value1) < scalar Util::trimmed($value2);
}

# Test if a trimmed string exists in a string array
# (not to be confused with Util::in_array)
sub any_of {

    # Parameters:, value, reference to array of values
    my ( $value, $values ) = @_;
    $value = Util::trimmed($value);
    return any { Util::trimmed($_) eq $value } @{$values};
}
1;
