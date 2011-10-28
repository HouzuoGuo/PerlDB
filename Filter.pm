# Some useful filter functions, to be used by some functions in RA module.
package Filter;
use strict;
use warnings;
use diagnostics;
use Carp;
use English qw(-no_match_vars);
use Util;

# Test if two trimmed string scalars are equal
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
1;
