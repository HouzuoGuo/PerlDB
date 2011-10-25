# This module provides some useful tools for file and text processing.
package Util;
use strict;
use warnings;
use diagnostics;
use Carp;
use English qw(-no_match_vars);
use List::MoreUtils qw{ any };

# Trim string into a desired length
sub trim {

    # Parameters: the string, the desired length
    my ( $string, $length ) = @_;
    my $length_diff = $length - length $string;
    if ( $length_diff > 0 ) {

        # Return string with stuff spaces if the length should be longer
        return $string . q{ } x $length_diff;
    } elsif ( $length_diff < 0 ) {

        # Return truncated string if the length should be shorter
        return substr $string, 0, $length;
    } else {
        return $string;
    }
}

# Remove leading and trailing spaces from a string and return the result
sub trimmed {

    # Parameter: the string
    my $string = shift;
    $string =~ s/^\s+|\s+$//gmsx;
    return $string;
}

# Convert hash into string
sub h2s {

    # Parameter: reference to the hash
    my $hash = shift;
    return qq/{${\(join ',', map "$_=>$hash->{$_}", keys %$hash)}}/;
}

# Remove lines from file, using a regex as filter
sub remove_by_regex {

    # Parameters: file name, regex
    my ( $filename, $regex ) = @_;
    open my $file_reader, '<', $filename
      or croak
      "(remove_by_regex) Unable to open file $filename for reading: $OS_ERROR";
    my @lines = grep { !/$regex/msx } <$file_reader>;
    close $file_reader
      or croak "(remove_by_regex) Unable to close file handler: $OS_ERROR";
    open my $file_writer, '>', $filename
      or croak "(remove_by_regex) Unable to truncate file $filename: $OS_ERROR";
    print {$file_writer} @lines
      or croak "(remove_by_regex) Unable to write to file $filename: $OS_ERROR";
    close $file_writer
      or croak "(remove_by_regex) Unable to close file handler: $OS_ERROR";
    return;
}

# Create a file with optional content
sub create_file {

    # Parameter: path, content
    my ( $path, $content ) = @_;
    open my $file, '>', $path
      or croak "(Util->create_empty_file) Cannot create file $path: $OS_ERROR";
    if ($content) {
        print $file $content;
    }
    close $file or carp "(Util->create_empty_file) Cannot close file $path";
    return;
}

# Return whether an element is in an array
sub in_array {

    # Parameters: element, array
    my ( $element, @array ) = @_;
    return any { $_ eq $element } @array;
}
1;
