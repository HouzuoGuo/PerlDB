# Relational Algebras.
#
# Results are stored as: tables and their kept row numbers; kept columns and
# their tables.
#
# Example:
# Table t1 (columns A, c1) joins with t2 (columns A, c2) using column A.
# And as result of join, t1 rows 1, 3 correspond to t2 rows 4, 2.
# RA result structures:
# $self->{'tables'} = { 't1' => { 'ref' => ref_to_t1, 'row_numbers' => [1, 3] },
#                       't2' => { 'ref' => ref_to_t2, 'row_numbers' => [4, 2] }
#                     }
# $self->{'columns'} = { 'A' => { 'table' => 't1', 'name' => 'A' },
#                        'c1' => { 'table' => 't1', 'name' => 'c1' },
#                        'c2' => { 'table' => 't2', 'name' => 'c2' } }
# (The key 'A' 'c1' 'c2' are in fact alias of the columns)
package RA;
use strict;
use warnings;
use diagnostics;
use Carp;
use English qw(-no_match_vars);
use Util;
use List::MoreUtils qw{ any };
INITIALIZER: {

    # Constructor
    sub new {
        my $type = shift;
        my $self = { 'tables' => undef, 'columns' => undef };
        bless $self, $type;
        return $self;
    }
}
RELATIONAL_ALGEBRA_FUNCTIONS: {

    # Relational algebra Select
    sub select {

        # Parameters: self, column alias, filter, parameter to filter
        my ( $self, $alias, $filter_function, $parameter ) = @_;
        my $table_name  = $self->{'columns'}->{$alias}->{'table'};
        my $column_name = $self->{'columns'}->{$alias}->{'name'};
        my $table_ref   = $self->{'tables'}->{$table_name}->{'ref'};
        my $row_numbers = $self->{'tables'}->{$table_name}->{'row_numbers'};
        my @kept        = ();

        # For all the rows
        for ( my $i = 0 ; $i < scalar @{$row_numbers} ; ++$i ) {

            # Use the filter function to pick out kept row numbers
            if (
                 $filter_function->(
                                  Util::trimmed(
                                      $table_ref->read_row( $row_numbers->[$i] )
                                        ->{$column_name}
                                  ),
                                  Util::trimmed($parameter)
                 )
              )
            {
                push @kept, $i;
            }
        }
        while ( my ( $a_name, $a_ref ) = each %{ $self->{'tables'} } ) {

            # Each table now only keeps the kept row numbers
            @{ $a_ref->{'row_numbers'} } = @{ $a_ref->{'row_numbers'} }[@kept];
        }
        return $self;
    }

    # Relational algebra Project
    sub project {

        # Parameters: self, column aliases
        my ( $self, @column_aliases ) = @_;

        # For all the aliases we have
        foreach my $alias ( keys %{ $self->{'columns'} } ) {

            # If it is not one we want to keep
            if ( not any { $_ eq $alias } @column_aliases ) {

                # Find out its associated table name
                my $table_name = $self->{'columns'}->{$alias}->{'table'};
                delete $self->{'columns'}->{$alias};

                # Does the associated table associate with any other column?
                my @other_associations =
                  grep { $self->{'columns'}->{$_}->{'table'} eq $table_name }
                  keys %{ $self->{'columns'} };

                # If it does not associate with any other column
                if ( scalar @other_associations == 0 ) {
                    delete $self->{'tables'}->{$table_name};
                }
            }
        }
        return $self;
    }
}
OTHER_FUNCTIONS: {

    # Prepare a table for being used by RA
    # (A table reference may not be used by any relational algebra function)
    sub prepare_table {

        # Parameters: self, reference to the table to be read
        my ( $self, $table_ref ) = @_;
        my $table_name = $table_ref->{'name'};

        # Load all row numbers
        $self->{'tables'}->{$table_name} = {
                        'ref'         => $table_ref,
                        'row_numbers' => [ 0 .. $table_ref->number_of_rows - 1 ]
        };

        # Load all columns
        foreach ( keys %{ $table_ref->{'columns'} } ) {
            $self->{'columns'}->{$_} = { 'table' => $table_name, 'name' => $_ };
        }
        return $self;
    }
}
1;
