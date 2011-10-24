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

        # Name of the table of the column alias
        my $table_name = $self->{'columns'}->{$alias}->{'table'};

        # Column name of the column alias
        my $column_name = $self->{'columns'}->{$alias}->{'name'};

        # Table of the column alias
        my $table_ref = $self->{'tables'}->{$table_name}->{'ref'};

        # Selected row numbers of the table
        my $row_numbers = $self->{'tables'}->{$table_name}->{'row_numbers'};

        # Row numbers to keep (after select operation)
        my @kept = ();

        # For all the rows
        for ( 0 .. scalar @{$row_numbers} - 1 ) {
            my $row = $table_ref->read_row( $row_numbers->[$_] );

            # Use the filter function to pick out kept row numbers
            if (
                 not $row->{'~del'}
                 and $filter_function->(
                               Util::trimmed( $row->{$column_name} ), $parameter
                 )
              )
            {
                push @kept, $_;
            }
        }
        foreach ( values %{ $self->{'tables'} } ) {

            # Each table now only keeps the kept row numbers
            @{ $_->{'row_numbers'} } = @{ $_->{'row_numbers'} }[@kept];
        }
        return $self;
    }

    # Relational algebra Project
    sub project {

        # Parameters: self, column aliases
        my ( $self, @column_aliases ) = @_;

        # For all the aliases we have
        while ( my ( $alias, $column ) = each %{ $self->{'columns'} } ) {

            # If it is not one we want to keep
            if ( not any { $_ eq $alias } @column_aliases ) {

                # Find out its associated table name
                my $table_name = $column->{'table'};
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

    # Relational algebra Redefine
    sub redefine {

        # Parameters: self, old alias, new alias
        my ( $self, $old_alias, $new_alias ) = @_;
        if ( exists $self->{'columns'}->{$old_alias} ) {
            if ( exists $self->{'columns'}->{$new_alias} ) {
                croak "(RA->rename) the new alias $new_alias already exists";
            } else {
                $self->{'columns'}->{$new_alias} =
                  $self->{'columns'}->{$old_alias};
                delete $self->{'columns'}->{$old_alias};
            }
        } else {
            croak "(RA->rename) the old alias $old_alias does not exist";
        }
        return $self;
    }

    # Relational algebra Cross
    sub cross {

        # Parameters: self, reference to the table
        my ( $self, $table_ref ) = @_;

        # The name of the new table
        my $new_table_name = $table_ref->{'name'};

        # Number of rows in the new table
        my $new_table_number_rows = $table_ref->number_of_rows;

        # Number of selected rows in an existing table
        my $existing_table_number_rows;

        # For each table we have
        while ( my ( $name, $ref ) = each %{ $self->{'tables'} } ) {
            $existing_table_number_rows = scalar @{ $ref->{'row_numbers'} };
            my @temp_row_numbers;

            # Repeat the existing table's row numbers
            foreach ( 0 .. $new_table_number_rows - 1 ) {
                push @temp_row_numbers, @{ $ref->{'row_numbers'} };
            }
            $ref->{'row_numbers'} = \@temp_row_numbers;
        }
        my @temp_row_numbers;

        # Prepare the table just as normal
        $self->prepare_table($table_ref);

        # Reference of the new table in this RA
        my $new_table_in_ra = $self->{'tables'}->{$new_table_name};

        # Repeat each selected row number in the new table
        foreach my $row_number ( @{ $new_table_in_ra->{'row_numbers'} } ) {
            foreach ( 0 .. $existing_table_number_rows - 1 ) {
                push @temp_row_numbers, $row_number;
            }
        }
        $new_table_in_ra->{'row_numbers'} = \@temp_row_numbers;
        return $self;
    }

    # Relational algebra Join (using nested loop)
    sub nl_join {

        # Parameters: self, column alias, table, column name
        # (Use RA result's $alias column to join $table_ref's $column_name
        my ( $self, $alias, $table_ref, $column_name ) = @_;

        # Column name of the alias
        my $t1_column = $self->{'columns'}->{$alias}->{'name'};

        # The table hash which the column alias belongs to
        my $t1 = $self->{'tables'}->{ $self->{'columns'}->{$alias}->{'table'} };

        # Reference to the table of the column alias
        my $t1_ref = $t1->{'ref'};

        # The order of row numbers
        my @new_order_t1 = ();
        my @new_order_t2 = ();
        foreach my $rn1 ( @{ $t1->{'row_numbers'} } ) {
            foreach my $rn2 ( 0 .. $table_ref->number_of_rows - 1 ) {
                my $r1 = $t1_ref->read_row($rn1);
                my $r2 = $table_ref->read_row($rn2);
                if (     not $r1->{'~del'}
                     and not $r2->{'~del'}
                     and Util::trimmed( $r1->{$t1_column} ) eq
                     Util::trimmed( $r2->{$column_name} ) )
                {
                    push @new_order_t1, $rn1;
                    push @new_order_t2, $rn2;
                }
            }
        }
        foreach ( values %{ $self->{'tables'} } ) {

            # Correct order of row numbers in each table
            @{ $_->{'row_numbers'} } = @{ $_->{'row_numbers'} }[@new_order_t1];
        }

        # Prepare the new table as usual
        $self->prepare_table($table_ref);

        # Assign the new table new row numbers
        $self->{'tables'}->{ $table_ref->{'name'} }->{'row_numbers'} =
          \@new_order_t2;
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
        if ( exists $self->{'tables'}->{$table_name} ) {
            croak "(RA->prepare_table) Table name $table_name already exists";
        } else {

            # Load all row numbers
            $self->{'tables'}->{$table_name} = {
                        'ref'         => $table_ref,
                        'row_numbers' => [ 0 .. $table_ref->number_of_rows - 1 ]
            };

            # Load all columns
            foreach ( keys %{ $table_ref->{'columns'} } ) {
                $self->{'columns'}->{$_} =
                  { 'table' => $table_name, 'name' => $_ };
            }
        }
        return $self;
    }

    # Report the tables, row numbers, columns and aliases, for debugging
    sub report {

        # Parameter: self
        my $self = shift;
        while ( my ( $name, $ref ) = each %{ $self->{'tables'} } ) {
            print "Table $name @{$ref->{'row_numbers'}}\n";
        }
        while ( my ( $alias, $ref ) = each %{ $self->{'columns'} } ) {
            print "Alias $alias of table $ref->{'table'} $ref->{'name'}\n";
        }
        return $self;
    }

    # Return a copy of RA result
    sub copy {
        my $self = shift;
        my $copy = RA->new();

        # Make a copy of the two hashes which store RA result
        $copy->{'tables'}  = \%{ $self->{'tables'} };
        $copy->{'columns'} = \%{ $self->{'columns'} };
        return $copy;
    }

    # Read and return row's hash according to a row number
    sub read_row {

        # Parameters: self, row number
        my ( $self, $row_number ) = @_;
        my %hash;
        my $tables = $self->{'tables'};

        # For each column in RA result
        while ( my ( $alias, $column ) = each %{ $self->{'columns'} } ) {

            # Table of the column
            my $table = $tables->{ $column->{'table'} };

            # Name of the column
            my $column_name = $column->{'name'};

            # .....
            $hash{$column_name} =
              $table->{'ref'}
              ->read_row( $table->{'row_numbers'}->[$row_number] )
              ->{$column_name};
        }
        return \%hash;
    }

    # Return number of rows in RA result
    sub number_of_rows {

        # Parameter: self
        my $self = shift;
        while ( my ( $table_name, $table ) = each %{ $self->{'tables'} } ) {

            # Return number of kept rows
            return scalar @{ $table->{'row_numbers'} };
        }
        return 0;
    }
}
1;
