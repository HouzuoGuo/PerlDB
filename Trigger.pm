# Programmed database triggers and trigger-related logics.
#
# PerlDB supports both "before" triggers and "after" triggers. Triggers are
# executed for each of new/deleted rows.
# PerlDB database uses two special tables for storing triggers: ~before and
# ~after. The tables are created by calling Database->init_dir.
#
# Definition of the table is given as following:
# ~del:1        (Default database column)
# table:50      (The table's name which the trigger executes on)
# column:50     (The column's name)
# operation:6   (Type of operation: insert, update or delete)
# function:50   (Trigger function's key)
# parameters:50 (Extra parameters to trigger function, separated by ;)
#
# For example, when a new row is inserted:
# 1. Load ~before table into RA  (in table operation function)
# 2. Filter RA by table name     (in table operation function)
# 3. Call execute_trigger        (in table operation function)
# (For each column-value pairs)
# 3.1. Filter RA by column name    (in execute_trigger function)
# 3.2. Filter RA by operation type (in execute_trigger function)
# 3.3. For each row in RA result, call the trigger function with parameters
# (Loop done)
# 4. Physically insert the row   (in table operation function)
# 5. Load ~after table into RA   (in table operation function)
# 6. Call execute_trigger        (in table operation function)
# 7. Perform "after" triggers as in the loop above
#
# Thus, customized trigger functions are also supported.
package Trigger;
use strict;
use warnings;
use diagnostics;
use Util;
use Constant;
use Carp;

# Execute table triggers
sub execute_trigger {

    # Parameters: reference to table, reference to RA of trigger table,
    # row 1, row 2
    #
    # For insert, row 1 is the new row
    # For update, row 1 is the old row, row 2 is the new row
    # For delete, row 1 is the old row
    my ( $table, $ra, $operation, $row1, $row2 ) = @_;

    # For each column value to be changed
    while ( my ( $column_name, $value ) = each %{$row1} ) {

        # Do not wanna mess up the next while loop iteration, so make a copy
        my $loop_ra = $ra->copy();

        # Filter the triggers only for this column
        $loop_ra->select( 'column', \&Filter::equals, $column_name );

        # Filter the triggers only for the operation
        $loop_ra->select( 'operation', \&Filter::equals, $operation );

        # For each row of RA result (i.e. for each defined trigger)
        foreach my $cursor ( 0 .. $loop_ra->number_of_rows - 1 ) {

            # Read the row
            my $trigger_row  = $loop_ra->read_row($cursor);
            my %all_triggers = %Constant::TRIGGERS;

            # Call trigger function with parameters
            $all_triggers{ Util::trimmed( $trigger_row->{'function'} ) }->(
                {
                  'table'  => $table,          # Affected table
                  'column' => $column_name,    # Affected column
                  'row1'   => $row1,
                  'row2'   => $row2
                },

                # Extra parameters
                split( /;/msx, Util::trimmed( $trigger_row->{'parameters'} ) )
            );
        }
    }
    return;
}

# Prepare a table for storing database triggers
sub prepare_trigger_table {

    # Parameter: a database table
    my $table = shift;
    $table->add_column( 'table',      $Constant::TABLE_NAME_LIMIT );
    $table->add_column( 'column',     $Constant::COLUMN_NAME_LIMIT );
    $table->add_column( 'operation',  $Constant::OPERATION_NAME_LIMIT );
    $table->add_column( 'function',   $Constant::TRIGGER_FUNC_NAME_LIMIT );
    $table->add_column( 'parameters', $Constant::TRIGGER_PARAMS_LIMIT );
    return;
}

# btw, constraints must all be "before" triggers
CONSTRAINTS: {

    sub pk {

        # Parameter: hash of parameters passed by execute_trigger
        my $params = shift;

        # The affected table
        my $table = $params->{'table'};

        # The affected column
        my $column_name = $params->{'column'};

        # New value for the column
        my $new_value = $params->{'row1'}->{$column_name};
        if ( Trigger::found( $new_value, $table, $column_name ) ) {
            croak "(Trigger->pk) New value $new_value violates PK constraint on"
              . ' Table '
              . $table->{'name'}
              . " column $column_name";
        }
        return;
    }

    sub fk {

        # Parameter: hash of parameters passed from trigger, extra paremeters
        my ( $params, @extra_params ) = @_;

        # The affected table
        my $table = $params->{'table'};

        # The affected column
        my $column_name = $params->{'column'};

        # New value for the column
        my $new_value = $params->{'row1'}->{$column_name};
        # KNOWN BUG: updating FK shall not use this trigger function. To be fixed.

        # Reference to PK table (table name is [0] in extra parameters)
        my $pk_table = $table->{'database'}->table( $extra_params[0] );

        # Foreign key column name ([1] in extra parameters)
        my $pk_column = $extra_params[1];
        if ( not Trigger::found( $new_value, $pk_table, $pk_column ) ) {
            croak "(Trigger->fk) New value $new_value violates FK constraint on"
              . ' Table '
              . $table->{'name'}
              . " column $pk_column";
        }
        return;
    }
}
TRIGGERS: {

    sub delete_restricted {

        # Parameter: hash of parameters passed from trigger, extra paremeters
        my ( $params, @extra_params ) = @_;

        # The affected table
        my $table = $params->{'table'};

        # The affected column
        my $column_name = $params->{'column'};

        # Value of the column
        my $value = Util::trimmed( $params->{'row1'}->{$column_name} );

        # Reference to FK table (table name is [0] in extra parameters)
        my $fk_table = $table->{'database'}->table( $extra_params[0] );

        # Foreign key column name ([1] in extra parameters)
        my $fk_column = $extra_params[1];
        if ( Trigger::found( $value, $fk_table, $fk_column ) ) {
            croak "(Trigger->delete_restricted) Delete of value $value is "
              . 'restricted on Table '
              . $table->{'name'}
              . " column $fk_column";
        }
        return;
    }

    sub update_restricted {

        # Parameter: hash of parameters passed from trigger, extra paremeters
        my ( $params, @extra_params ) = @_;

        # The affected table
        my $table = $params->{'table'};

        # The affected column
        my $column_name = $params->{'column'};

        # Old value of the column
        my $value = Util::trimmed( $params->{'row1'}->{$column_name} );

        # Reference to FK table (table name is [0] in extra parameters)
        my $fk_table = $table->{'database'}->table( $extra_params[0] );

        # Foreign key column name ([1] in extra parameters)
        my $fk_column = $extra_params[1];
        if ( Trigger::found( $value, $fk_table, $fk_column ) ) {
            croak "(Trigger->update_restricted) Update of value $value is "
              . 'restricted on Table '
              . $table->{'name'}
              . " column $fk_column";
        }
        return;
    }
}

# General behaviors of some triggers
BEHAVIORS: {

    # Return 1 if a value is found in a column, otherwise 0
    sub found {
        my ( $value, $table, $column_name ) = @_;
        $value = Util::trimmed($value);
        foreach my $cursor ( 0 .. $table->number_of_rows - 1 ) {
            if ( Util::trimmed( $table->read_row($cursor)->{$column_name} ) eq
                 $value )
            {
                return 1;
            }
        }
        return 0;
    }
}
1;
