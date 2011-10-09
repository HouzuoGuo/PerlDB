package Trigger;
use strict;
use warnings;
use diagnostics;
use Util;
use Constant;
use Carp;

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

# btw, constraints are all "before" triggers
CONSTRAINTS: {

    sub pk {

        # Parameter: hash of parameters passed from trigger
        my $params = shift;
        my $table       = $params->{'table'};
        my $column_name = $params->{'column'};
        my $new_value   = $params->{'new'}->{$column_name};
        foreach my $cursor ( 0 .. $table->number_of_rows - 1 ) {

            # If any existing value duplicates the new value, croak
            if ( Util::trimmed( $table->read_row($cursor)->{$column_name} ) eq
                 $new_value )
            {
                croak
                  "(Trigger->pk) New value $new_value violates PK constraint on"
                  . ' Table '.$table->{'name'}." column $column_name";
            }
        }
        return;
    }

    sub fk {
        return;
    }
}
TRIGGERS: {

    sub delete_restricted {
    }

    sub update_restricted {
    }
}
1;
