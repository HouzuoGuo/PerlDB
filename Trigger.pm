package Trigger;
use strict;
use warnings;
use diagnostics;
use Constant;

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
CONSTRAINTS: {

    sub pk {
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
