# Database constants.
package Constant;
use strict;
use warnings;
use diagnostics;
use Readonly;
use Trigger;
DB: {

    # Default database columns
    Readonly our %DB_COLUMNS => ( '~del' => 1 );

    # Maximum length of table name
    Readonly our $TABLE_NAME_LIMIT => 50;

    # Maximum length of column name
    Readonly our $COLUMN_NAME_LIMIT => 50;
}
TRIGGER: {

    # Maximum length of name of a trigger function
    Readonly our $TRIGGER_FUNC_NAME_LIMIT => 50;

    # Maximum length of trigger parameters list
    Readonly our $TRIGGER_PARAMS_LIMIT => 50;

    # Maximum length of trigger operation name
    Readonly our $OPERATION_NAME_LIMIT => 6;
    
    # Table triggers, all triggers in-use must be defined here
    # Format: 'function name as in trigger table'=>reference_to_function
    our %TRIGGERS = ('pk'=>\&Trigger::pk, 'fk'=>\&Trigger::fk);
}
1;
