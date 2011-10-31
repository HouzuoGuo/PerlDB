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
FILESYSTEM: {

    # Table files
    Readonly our @TABLE_FILES => ( '.data', '.log', '.def' );

    # Table directories
    Readonly our @TABLE_DIRS => ('.shared');

    # Return a hash of old table file names VS new table file names
    sub renamed_table_files {

        # Parameter: old name, new name
        my ( $old_name, $new_name ) = @_;
        my %old_vs_new = ();
        foreach ( @TABLE_FILES, @TABLE_DIRS ) {
            $old_vs_new{ $old_name . $_ } = $new_name . $_;
        }
        return %old_vs_new;
    }
}
TRANSACTION: {

    # Timeout for unreleased table lock (both exclusive and shared) in seconds
    Readonly our $LOCK_TIMEOUT => 300;
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
    our %TRIGGERS = (
                      'pk'                => \&Trigger::pk,
                      'fk'                => \&Trigger::fk,
                      'update_restricted' => \&Trigger::update_restricted,
                      'delete_restricted' => \&Trigger::delete_restricted
    );
}
1;
