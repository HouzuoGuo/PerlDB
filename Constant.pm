# Database constants. All PerlDB constants are kept here
package Constant;
use strict;
use warnings;
use diagnostics;
use Readonly;

# Default database columns
Readonly our %DB_COLUMNS => ( '~del' => 1 );

# Maximum length of table name
Readonly our $TABLE_NAME_LIMIT => 50;

# Maximum length of column name
Readonly our $COLUMN_NAME_LIMIT => 50;

# Maximum length of trigger parameters list
Readonly our $TRIGGER_PARAMS_LIMIT => 50;

# Maximum length of trigger operation name
Readonly our $OPERATION_NAME_LIMIT => 6;
1;
