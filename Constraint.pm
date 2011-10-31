# Make/remove constraints
package Constraint;
use strict;
use warnings;
use diagnostics;
use RA;
use Filter;

# Make a column PK
sub pk {

    # Parameters: PK table, PK column name
    my ( $table, $column_name ) = @_;
    $table->{'database'}->table('~before')->insert(
                                                 {
                                                   'table'  => $table->{'name'},
                                                   'column' => $column_name,
                                                   'function'  => 'pk',
                                                   'operation' => 'insert'
                                                 }
    );
    $table->{'database'}->table('~before')->insert(
                                                 {
                                                   'table'  => $table->{'name'},
                                                   'column' => $column_name,
                                                   'function'  => 'pk',
                                                   'operation' => 'update'
                                                 }
    );
    return;
}

# Make a column FK
sub fk {

    # Parameters: FK table, FK column name, PK table, PK column name
    my ( $fk_table, $fk_column_name, $pk_table, $pk_column_name ) = @_;
    $fk_table->{'database'}->table('~before')->insert(
                 {
                   'table'      => $fk_table->{'name'},
                   'column'     => $fk_column_name,
                   'function'   => 'fk',
                   'operation'  => 'insert',
                   'parameters' => $pk_table->{'name'} . q{;} . $pk_column_name
                 }
    );
    $fk_table->{'database'}->table('~before')->insert(
                 {
                   'table'      => $fk_table->{'name'},
                   'column'     => $fk_column_name,
                   'function'   => 'fk',
                   'operation'  => 'update',
                   'parameters' => $pk_table->{'name'} . q{;} . $pk_column_name
                 }
    );
    $pk_table->{'database'}->table('~before')->insert(
                 {
                   'table'      => $pk_table->{'name'},
                   'column'     => $pk_column_name,
                   'function'   => 'update_restricted',
                   'operation'  => 'update',
                   'parameters' => $fk_table->{'name'} . q{;} . $fk_column_name
                 }
    );
    $pk_table->{'database'}->table('~before')->insert(
                 {
                   'table'      => $pk_table->{'name'},
                   'column'     => $pk_column_name,
                   'function'   => 'delete_restricted',
                   'operation'  => 'delete',
                   'parameters' => $fk_table->{'name'} . q{;} . $fk_column_name
                 }
    );
    return;
}

# Remove PK constraint on a column
sub remove_pk {

    # Parameters: PK table, PK column name
    my ( $table, $column_name ) = @_;
    my $ra            = RA->new();
    my $trigger_table = $table->{'database'}->table('~before');

    # Query on ~before table
    $ra->prepare_table($trigger_table);

    # Filter by column name
    $ra->select( 'column', \&Filter::equals, $column_name );

    # Filter by table name
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # For each row
    foreach ( @{ $ra->{'tables'}->{'~before'}->{'row_numbers'} } ) {

        # Delete the row (delete the constraint)
        $trigger_table->delete_row($_);
    }
    return;
}

# Remove FK constraint on a column
sub remove_fk {

    # Parameters: PK table, PK column name, FK table, FK column name
    my ( $fk_table, $fk_column_name, $pk_table, $pk_column_name ) = @_;
    my $ra            = RA->new();
    my $trigger_table = $fk_table->{'database'}->table('~before');

    # Query on ~before table
    $ra->prepare_table( $fk_table->{'database'}->table('~before') );

    # Filter by column name
    $ra->select( 'column', \&Filter::equals, $fk_column_name );

    # Filter by table name
    $ra->select( 'table', \&Filter::equals, $fk_table->{'name'} );

    # Filter by parameter (PK table and PK column name)
    $ra->select( 'parameters', \&Filter::equals,
                 $pk_table->{'name'} . q{;} . $pk_column_name );

    # For each row
    foreach ( @{ $ra->{'tables'}->{'~before'}->{'row_numbers'} } ) {

        # Delete the row (delete the constraint)
        $trigger_table->delete_row($_);
    }
    return;
}
1;
