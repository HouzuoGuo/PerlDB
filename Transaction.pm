# Transaction provides concurrency control, commit and rollback mechanisms.
# A transaction is represented by an object of Transaction.
# To carry out a transaction:
# 1. Begin a new transaction by getting an object of Transaction.
# 2. Acquire locks on necessary tables by calling Transaction->lock
# 3. Execute table operations
# 4. After finishing all table operations, Transaction->commit or rollback
#
# Instance of Transaction may be re-used after commit or rollback.
#
# Subroutines in Transaction must be used for inserting, updating or deleting
# rows in tables, thus the operations may be captured into log and be used for
# rolling back when necessary.
package Transaction;
use strict;
use warnings;
use diagnostics;
use Carp;
use Insert;
use Update;
use Delete;
use Util;

sub new {
    my $type = shift;

    # Attributes: transaction logs, acquired locks
    my $self = { 'log' => [], 'locked_tables' => {} };
    bless $self, $type;
    return $self;
}

# Lock a table in exclusive mode
sub e_lock {

    # Parameters: self, the table
    my ( $self, $table ) = @_;
    return;
}

# Lock a table in shared mode
sub s_lock {
    my ( $self, $table ) = @_;
    return;
}

# Unlock a table, no matter it was locked exclusively or "sharedly"
sub unlock {
    my ( $self, $table ) = @_;
    return;
}

# Insert a row to table
sub insert {

    # Parameters: self, the table, the new row
    my ( $self, $table, $row ) = @_;
    eval {
        my $physically_insert = Insert->new( $table, $row );

        # Remember the row number of the new row
        push @{ $self->{'log'} },
          (
            'op'         => 'insert',
            'row_number' => $table->number_of_rows,
            'table'      => $table
          );
        1;
      }
      or do {
        $self->rollback();
        croak "(Transaction->insert) Failed to insert the row:@!";
      };
    return;
}

# Update a row
sub update {

    # Parameters: self, the table, row number, new row
    my ( $self, $table, $row_number, $row ) = @_;
    eval {
        my $old_row = $table->read_row($row_number);
        my $physically_update = Update->new( $table, $row_number, $row );

        # Remember: the updated row number, the original row values
        push @{ $self->{'log'} },
          (
            'op'         => 'update',
            'row_number' => $row_number,
            'old_row'    => $old_row,
            'table'      => $table
          );
      }
      or do {
        $self->rollback();
        croak "(Transaction->update) Failed to update row $row_number into "
          . Util::h2s($row) . " :@!";
      };
    return;
}

# Delete a row
sub delete_row {

    # Parameters: self, the table, row number
    my ( $self, $table, $row_number ) = @_;
    eval {
        my $physically_delete = Delete->new( $table, $row_number );

        # Remember: the deleted row number, the original row values
        push @{ $self->{'log'} },
          (
            'op'         => 'update',
            'row_number' => $row_number,
            'table'      => $table
          );
      }
      or do {
        $self->rollback();
        croak "(Transaction->delete) Failed to delete row $row_number:@!";
      };
    return;
}

# Roll back table operations then release all locked tables and clear log
sub rollback {

    # Parameter: self
    my $self = shift;
    # Reverse actions
    foreach ( reverse @{ my $self->{'log'} } ) {
        if ( $_->{'op'} eq 'insert' ) {

            # Roll back of insert = delete
            $_->{'table'}->delete_row( $_->{'row_number'} );
        } elsif ( $_->{'op'} eq 'update' ) {

            # Roll back of update = update to original
            $_->{'table'}->update( $_->{'row_number'}, $_->{'old_row'} );
        } elsif ( $_->{'op'} eq 'delete' ) {

            # Roll back of delete = clear '~del' flag
            $_->{'table'}->update( $_->{'row_number'}, { '~del', q{ } } );
        }
    }
    $self->commit;
    return;
}

# Release all locked tables and clear log
sub commit {

    # Parameter: self
    my $self = shift;
    return;
}
1;
