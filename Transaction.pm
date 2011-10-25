# Transaction provides concurrency control, commit and rollback mechanisms.
# A transaction is represented by an object of Transaction.
# To carry out a transaction:
# 1. Begin a new transaction by getting an object of Transaction.
# 2. Acquire locks on necessary tables
# 3. Execute table operations
# 4. After finishing all table operations, Transaction->commit or rollback
#
# Instance of Transaction may be re-used after commit or rollback.
#
# Subroutines in Transaction must be used for inserting, updating or deleting
# rows in tables, thus the operations may be captured into log and be used for
# rolling back when necessary.
#
# Transaction ID is a floating point of current system time.
# Exclusive lock is a file "table name.exclusive", the content of the file
# determines the transaction ID which acquired the lock.
# Shared lock is a file "table name.shared/transaction ID", the file name
# determines the transaction ID which acquired the lock.
# Locks have a timeout, if a lock is not released within the timeout, then it
# is automatically released next time Transaction->locks_of function is called.
# Due to the mechanism transaction ID is allocated, thus the locking mechanism
# is not guaranteed to be safe.
package Transaction;
use strict;
use warnings;
use diagnostics;
use Carp;
use English qw(-no_match_vars);
use Insert;
use Update;
use Delete;
use Util;
use Constant;
use Time::HiRes qw(time);

sub new {
    my $type = shift;

    # Attributes: transaction logs, acquired locks, an ID (current system time)
    my $self = { 'log' => [], 'locked_tables' => {}, 'id' => time };
    bless $self, $type;
    return $self;
}

# Lock a table in exclusive mode
sub e_lock {

    # Parameters: self, the table
    my ( $self, $table ) = @_;
    my %existing_locks = %{ $self->locks_of($table) };

    # If any of the following happens, carp:
    # 1. Someone acquired shared lock, and it wasn't this transaction
    # 2. Someone acquired exclusive lock, and it wasn't this transaction
    if (
         (
           @{ $existing_locks{'shared'} }
           and $existing_locks{'shared'}[0] ne $self->{'id'}
         )
         or (     $existing_locks{'exclusive'} ne q{}
              and $existing_locks{'exclusive'} ne $self->{'id'} )
      )
    {
        carp '(Transaction->e_lock) '
          . $self->{'id'}
          . ' is unable to acquire exclusive lock on table '
          . $table->{'name'};
    } else {

        # If this transaction previously acquired a shared lock, remove it
        if ( $existing_locks{'shared'}[0] eq $self->{'id'} ) {
            $self->unlock($table);
        }

        # Create exclusive lock file and write this ID into it
        Util::create_file( $table->{'path'} . $table->{'name'} . '.exclusive',
                           $self->{'id'} );
    }
    return;
}

# Lock a table in shared mode
sub s_lock {
    my ( $self, $table ) = @_;
    my %existing_locks = %{ $self->locks_of($table) };

    # If someone else has got exclusive lock
    if (     $existing_locks{'exclusive'} ne q{}
         and $existing_locks{'exclusive'} ne $self->{'id'} )
    {
        carp '(Transaction->s_lock) '
          . $self->{'id'}
          . ' is unable to acquire shared lock on table '
          . $table->{'name'};
    } else {

        # If this transaction previously acquired an exclusive lock, remove it
        if ( $existing_locks{'exclusive'} eq $self->{'id'} ) {
            $self->unlock($table);
        }

        # Create shared lock file and name it using this transaction ID
        Util::create_file(
             $table->{'path'} . $table->{'name'} . '.shared/' . $self->{'id'} );
    }
    return;
}

# Return existing locks of a table, if lock(s) is expired, clear the lock
sub locks_of {
    my ( $self, $table ) = @_;
    my ( @shared_locks, $exclusive_lock );
    my $shared_locks_path = $table->{'path'} . $table->{'name'} . '.shared';

    # Open the directory of shared locks
    opendir my $shared_locks_dir, $shared_locks_path
      or croak
"(Transaction->locks_of) Cannot open shared locks directory $shared_locks_path: $OS_ERROR";

    # Read each file's name (each file name is a transaction ID)
    while ( readdir $shared_locks_dir ) {
        if ( $_ ne q{.} and $_ ne q{..} ) {

            # If the transaction has expired
            if ( time - $_ > $Constant::LOCK_TIMEOUT ) {

                # Delete the shared lock file
                unlink $table->{'path'} . $table->{'name'} . '.shared/' . $_
                  or carp
"(Transaction->locks_of) Unable to removed expired lock $_: $OS_ERROR";
            } else {
                push @shared_locks, $_;
            }
        }
    }
    closedir $shared_locks_dir;

    # Exclusive lock file path
    my $exclusive_lock_path =
      $table->{'path'} . $table->{'name'} . '.exclusive';

    # Exclusive lock transaction ID defaults to empty string
    $exclusive_lock = q{};

    # If exclusive lock exists
    if ( -f $exclusive_lock_path ) {

        # Read the exclusive lock for transaction ID
        open my $exclusive_lock_file, '<', $exclusive_lock_path
          or croak
"(Transaction->locks_of) Unable to read exclusive lock file $exclusive_lock_path: $OS_ERROR";
        $exclusive_lock = readline $exclusive_lock_file;
        close $exclusive_lock_file
          or carp
"(Transaction->locks_of) Exclusive lock file $exclusive_lock_path is left open";

        # If the transaction has expired
        if ( time - $exclusive_lock > $Constant::LOCK_TIMEOUT ) {

            # Delete the exclusive lock file
            unlink $exclusive_lock_file
              or croak
"(Transaction->locks_of) Unable to remove expired lock $exclusive_lock_path: $OS_ERROR";
        }
    }
    return { 'shared' => \@shared_locks, 'exclusive' => $exclusive_lock };
}

# Unlock a table, no matter it was locked exclusively or "sharedly"
sub unlock {
    my ( $self, $table ) = @_;
    my %existing_locks = %{ $self->locks_of($table) };
    if ( $existing_locks{'exclusive'} eq $self->{'id'} ) {
        unlink $table->{'path'} . $table->{'name'} . '.exclusive'
          or carp
          "(Transaction->unlock) Unable to release exclusive lock: $OS_ERROR";
    } elsif ( Util::in_array( $self->{'id'}, @{ $existing_locks{'shared'} } ) )
    {
        unlink $table->{'path'} . $table->{'name'} . '.shared/' . $self->{'id'}
          or carp carp
          "(Transaction->unlock) Unable to release shared lock: $OS_ERROR";
    }
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
