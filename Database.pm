# Database logics, such as add/rename/delete tables.
# A PerlDB table has three files: .data, .log and .def.
# .data is data file, where table rows are stored.
#
# .log is log file, all operations done to the table are logged (such as
# insert, delete, update, add/remove column, etc.)
#
# .def is columns definition file, it defines the order, name and length of
# columns.
#
# .log file has the ability to recover a corrupted data file or/and def file,
# however the recovery feature is still not implemented yet.
package Database;
use strict;
use warnings;
use diagnostics;
use Carp;
use English qw(-no_match_vars);
use File::Path qw{rmtree};
use Table;
use Constant;
use Trigger;
INITIALIZER: {

    # Constructor
    sub new {

        # Parameters: type, path of database directory
        my ( $type, $path ) = @_;
        -d $path or croak "(Database->new) $path has to be a directory";

        # Attributes: path of database directory, a hash of table name and refs
        my $self = { 'path' => $path, 'tables' => {} };
        opendir my $dir, $path
          or croak "(Database->new) Unable to open directory $path: $OS_ERROR";
        while ( readdir $dir ) {

            # If it looks like a PerlDB file (either data or log or definition)
            if (/(^[^.].*)(\.data|\.log|\.def)$/msx) {
                if ( not exists $self->{'tables'}->{$1} ) {
                    $self->{'tables'}->{$1} = Table->new( $self, $path, $1 );
                }
            }
        }
        closedir $dir;
        bless $self, $type;
        return $self;
    }

    # Initialize a database directory
    # If a directory is not initialize, many PerlDB features may malfunction
    sub init_dir {

        # Parameter: self
        my $self          = shift;
        my $initfile_path = $self->{'path'} . '.init';
        if ( not -f $initfile_path ) {

            # ~before table stores "before" triggers
            my $before_table = $self->new_table('~before');
            Trigger::prepare_trigger_table($before_table);

            # ~after table stores "after" triggers
            my $after_table = $self->new_table('~after');
            Trigger::prepare_trigger_table($after_table);

            # Create a flag file to indicate that the directory is initialized
            open my $init_file, '+>', $initfile_path
              or croak
"(Database->init_dir) Unable to create file $initfile_path: $OS_ERROR";
            close $init_file
              or croak "(Database->init_dir) Unable to close $initfile_path";
        }
        return;
    }
}
STRUCTURE_CHANGER: {

    # Create a new table
    sub new_table {

        # Parameters: self, table name
        my ( $self, $name ) = @_;
        my ( $def_path, $data_path, $log_path ) = (
                                              $self->{'path'} . $name . '.data',
                                              $self->{'path'} . $name . '.def',
                                              $self->{'path'} . $name . '.log',
        );
        if ( -f $def_path or -f $data_path or -f $log_path ) {
            croak
"(Database->new_table) Cannot create table $name , table files already exist";
        } elsif ( length $name > $Constant::TABLE_NAME_LIMIT ) {
            croak "(Database->new_table) Table name $name is too long";
        } else {

            # Create files and directories for the new table
            foreach (@Constant::TABLE_FILES) {
                Util::create_empty_file( $self->{'path'} . $name . $_ );
            }
            foreach (@Constraint::TABLE_DIRS) {
                mkdir $self->{'path'} . $name . $_;
            }
            my $new_table = Table->new( $self, $self->{'path'}, $name );

            # Add database columns (default columns) into the table
            while ( my ( $column_name, $length ) = each %Constant::DB_COLUMNS )
            {
                $new_table->add_column( $column_name, $length );
            }

            # Put the new table into table hash
            $self->{'tables'}->{$name} = $new_table;
            return $new_table;
        }
    }

    # Delete a table
    sub delete_table {

        # Parameters: self, table name
        my ( $self, $name ) = @_;
        if ( exists $self->{'tables'}->{$name} ) {
            foreach (@Constant::TABLE_FILES) {
                unlink $self->{'path'} . $name
                  or carp
"(Database->delete_table) Unable to delete file $_: $OS_ERROR";
            }
            foreach (@Constraint::TABLE_DIRS) {
                rmtree $_
                  or carp
"(Database->delete_table) Unable to delete directory $_: $OS_ERROR";
            }

            # Remove the table from table hash
            delete $self->{'tables'}->{$name};
        } else {
            croak "(Database->delete_table) Table $name does not exist";
        }
        return;
    }

    # Rename a table
    sub rename_table {

        # Parameters: self, old table name, new table name
        my ( $self, $old_name, $new_name ) = @_;
        if ( exists $self->{'tables'}->{$old_name} ) {
            if ( exists $self->{'tables'}->{$new_name} ) {
                croak "(Database->rename_table) Table $new_name already exists";
            } elsif ( length $new_name > $Constant::TABLE_NAME_LIMIT ) {
                croak
                  "(Database->rename_table) Table name $new_name is too long";
            } else {
                my $new_data_path = $new_name . '.data';
                my $new_log_path  = $new_name . '.log';
                my $new_def_path  = $new_name . '.def';
                while ( my ( $old_file_name, $new_file_name ) =
                    each
                    %{ Constant::renamed_table_files( $old_name, $new_name ) } )
                {
                    rename $old_name, $new_name
                      or croak
"(Database->rename_table) Cannot rename $old_name into $new_name: $OS_ERROR";
                }

                # Update table hash
                my $table = $self->{'tables'}->{$new_name} =
                  $self->{'tables'}->{$old_name};

                # Update file paths
                $table->{'datafile_path'} = $self->{'path'} . $new_data_path;
                $table->{'logfile_path'}  = $self->{'path'} . $new_log_path;
                $table->{'deffile_path'}  = $self->{'path'} . $new_def_path;

                # Since file names are changed, re-open file handles
                $table->open_file_handles;
                delete $self->{'tables'}->{$old_name};
            }
        } else {
            croak "(Database->rename_table) Table $old_name does not exist";
        }
        return;
    }
}
ACCESS: {

    # Get reference to a table (in order to perform table operations)
    sub table {
        my ( $self, $table_name ) = @_;
        if ( exists $self->{'tables'}->{$table_name} ) {
            return $self->{'tables'}->{$table_name};
        } else {
            croak "(Database->table) Table $table_name does not exist";
        }
    }
}
1;
