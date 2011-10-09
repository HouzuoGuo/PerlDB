# This module defines table storage operations (mostly at file level)
# PerlDB table file (.data) is human readable, similar to a spreadsheet.
#
# Example (A table with column first name, last name, phone number):
# Christina Gabby    03 2343 3232
# Joshua    Hill     02 1232 12
# David     Scott    12 3212 14
# Steve     Robinson 09 2321 1232
#
# PerlDB table columns definition file is also human readable. It defines the
# order of columns, as well as their name and size.
# The format is:
# column_name1:size1
# column_name2:size2
# column_name3:size3
#
# The order of those lines is important.
#
# Example (definition file (.def) for the table above):
# first name:10
# last name:9
# phone number:12
#
# PerlDB log file is human readable as well.
# The format is:
# Unix time stamp (tab) operation name (tab) operation details
#
# Example:
# 1317296748	AddColumn	~del,1 (A new column '~del' of size 1 is added)
# 1317296748	Insert	{col3=>3   ,col1=>a ,col2=>,~del=> } (A row is inserted)
# 1317296748	Delete	1 (Row 1 is deleted, '~del' updated to 'y')
# 1317296748	Update	0{col1=>a} (Row 0 is updated, 'col1' updated to 'a')
#
# By default, when a new table is created, it comes with the default DB_COLUMNS.
# One of them is '~del' of size 1, if a row is deleted, it will be set to 'y'.
package Table;
use strict;
use warnings;
use diagnostics;
use Carp;
use English qw(-no_match_vars);
use IO::Handle;
use Util qw(h2s trim);
use Constant qw(DB_COLUMNS);
INITIALIZER: {

    # Constructor
    sub new {

        # Parameters: type, reference to database object,
        # path of the table's directory, name of the table
        my ( $type, $database, $path, $name ) = @_;

        # Attributes:
        my $self = {

            # Path of the table's directory
            'path' => $path,

            # Name of the table
            'name' => $name,

            # Reference to the table's belonged database
            'database' => $database,

            # A hash of column information (name, length, offset, etc.)
            'columns' => {},

            # Order of columns
            'order' => [],

            # Length of each row
            'row_length' => 0,

            # Path to table definition file
            'deffile_path' => $path . $name . '.def',

            # Path to table data file
            'datafile_path' => $path . $name . '.data',

            # Path to table log file
            'logfile_path' => $path . $name . '.log'
        };
        bless $self, $type;
        $self->initialize;
        return $self;
    }

    # Initialize the table
    # (this subroutine may be called after the table is already initialized)
    sub initialize {

        # Parameter: self
        my $self = shift;

        # This subroutine may be called later again, thus reset some attributes
        $self->{'order'}      = [];
        $self->{'columns'}    = {};
        $self->{'row_length'} = 0;

        # Open necessary file handles
        $self->open_file_handles;

        # Process table definition file (where columns are defined)
        # Each line in the file is like: 'client_details:128'
        #                                (column name)  (length)
        while ( readline $self->{'deffile'} ) {
            chomp;
            my ( $column_name, $length ) = split /:/msx;

            # Each column has length and offset
            # offset is the beginning position of the column's data in each row
            $self->{'columns'}->{$column_name} =
              { 'length' => $length, 'offset' => $self->{'row_length'} };

            # Maintain the order of columns as well
            push @{ $self->{'order'} }, $column_name;

            # Accumulate row length
            $self->{'row_length'} += $length;
        }

        # Because each table row contains an EOL character...
        ++$self->{'row_length'};
        return;
    }

    # Open table definition, data and log file handles
    sub open_file_handles {

        # Parameter: self
        my $self = shift;

        # Open table definition file
        open $self->{'deffile'}, '+<', $self->{'deffile_path'}
          or croak
"(Table->new) Unable to open definition file $self->{'deffile_path'}: $OS_ERROR";
        $self->{'deffile'}->autoflush(1);

        # Open table data file
        open $self->{'datafile'}, '+<', $self->{'datafile_path'}
          or croak
"(Table->new) Unable to open data file $self->{'datafile_path'}: $OS_ERROR";
        $self->{'datafile'}->autoflush(1);

        # Open table log file
        open $self->{'logfile'}, '>>', $self->{'logfile_path'}
          or croak
"(Table->new) Unable to open log file $self->{'logfile_path'}: $OS_ERROR";
        $self->{'logfile'}->autoflush(1);
        return;
    }
}
SEEKER: {

    # Seek to row according to a row number
    sub seek_row {

        # Parameters: self, row number
        my ( $self, $row_number ) = @_;

        # row number * row size = beginning position of the row in data file
        seek $self->{'datafile'}, $self->{'row_length'} * $row_number, 0
          or croak
          "(Table->seek_row) Unable to seek to row $row_number: $OS_ERROR";
        return;
    }

    # Seek to a column according to a row number and column name
    sub seek_column {

        # Parameters: self, row number, column name
        my ( $self, $row_number, $column_name ) = @_;
        $self->seek_row($row_number);

        # Seek from current cursor position to the offset of the column
        seek $self->{'datafile'},
          $self->{'columns'}->{$column_name}->{'offset'}, 1
          or croak
"(Table->seek) Unable to seek to $row_number, $column_name: $OS_ERROR";
        return;
    }
}
READER: {

    # Read and return row's hash according to a row number
    sub read_row {

        # Parameters: self, row number
        my ( $self, $row_number ) = @_;
        $self->seek_row($row_number);

        # Read a whole row, split it later
        read $self->{'datafile'}, my $row, $self->{'row_length'};
        my %hash;

        # Split the row into column_name:value pairs
        foreach ( @{ $self->{'order'} } ) {
            my $column = $self->{'columns'}->{$_};
            $hash{$_} = substr $row, $column->{'offset'}, $column->{'length'};
        }

        # Return value is like: {'name'=>'Howard', 'age'=>'18'}
        return \%hash;
    }
}
WRITER: {

    # Write value of a column to the current data file cursor position
    sub write_column {

        # Parameters: self, column name, value of the column
        my ( $self, $column_name, $value ) = @_;
        print { $self->{'datafile'} }
          Util::trim( $value, $self->{'columns'}->{$column_name}->{'length'} )
          or croak
          "(Table->write) Unable to write $value to $column_name: $OS_ERROR";
        return;
    }

    # Insert a new row
    sub insert {

        # Parameters: self, row hash (e.g. {'name'=>'Howard', 'age'=>'18'})
        my ( $self, $row ) = @_;
        $self->memo( 'Insert', Util::h2s($row) );
        seek $self->{'datafile'}, 0, 2
          or croak "(Table->insert) Unable to seek to EOF: $OS_ERROR";

        # Write value of each column according to the order of columns
        foreach ( @{ $self->{'order'} } ) {
            $self->write_column( $_, $row->{$_} ? $row->{$_} : q{} );
        }

        # Rows in table are separated by a new line char
        print { $self->{'datafile'} } "\n"
          or croak "(Table->insert) Unable to write EOL: $OS_ERROR";
        return;
    }

    # Delete row according to a row number
    sub delete_row {

        # Parameters: self, row number
        my ( $self, $row_number ) = @_;
        if ( $row_number < $self->number_of_rows ) {
            if ( exists $self->{'columns'}->{'~del'} ) {
                $self->memo( 'Delete', $row_number );
                $self->seek_column( $row_number, '~del' );

                # set '~del' to 'y' indicates that the row has been deleted
                $self->write_column( '~del', 'y' );
            } else {
                croak '(Table->delete) There is no definition for column ~del.';
            }
        } else {
            croak "(Table->delete) Row number $row_number is out of boundary";
        }
        return;
    }

    # Update row according to a row number and row hash
    sub update {

        # Parameters: self, row number, row hash
        my ( $self, $row_number, $row ) = @_;
        if ( $row_number < $self->number_of_rows ) {
            $self->memo( 'Update', $row_number . q{ } . Util::h2s($row) );
            while ( ( my $column_name, my $value ) = each %{$row} ) {
                if ( exists $self->{'columns'}->{$column_name} ) {

                    # Seek to the column and overwrite its value
                    $self->seek_column( $row_number, $column_name );
                    $self->write_column( $column_name, $value );
                }
            }
        } else {
            croak "(Table->update) Row number $row_number is out of boundary";
        }
        return;
    }
}
STRUCTURE_CHANGER: {

    # Push a new column to both column hash and column order array
    # Be aware: this subroutine does not accumulate into row_length
    sub push_new_column {

        # Parameter: self, new column name, new column length
        my ( $self, $column_name, $length ) = @_;
        push @{ $self->{'order'} }, $column_name;
        $self->{'columns'}->{$column_name} = {
                                           'length' => $length,
                                           'offset' => $self->{'row_length'} - 1
        };
        return;
    }

    # Rebuild data file, useful for applying structure change or cleaning rows
    # Optionally may also add a new column while rebuilding data file
    sub rebuild_data_file {

        # Parameter: self, new column name, new column length
        my ( $self, $new_column_name, $new_column_length ) = @_;
        my $tempname = q{~} . time;

        # Create a temporary table for rebuilding data file
        my $temptable = $self->{'database'}->new_table($tempname);
        if ($new_column_name) {
            $self->push_new_column( $new_column_name, $new_column_length );
        }

        # Re-create all columns
        foreach ( @{ $self->{'order'} } ) {
            if ( not exists $temptable->{'columns'}->{$_} ) {
                $temptable->add_column( $_,
                                        $self->{'columns'}->{$_}->{'length'} );
            }
        }

        # Re-insert all the rows
        if ($new_column_name) {
            for ( 0 .. $self->number_of_rows - 1 ) {
                my $row = $self->read_row($_);

                # The new column has empty value
                $row->{$new_column_name} = q{};

                # Do not re-insert a row which was previously deleted
                if ( $row->{'~del'} ne 'y' ) {
                    $temptable->insert($row);
                }
            }
        } else {
            for ( 0 .. $self->number_of_rows - 1 ) {
                my $row = $self->read_row($_);

                # Do not re-insert a row which was previously deleted
                if ( $row->{'~del'} ne 'y' ) {
                    $temptable->insert($row);
                }
            }
        }
        $self->{'database'}->delete_table( $self->{'name'} );
        $self->{'database'}->rename_table( $tempname, $self->{'name'} );
        $self->open_file_handles;
        return;
    }

    # Add a column
    sub add_column {

        # Parameters: self, new column name, new column length
        my ( $self, $column_name, $length ) = @_;
        if ( exists $self->{'columns'}->{$column_name} ) {
            croak "(Table->add_column) Column $column_name already exists";
        } elsif ( length $column_name > $Constant::COLUMN_NAME_LIMIT ) {
            croak "(Table->add_column) Name $column_name is too long";
        } else {
            $self->memo( 'AddColumn', "$column_name,$length" );

            # If there are rows in the table already, the table must be rebuilt
            # to apply this structural change
            if ( $self->number_of_rows > 0 ) {
                $self->rebuild_data_file( $column_name, $length );
            } else {
                $self->push_new_column( $column_name, $length );

                # Write new column into definition file
                seek $self->{'deffile'}, 0, 2;
                print { $self->{'deffile'} } "$column_name:$length\n"
                  or croak
"(Table->add_column) Failed to update definition file: $OS_ERROR";
            }
            $self->{'row_length'} += $length;
        }
        return;
    }

    # Delete column according to a column name
    sub delete_column {

        # Parameters: self, column name
        my ( $self, $name ) = @_;
        if ( exists $self->{'columns'}->{$name} ) {
            if ( exists $Constant::DB_COLUMNS{$name} ) {
                croak
                  '(Table->delete_column) Must not delete a database column';
            }
            $self->memo( 'DeleteColumn', $name );
            my $removed_column_length = $self->{'columns'}->{$name}->{'length'};

            # Remove definition of the column in table definition file
            Util::remove_by_regex( $self->{'deffile_path'}, $name );

            # Remove the column from column hash
            delete $self->{'columns'}->{$name};

            # Remove the column from column order array
            my @new_column_order = grep { !/$name/msx } @{ $self->{'order'} };
            $self->{'order'} = \@new_column_order;

            # If there are rows in the table already, the table must be rebuilt
            # to apply this structural change
            if ( $self->number_of_rows > 0 ) {
                $self->rebuild_data_file;
            }

            # Deduct the removed column's length from row_length
            $self->{'row_length'} -= $removed_column_length;
        } else {
            croak "(Table->delete_column) Column $name does not exist";
        }
        return;
    }
}
STATUS_REPORTER: {

    # Return number of rows in this table
    sub number_of_rows {

        # Parameter: self
        my $self = shift;
        return ( -s $self->{'datafile'} ) / $self->{'row_length'};
    }

    # Return number of columns in this table
    sub number_of_columns {

        # Parameter: self
        my $self = shift;
        return scalar @{ $self->{'order'} } - scalar keys %Constant::DB_COLUMNS;
    }
}
LOG: {

    # Write an entry to table log file
    sub memo {

        # Parameters: self, log entry type (string), log entry details (string)
        my ( $self, $type, $details ) = @_;
        print { $self->{'logfile'} } time . "\t$type\t$details\n"
          or croak
          "(Table->log) Unable to write log entry $type => $details: $OS_ERROR";
        return;
    }
}

sub DESTROY {
    my $self = shift;
    close $self->{'logfile'}  or carp '(Table->DESTROY) Cannot close logfile';
    close $self->{'datafile'} or carp '(Table->DESTROY) Cannot close datafile';
    close $self->{'deffile'}  or carp '(Table->DESTROY) Cannot close deffile';
    return;
}
1;
