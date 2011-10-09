package Insert;

#use strict;
use warnings;
use diagnostics;
use RA;
use Filter;
use Trigger;
use Constant;

sub new {
    my ( $type, $table, $before_table, $after_table, $row ) = @_;
    my $self = {};
    my $ra;

    # 1. Prepare query for "before" triggers
    $ra = RA->new();
    $ra->prepare_table($before_table);
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 2. Perform "before" triggers
    execute_trigger( $table, $ra, $row );

    # 2. Physically insert the record
    $table->insert($row);

    # 3. Prepare query for "after" triggers
    $ra = RA->new();
    $ra->prepare_table($after_table);
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 4. Perform "after" triggers
    execute_trigger( $table, $ra, $row );
    bless $self, $type;
    return $self;
}

# Execute insert triggers (either "before" triggers or "after" triggers)
sub execute_trigger {

    # Parameters: reference to table, reference to RA, new row
    my ( $table, $ra, $row ) = @_;
    while ( my ( $column_name, $value ) = each %{$row} ) {

        # Do not wanna mess up the next while loop iteration, so make a copy
        my $loop_ra = $ra->copy();

        # Filter out the triggers for this column
        $loop_ra->select( 'column', \&Filter::equals, $column_name );

        # Filter out the triggers for insert operation
        $loop_ra->select( 'operation', \&Filter::equals, 'insert' );

        # For each trigger
        foreach my $cursor ( 0 .. $loop_ra->number_of_rows - 1 ) {
            my $trigger_row  = $loop_ra->read_row($cursor);
            my %all_triggers = %Constant::TRIGGERS;

            # Call trigger function with parameters
            $all_triggers{ Util::trimmed( $trigger_row->{'function'} ) }->(
                  {
                    'table'  => $table,
                    'column' => $column_name,
                    'new'    => $row
                  },
                  split( /;/msx, Util::trimmed( $trigger_row->{'parameters'} ) )
            );
        }
    }
    return;
}
1;
