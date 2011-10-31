# Delete table row operation including execution of appropriate table triggers.
package Delete;
use strict;
use warnings;
use diagnostics;
use RA;
use Filter;
use Trigger;
use Constant;

sub new {

    # Parameters: type, reference to table, number of the row to be deleted
    my ( $type, $table, $row_number ) = @_;

    # Attributes: none
    my $self = {};
    my $ra;

    # 1. Prepare RA for "before" triggers
    $ra = RA->new();
    $ra->prepare_table( $table->{'database'}->table('~before') );
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 2. Perform "before" triggers
    Trigger::execute_trigger( $table, $ra, 'delete',
                              $table->read_row($row_number) );

    # 3. Physically delete the row
    $table->delete_row($row_number);

    # 4. Prepare RA for "after" triggers
    $ra = RA->new();
    $ra->prepare_table( $table->{'database'}->table('~after') );
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 5. Perform "after" triggers
    Trigger::execute_trigger( $table, $ra, 'delete',
                              $table->read_row($row_number) );
    bless $self, $type;
    return $self;
}
1;
