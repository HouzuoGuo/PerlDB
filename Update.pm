# Update table row operation including execution of appropriate table triggers.
package Update;
use strict;
use warnings;
use diagnostics;
use RA;
use Filter;
use Trigger;
use Constant;

sub new {

    # Parameters: type, reference to table, number of the row to be update,
    # new row values (in hash)
    my ( $type, $table, $row_number, $row ) = @_;

    # Attributes: none
    my $self = {};
    my $ra;

    # 1. Prepare RA for "before" triggers
    $ra = RA->new();
    $ra->prepare_table( $table->{'database'}->table('~before') );
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 2. Perform "before" triggers
    Trigger::execute_trigger( $table, $ra, 'update',
                              $table->read_row($row_number), $row );

    # 3. Physically update the row
    $table->update( $row_number, $row );

    # 4. Prepare RA for "after" triggers
    $ra = RA->new();
    $ra->prepare_table( $table->{'database'}->table('~after') );
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 5. Perform "after" triggers
    Trigger::execute_trigger( $table, $ra, 'update',
                              $table->read_row($row_number), $row );
    bless $self, $type;
    return $self;
}
1;
