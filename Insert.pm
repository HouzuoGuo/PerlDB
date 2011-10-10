# Insert to table operation including execution of appropriate table triggers.
# Also keeps enough information for roll-back in a transaction.
package Insert;
use strict;
use warnings;
use diagnostics;
use RA;
use Filter;
use Trigger;
use Constant;

sub new {
    my ( $type, $table, $row ) = @_;

    # Attributes: new row, new row's row number
    my $self = { 'row' => $row, 'row_number' => $table->number_of_rows };
    my $ra;

    # 1. Prepare RA for "before" triggers
    $ra = RA->new();
    $ra->prepare_table( $table->{'database'}->table('~before') );
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 2. Perform "before" triggers
    Trigger::execute_trigger( $table, $ra, 'insert', $row );

    # 2. Physically insert the record
    $table->insert($row);

    # 3. Prepare RA for "after" triggers
    $ra = RA->new();
    $ra->prepare_table( $table->{'database'}->table('~after') );
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 4. Perform "after" triggers
    Trigger::execute_trigger( $table, $ra, 'insert', $row );
    bless $self, $type;
    return $self;
}
1;
