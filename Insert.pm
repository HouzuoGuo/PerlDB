# Insert to table operation including execution of appropriate table triggers.
package Insert;
use strict;
use warnings;
use diagnostics;
use RA;
use Filter;
use Trigger;
use Constant;

sub new {

    # Parameters: type, reference to table, new row values (in hash)
    my ( $type, $table, $row ) = @_;

    # Attributes: none
    my $self = {};
    my $ra;

    # 1. Prepare RA for "before" triggers
    $ra = RA->new();
    $ra->prepare_table( $table->{'database'}->table('~before') );
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 2. Perform "before" triggers
    Trigger::execute_trigger( $table, $ra, 'insert', $row );

    # 3. Physically insert the record
    $table->insert($row);

    # 4. Prepare RA for "after" triggers
    $ra = RA->new();
    $ra->prepare_table( $table->{'database'}->table('~after') );
    $ra->select( 'table', \&Filter::equals, $table->{'name'} );

    # 5. Perform "after" triggers
    Trigger::execute_trigger( $table, $ra, 'insert', $row );
    bless $self, $type;
    return $self;
}
1;
