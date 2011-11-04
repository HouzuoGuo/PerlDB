use strict;
use warnings;
use diagnostics;

# Assign $dir to an empty directory, you must have read/write access to it
# The directory will be the workspace for the example database
my $dir = '/home/houzuo/temp/';

# Open the directory as a database
use Database;
my $db = Database->new($dir);

# Create a table called FRIEND
my $friend = $db->new_table('FRIEND');

# Create a table called Contact
my $contact = $db->new_table('CONTACT');

# Add two columns (NAME and AGE) to FRIEND table
$friend->add_column( 'NAME', 20 );    # Length is 20
$friend->add_column( 'AGE',  2 );     # Length is 2 (i.e. 0 - 99 years old)

# Add three columns (NAME, WEB, USERNAME) to CONTACT table
$contact->add_column( 'NAME',     20 );    # Length is 20
$contact->add_column( 'WEB',      10 );    # Length is 10
$contact->add_column( 'USERNAME', 20 );    # Length is 20

# Begin a transaction (we're going to modify the two tables and insert rows)
use Transaction;
my $tr = Transaction->new;

# Lock all tables in the database
$tr->lock_all($db);

# Create PK constraint on FRIEND.NAME
use Constraint;
Constraint::pk( $friend, 'NAME' );

# Create FK constraint on CONTACT.NAME (references FRIEND.NAME)
Constraint::fk( $contact, 'NAME', $friend, 'NAME' );

# Insert rows into FRIEND table;
$tr->insert( $friend, { 'NAME' => 'Buzz',      'AGE' => 18 } );
$tr->insert( $friend, { 'NAME' => 'Christoph', 'AGE' => 15 } );
$tr->insert( $friend, { 'NAME' => 'Christina', 'AGE' => 16 } );
$tr->insert( $friend, { 'NAME' => 'Stephanie', 'AGE' => 18 } );

# Insert rows into CONTACT table;
$tr->insert( $contact,
             { 'NAME' => 'Buzz', 'WEB' => 'Twitter', 'USERNAME' => 'buzz' } );
$tr->insert( $contact,
             { 'NAME' => 'Buzz', 'WEB' => 'G+', 'USERNAME' => 'jm' } );
$tr->insert( $contact,
             { 'NAME' => 'Christoph', 'WEB' => 'FB', 'USERNAME' => 'cg1' } );
$tr->insert( $contact,
             { 'NAME' => 'Christina', 'WEB' => 'FB', 'USERNAME' => 'cg2' } );

# Commit the transaction
$tr->commit;

# Print the two tables
use Util;    # Converting hash to string for output
foreach ( 0 .. $friend->number_of_rows - 1 ) {
    print Util::h2s( $friend->read_row($_) ), "\n";
}
foreach ( 0 .. $contact->number_of_rows - 1 ) {
    print Util::h2s( $contact->read_row($_) ), "\n";
}

# UPDATE CONTACT SET WEB = 'Facebook' WHERE WEB = 'FB'
# 1. Lock CONTACT table in exclusive mode
$tr->e_lock($contact);

# 2. Initialize a RA
use RA;
my $update_source = RA->new;

# 3. Put table CONTACT into RA
$update_source->prepare_table($contact);
use Filter;

# 4. Filter by WEB column
$update_source->select( 'WEB', \&Filter::equals, 'FB' );

# 5. Iterate the result (row numbers in CONTACT table)
foreach ( @{ $update_source->{'tables'}->{'CONTACT'}->{'row_numbers'} } ) {

    # 6. Update each row and set WEB = Facebook
    $tr->update( $contact, $_, { 'WEB' => 'Facebook' } );
}
$tr->commit;
print "\nAfter update:\n";
foreach ( 0 .. $contact->number_of_rows - 1 ) {
    print Util::h2s( $contact->read_row($_) ), "\n";
}

# Remove PK constraint
Constraint::remove_pk( $friend, 'NAME' );

# Now duplicated name will not raise an exception
$tr->insert( $friend, { 'NAME' => 'Buzz' } );

# Remove FK constraint
Constraint::remove_fk( $contact, 'NAME', $friend, 'NAME' );

# Insert a NAME without corresponding FIREND.NAME will not raise an exception
$tr->insert( $contact, { 'NAME' => 'Joshua' } );
$tr->commit;

# DELETE FROM FRIEND WHERE NAME = (SELECT NAME FROM CONTACT WHERE WEB = 'Facebook')
# 1. Lock CONTACT table in shared mode
$tr->s_lock($contact);

# 2. Lock FRIEND table in exclusive mode
$tr->e_lock($friend);

# 3. Join CONTACT and FRIEND using NAME
my $delete_source = RA->new;
$delete_source->prepare_table($contact);
$delete_source->nl_join( 'NAME', $friend, 'NAME' );

# 4. Filter by WEB column
$delete_source->select( 'WEB', \&Filter::equals, 'Facebook' );

# 5. Iterate the result (row numbers in FRIEND table)
foreach ( @{ $delete_source->{'tables'}->{'FRIEND'}->{'row_numbers'} } ) {

    # 6. Delete the row
    $tr->delete_row( $friend, $_ );
}
$tr->commit;
print "\nAfter delete:\n";
foreach ( 0 .. $friend->number_of_rows - 1 ) {
    print Util::h2s( $friend->read_row($_) ), "\n";
}
1;
