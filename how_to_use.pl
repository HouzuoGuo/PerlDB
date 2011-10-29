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
$tr->lock_all;

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

# (Use RA (relational algebra) to find the rows we want to update)
use RA;
my $update_source = RA->new;

# 2. Put table CONTACT into RA
$update_source->prepare_table($contact);
use Filter;

# 3. Tell RA to filter rows by WEB column
$update_source->select( 'WEB', \&Filter::equals, 'FB' );

# 4. (After filtering), iterate row numbers left
foreach ( @{ $update_source->{'tables'}->{'CONTACT'}->{'row_numbers'} } ) {

    # Update each row and set WEB = Facebook
    $tr->update( $contact, $_, { 'WEB' => 'Facebook' } );
}
$tr->commit;
foreach ( 0 .. $contact->number_of_rows - 1 ) {
    print Util::h2s( $contact->read_row($_) ), "\n";
}

# DELETE CONTACT 