#!/usr/local/bin/perl

use strict;
use warnings;
use DBI;
use Getopt::Std;
use vars qw/ %opt /;


my ($dbh,$dbrow,$url,$feed_table,@rtable);
my $client_ID = $opt{i};
my $time = $opt{d};

init();




##connect to the initial database 
my $from_host="icuc-prod-rds-1.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
my $from_db="social_patrol_primary";
my $from_dsn = "DBI:mysql:database=$from_db;host=$from_host";
my $from_user="sproot";
my $from_pass="socialpatrol";
$dbh = DBI->connect( $from_dsn, $from_user, $from_pass, { RaiseError => 1 }) or die ( "Couldn't connect to database: " . DBI->errstr );


	##Get the database
	my $feed_db= $dbh->prepare("SELECT feed_db FROM social_patrol_primary.stream where id = $client_ID");
	$feed_db->execute();
	 $dbrow =  $feed_db->fetchrow_array() ;
	my @words = split (/_/, $dbrow);
	foreach(@words)
	{
		if(/feed*/)
		{
			if(/(\w)(\w)(\w)(\w)(\w)(\w)(\w)/)	
			{
			$url="$1$2$3$4$5$7";
			chomp($url);
			}

		}
	}


	##get the feed table
	my $table= $dbh->prepare("SELECT feed_table FROM social_patrol_primary.stream where id = $client_ID ");

	$table->execute();


	while(my $tblrow = $table->fetchrow_array())
	{
     $feed_table="$tblrow";
	}       


	##connect to the feed database and then run query to find tables 
	#my $from_feed_host="icuc-test-$url.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
	my $from_feed_host="icuc-prod-$url.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
	my $from_feed_dsn = "DBI:mysql:database=$dbrow;host=$from_feed_host";
	#my $from_feed_dsn = "DBI:mysql:host=$from_feed_host";
	my $from_feed_user="sproot";
	my $from_feed_pass="socialpatrol";
	my $dbh2 = DBI->connect( $from_feed_dsn, $from_feed_user, $from_feed_pass, { RaiseError => 1 }) or die ( "Couldn't connect to database: " . DBI->errstr );

		##Connect to the test feed02 database instance and insert data
		my $destination_host="icuc-test-feed02.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
        	my $destination_dsn = "DBI:mysql:database=$dbrow;host=$destination_host";
	        my $destination_user="sproot";
        	my $destination_pass="SocialPatrol^16";
	        my $dbh3 = DBI->connect( $from_feed_dsn, $from_feed_user, $from_feed_pass, { RaiseError => 1 }) or die ( "Couldn't connect to database: " . DBI->errstr );



print "\n";
print "the database you will connect to is-> $from_feed_host\n";
print "The table will be -> $feed_table\n";

	my $table_results= $dbh2->prepare("SELECT * from $feed_table WHERE entry_time >= $time");

	$table_results->execute();
	
	#Prepare insert statement
	my $sth_insert = $dbh3->prepare("insert into $feed_table (id_code,entry_time,parent_id_code,author_name,author_code,author_url,author_image_url,entry_url,entry_types,status_code,entry_text,entry_data,last_update,pull_time,queue_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ");


		##print and insert at the same time
		#while( my @results = $table_results->fetchrow_array())
		#{
		#  print join(",",@results);
		#  print "\n";
		#  $sth_insert->execute(@results);
		#}




while (my $insert = $table_results->fetchrow_array()) {
    $sth_insert->execute(@$insert);
}




sub init(){
    my $opt_string = 'i:d:';
    getopts( "$opt_string", \%opt ) or usage();
    usage() if !$opt{i};
    usage() if !$opt{d};

}



sub usage(){
    print STDERR << "EOF";

usage:   $0 -i clientID -d date period

example: $0 -i 447 -d 2016-08-12

 -i          : This is the identification number for the client.
 -d          : The  date from which you want to grab stream data (year-month-day).


EOF
    exit;
}



