#!/usr/local/bin/perl

use strict;
use warnings;
use DBI;
use Getopt::Std;
use vars qw/ %opt /;
#DBI->trace(2);

init();

my ($dbh,$activate_stream,$dbrow,$url,$feed_table,@rtable,@tuple_status);
my $client_ID = $opt{i};
my $time = $opt{d};
my $sth_insert;




##connect to the initial database 
#my $from_host="icuc-prod-rds-1.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
my $from_host="icuc-prod-primary.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
my $from_db="social_patrol_primary";
my $from_dsn = "DBI:mysql:database=$from_db;host=$from_host";
my $from_user="sproot";
my $from_pass="socialpatrol";
$dbh = DBI->connect( $from_dsn, $from_user, $from_pass, {RaiseError => 1, PrintError => 1 }) or die ( "Couldn't connect to database: " . DBI->errstr );


	##Get the database
	my $feed_db= $dbh->prepare("SELECT feed_db FROM social_patrol_primary.stream where id = ?");
	$feed_db->execute($client_ID);
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
	my $table= $dbh->prepare("SELECT feed_table FROM social_patrol_primary.stream where id = ? ");
	$table->execute($client_ID);
	$feed_table = $table->fetchrow_array();

	##connect to the feed database and then run query to find tables 
	#my $from_feed_host="icuc-test-$url.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
	my $from_feed_host="icuc-prod-$url.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
	my $from_feed_dsn = "DBI:mysql:database=$dbrow;host=$from_feed_host";
	#my $from_feed_dsn = "DBI:mysql:host=$from_feed_host";
	my $from_feed_user="sproot";
	my $from_feed_pass="socialpatrol";
	my $dbh2 = DBI->connect( $from_feed_dsn, $from_feed_user, $from_feed_pass, { RaiseError => 1, PrintError => 1 }) or die ( "Couldn't connect to database: " . DBI->errstr );

		##Connect to the test feed02 database instance and insert data
		my $destination_host="icuc-test-feed02.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
        	my $destination_dsn = "DBI:mysql:database=$dbrow;host=$destination_host";
	        my $destination_user="sproot";
        	my $destination_pass="SocialPatrol^16";
		my $dbh3 = DBI->connect( $destination_dsn, $destination_user, $destination_pass, {RaiseError => 1, PrintError => 1 }) or die ( "Couldn't connect to destination database: " . DBI->errstr );


			##connect to the host test primary database and get the name of the client from stream ID					
			my $primary_host="icuc-test-primary.czrsywfk6vbk.us-west-2.rds.amazonaws.com";
			my $primary_dsn = "DBI:mysql:database=$from_db;host=$primary_host";
	        	my $primary_user="sproot";
		 	my $primary_pass="SocialPatrol^16";
			my $dbh4 = DBI->connect( $primary_dsn, $primary_user, $primary_pass, {RaiseError => 1, PrintError => 1 }) or die ( "Couldn't connect to primary database: " . DBI->errstr );
		
				###get client's name from streamID
				my $primary= $dbh4->prepare("Select name from stream where id = ?") or die $dbh4->errstr;
			        $primary->execute($client_ID);
				my $clientName =  $primary->fetchrow_array() ;


							print "\n";
							print "The database you will connect to is-> $from_feed_host\n";
							print "database to query -> $dbrow\n";
							print "The table will be -> $feed_table\n";
							print "The destination feed host is-> icuc-test-feed02.czrsywfk6vbk.us-west-2.rds.amazonaws.com\n";
							print "The destination database is-> $dbrow\n";
							print "The client name is-> $clientName\n ";

	##get table results based on entry time
	my $table_results= $dbh2->prepare("SELECT * from $feed_table WHERE entry_time >= ?");
	$table_results->execute($time);

	#Prepare insert statement
	$sth_insert = $dbh3->prepare("INSERT IGNORE INTO $feed_table (id_code, entry_time, parent_id_code, author_name, author_code, author_url, author_image_url, entry_url, entry_types, status_code, entry_text, entry_data, last_update, pull_time, queue_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") or die $dbh3->errstr;




		##get table results from production stream table
		my $activate_info= $dbh->prepare("SELECT source_code, stream_code, client_id, name, url, status_code, stream_data, feed_db, feed_table, last_update, report_db, sla, exclude_request, owned, push FROM stream WHERE id = ?");
		$activate_info->execute($client_ID);

			##prepare the insert statement 
			 $activate_stream =$dbh4->prepare("INSERT INTO stream (source_code, stream_code, client_id, name, url, status_code, stream_data, feed_db, feed_table, last_update, report_db, sla, exclude_request, owned, push) VALUES(?, ?, 10, ?, ?, \'DISABLED\', ?, \'social_patrol_feed002\', ?, ?, ?, ?, ?, ?, ?)");

																					

			##Insert statement execution
			while (my @insert = $table_results->fetchrow_array())
				{
				$sth_insert->execute(@insert) or die $sth_insert->errstr;
				}
				#$sth_insert->finish();


						##Prepare insert statement to activate stream
						if($sth_insert->errstr)
						{
						print "The insert statement failed for data $sth_insert->errstr";
						}
							 else
								{
								##Insert statement execution to activate stream
								while (my  @activate_results =  $activate_info->fetchrow_array() )
                        					        {
                               					$activate_stream->execute(@activate_results[0, 1, 3, 4, 6, 8 .. 14]) or die $activate_stream->errstr;
									}	
				
	
								}
								#$activate_stream->finish();	





sub init{
    my $opt_string = 'i:d:';
    getopts( "$opt_string", \%opt ) or usage();
    usage() if !$opt{i};
    usage() if !$opt{d};

}



sub usage{
    print STDERR << "EOF";

usage:   $0 -i streamID -d date period

example: $0 -i 447 -d 2016-08-12

 -i          : This is the identification number for the client.
 -d          : The  date from which you want to grab stream data (year-month-day).


EOF
    exit;
}

