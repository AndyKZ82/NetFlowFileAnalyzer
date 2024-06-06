#!/usr/local/bin/perl -w

use DBI;
#use Time::localtime;
use Time::Piece;

#########################
#########################
my $serverdb = "localhost";
my $dbname = "netflow";
my $dbuser = "flowtools";
my $dbpass = "7ii48aws";
my $flowfile_log = "/tmp/load2sql.log";
my @flowfile_arr;
my @flowfile_arr_in;

my $fcat = "/usr/local/bin/flow-cat";
my $fprint = "/usr/local/bin/flow-print";
my $iface = "test";
my $type = 0;
##########################
##########################

$lt = localtime;
$year = $lt->year;
$month = sprintf("%02d",$lt->mon);
$mday = sprintf("%02d",$lt->mday);
$table_name = "$iface\_$year\_$month";

my $date_ins;
my $time_ins;

my $flowpath;
my $flowfile;
my @flowhour;

$flowpath = "/var/flow/$iface/$year-$month/$year-$month-$mday"; #flow-capture -N-2
$flowfile = "ft-v05."."*";

@flows = `ls $flowpath/$flowfile`;

while (@flows) {

    $frow = shift @flows;
    chomp($frow);
    system "$fcat $frow \| $fprint \| grep -v 'prot' > $flowfile_log";
    $fname = $frow;
    $fname =~ s/$flowpath\///;
    $ftime = Time::Piece->strptime($fname, 'ft-v05.%Y-%m-%d.%H%M%S+0700');
#        $ftime = Time::Piece->strptime($fname, 'ft-v05.%Y-%m-%d.%H%M%S%z'); with timezone
    $uftime = $ftime->datetime;
    &parse_log_file;
    &check_in_mysql;
#    &insert_comb_data_db;
    &insert_data_db;
}

sub check_in_mysql {

my ($dbh,$sth,$count);
$dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
    or &error_connection;
$sth = $dbh->prepare("SHOW tables");
$sth->execute ();
my @row;
my $tables;
while (@row = $sth->fetchrow_array) {
    foreach $tables (@row){
    push @dbtables, $tables;
    }
}
$crt_tbl="yes";
while (@dbtables) {
    $table = shift @dbtables;
    if (defined $table) {
        if ($table eq $table_name) {
                $crt_tbl="no";
        }
    }
}
if ($crt_tbl eq "yes") {
    print "Create Table\n";
    &crt_table_log;
}
$sth->finish;
$dbh->disconnect;
}

sub error_connection {

print "Error.\n";
foreach $line_arr(@flowfile_arr_in) {
    open (DUMPFILE, ">>$flowfile_log");
    $line_arr = "$line_arr\n";
    print DUMPFILE $line_arr;
    close (DUMPFILE);
}
die "\n";
}

sub crt_table_log {

    my ($dbh,$sth,$count);
    $dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
        or &error_connection;
    $create = "CREATE TABLE $table_name (src_ip INT UNSIGNED NOT NULL, src_port SMALLINT UNSIGNED DEFAULT(0) NOT NULL, dst_ip INT UNSIGNED NOT NULL, dst_port SMALLINT UNSIGNED DEFAULT(0) NOT NULL, proto TINYINT UNSIGNED, packets INT UNSIGNED DEFAULT(0), bytes BIGINT UNSIGNED DEFAULT(0), type TINYINT UNSIGNED DEFAULT(0), utime TIMESTAMP NOT NULL, INDEX(src_ip), INDEX(dst_ip), INDEX(proto), INDEX(type), INDEX(utime)) ENGINE = MyISAM";
    $sth = $dbh->prepare("$create");
    $sth->execute ();
    $sth->finish;
    $dbh->disconnect;
}

sub insert_data_db {

my ($dbh,$sth,$count);
$dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
    or &error_connection_in;
$insert = "INSERT INTO $table_name (src_ip,src_port,dst_ip,dst_port,proto,packets,bytes,type,utime) VALUES (INET_ATON(?),?,INET_ATON(?),?,?,?,?,?,?)";
$sth = $dbh->prepare("$insert");
while (@flowfile_arr_in) {
    $line_in = shift @flowfile_arr_in;
    ($src_ip,$dst_ip,$proto,$src_port,$dst_port,$bytes,$packets)=split(/[\s\t]+/,$line_in);
    if (!defined $proto){
        $proto="0";
    }
    if (!defined $packets){
        $packets="0";
    }
    if (!defined $bytes){
        $bytes="0";
    }
#        if ($dst_ip =~ m/.255/) {next;}
#        if ($dst_ip =~ m/239./) {next;}
#        if ($dst_ip =~ m/224./) {next;}
#        if ($proto eq "17" and $src_port eq "53") {next;}
#        if ($proto eq "17" and $dst_port eq "53") {next;}
#        if ($proto eq "1") {next;}
#        if ($src_ip eq "10.2.214.20") {next;}
#        if ($dst_ip eq "10.2.214.20") {next;}
#        if ($src_ip eq "81.30.196.90") {next;}
#        if ($ip_to eq "81.30.196.90") {next;}
    $sth->execute ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$packets,$bytes,$type,$uftime);
}
$sth->finish;
$dbh->disconnect;
}

sub insert_comb_data_db {

my ($dbh,$sth,$count);
$dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
    or &error_connection_in;
$insert = "INSERT INTO $table_name (src_ip,src_port,dst_ip,dst_port,proto,packets,bytes,type,utime) VALUES (INET_ATON(?),?,INET_ATON(?),?,?,?,?,?,?)";
while (@flowfile_arr_in) {
    $line_in = shift @flowfile_arr_in;
    ($src_ip,$dst_ip,$proto,$src_port,$dst_port,$bytes,$packets)=split(/[\s\t]+/,$line_in);
    if (!defined $proto){
        $proto="0";
    }
    if (!defined $packets){
        $packets="0";
    }
    if (!defined $bytes){
        $bytes="0";
    }
    if ($src_ip eq "192.168.36.15") {
        $check = "SELECT EXISTS (SELECT * from $table_name where src_ip=INET_ATON(?) and dst_port=? and proto=? and utime=?)";
        $sth = $dbh->prepare("$check");
        $sth->execute ($src_ip,$dst_port,$proto,$uftime);
        @row = $sth->fetchrow_array;
        print $row[0]," ",$src_port," ";
        $sth->finish;
        if ($row[0] eq 1) {
	    print "pass1 ";
	} else {
	    $sth = $dbh->prepare("$insert");
    	    $sth->execute ($src_ip,0,$dst_ip,$dst_port,$proto,$packets,$bytes,$type,$uftime);
	}
    } elsif ($src_ip eq "192.168.36.115") {
        $check = "SELECT EXISTS (SELECT * from $table_name where src_port=?)";
        $sth = $dbh->prepare("$check");
        $sth->execute ($src_port);
        @row = $sth->fetchrow_array;
        print $row[0]," ",$src_port," ";
        $sth->finish;
        if ($row[0] eq 1) {
	    print "pass2 ";
	} else {
	    $sth = $dbh->prepare("$insert");
    	    $sth->execute ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$packets,$bytes,$type,$uftime);
	}
    } else {
	$sth = $dbh->prepare("$insert");
	$sth->execute ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$packets,$bytes,$type,$uftime);
    }    
}
$sth->finish;
$dbh->disconnect;
}

sub parse_log_file {
open (PARSFILE, "$flowfile_log");
while ($line_parse=<PARSFILE>) {
    chomp $line_parse;
    $line_parse =~ s/[\s\t]+/\t/g;
    push @flowfile_arr_in, $line_parse;
}
close (PARSFILE);
truncate ("$flowfile_log",0);
}

exit(0);
