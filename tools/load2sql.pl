#!/usr/local/bin/perl -w

use DBI;
use Time::Piece;
use Net::IP::Match::Regexp qw( create_iprange_regexp match_ip );
use Term::ANSIColor qw(:constants);

#########################
#########################
my $serverdb = "localhost";
my $dbname = "netflow";
my $dbuser = "flowtools";
my $dbpass = "7ii48aws";
my $flowfile_log = "/tmp/load2sql.log";
my @flowfile_arr_in;

my $fcat = "/usr/local/bin/flow-cat";
my $fprint = "/usr/local/bin/flow-print";
my $ftable_name = "loaded_files";
my $iface = "test";
my $iface_id = 1;
my $type = 0;
my $show_messages = 1; #show messages
my $use_debug = 1; #more info
my $local_ips = create_iprange_regexp(
    qw( 192.168.36.0/24 84.54.5.29)
#    qw( 192.168.37.0/24 172.22.201.98)
);
my $ip_router = "84.54.5.29";
#my $ip_router = "172.22.201.98";
my @app_ports = (22,25,80,443,625,10022,8081,8082,8083);
my @service_ports = (53,123,161,500,1700,4500,8080);
my @torrent_ports = (6881,6882,6883,6884,6885,6886,6887,6888,6889);
##########################
##########################

if ($use_debug) {
    $start_tm = time();
}
$lt = localtime;
$year = $lt->year;
$month = sprintf("%02d",$lt->mon);
$mday = sprintf("%02d",$lt->mday);
$table_name = "$iface\_$year\_$month";

my $date_ins;
my $time_ins;

my $flowpath;
my $flowfile;

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
    if ($show_messages) {
	print $frow;
    }
    if ($parse_file_empty) {
	if ($show_messages) {
    	    print RED, "....empty!\n", RESET;
    	}
	next;
    }
    if ($is_loaded) {
	if ($show_messages) {
    	    print YELLOW, "....pass!\n", RESET;
    	}
	next;
    }
    &insert_comb_data_db;
#    &insert_data_db;
    if ($show_messages) {
	print GREEN, ".....done!", RESET;
	if ($use_debug) {
	    $curr_tm = time();
	    $diff_tm = ($curr_tm - $start_tm);
	    print " ",$diff_tm," sec";
	}
	print "\n";
    }
}

sub check_in_mysql {

$dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
    or &error_connection;
$show = "SHOW tables";
$sth = $dbh->prepare($show);
$sth->execute ();
my @row;
my $tables;
while (@row = $sth->fetchrow_array) {
    foreach $tables (@row){
    push @dbtables, $tables;
    }
}
$crt_tbl="yes";
$crt_files_tbl="yes";
while (@dbtables) {
    $table = shift @dbtables;
    if (defined $table) {
        if ($table eq $table_name) {
    	    $crt_tbl="no";
        }
        if ($table eq $ftable_name) {
    	    $crt_files_tbl="no";
    	}
    }
}
if ($crt_tbl eq "yes") {
    if ($show_messages) {
	print "Create Table $table_name\n";
    }
    &crt_table_log;
}
if ($crt_files_tbl eq "yes") {
    if ($show_messages) {
	print "Create Table $ftable_name\n";
    }
    &crt_table_load;
}
$is_loaded = 0;
$select_file = "SELECT Count(file) FROM $ftable_name WHERE file=?";
$sth = $dbh->prepare($select_file);
$sth->execute ($frow);
@row = $sth->fetchrow_array;
if ($row[0] > 0) {
    $is_loaded = 1;
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

    $create = "CREATE TABLE $table_name (src_ip INT UNSIGNED NOT NULL, src_port SMALLINT UNSIGNED DEFAULT(0) NOT NULL, dst_ip INT UNSIGNED NOT NULL, dst_port SMALLINT UNSIGNED DEFAULT(0) NOT NULL, proto TINYINT UNSIGNED, packets INT UNSIGNED DEFAULT(0), bytes BIGINT UNSIGNED DEFAULT(0), type TINYINT UNSIGNED DEFAULT(0), utime TIMESTAMP NOT NULL, INDEX(src_ip), INDEX(dst_ip), INDEX(proto), INDEX(type), INDEX(utime)) ENGINE = MyISAM";
    $sth = $dbh->prepare($create);
    $sth->execute ();
    $sth->finish;
}

sub crt_table_load {

    $create = "CREATE TABLE $ftable_name (iface_id TINYINT UNSIGNED NOT NULL, file VARCHAR(255) NOT NULL, archived BOOLEAN DEFAULT(0), file_arch VARCHAR(255), load_time DATETIME, INDEX(iface_id), INDEX(archived)) ENGINE = MyISAM";
    $sth = $dbh->prepare($create);
    $sth->execute ();
    $sth->finish;
}

sub insert_data_db {

my ($dbh,$sth,$count);
$dbh = DBI->connect("DBI:mysql:host=$serverdb;database=$dbname","$dbuser","$dbpass")
    or &error_connection_in;
$insert = "INSERT INTO $table_name (src_ip,src_port,dst_ip,dst_port,proto,packets,bytes,type,utime) VALUES (INET_ATON(?),?,INET_ATON(?),?,?,?,?,?,?)";
$sth = $dbh->prepare($insert);
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
$update = "UPDATE $table_name SET packets=packets+?, bytes=bytes+? where src_ip=INET_ATON(?) and src_port=? and dst_ip=INET_ATON(?) and dst_port=? and proto=? and type=? and utime=?";
$insert_is_loaded = "INSERT INTO loaded_files (iface_id,file,load_time) values (?,?,now())";
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
    if (match_ip($src_ip, $local_ips)) {
	if ($src_ip eq $ip_router) {
	    if (grep ( /^$src_port$/, @service_ports ) ) {
		$type = 5;
		$dst_port = 0;
	    } else {
		$type = 5;
		$src_port = 0;
	    }
	} elsif ( grep( /^$src_port$/, @app_ports ) ) {
	    $type = 3;
	    $dst_port = 0;
	} else {
	    $type = 1;
	    if ( grep( /^$src_port$/, @torrent_ports ) ) {
		$dst_port = 0;
		$dst_ip = "0.0.0.0";
	    } else {
		$src_port = 0;
	    }
	}
        $check = "SELECT EXISTS (SELECT * from $table_name where src_ip=INET_ATON(?) and src_port=? and dst_ip=INET_ATON(?) and dst_port=? and proto=? and type=? and utime=? limit 1)";
        $sth = $dbh->prepare($check);
        $sth->execute ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$type,$uftime);
        @row = $sth->fetchrow_array;
        $sth->finish;
        if ($row[0] eq 1) {
	    $sth = $dbh->prepare($update);
    	    $sth->execute ($packets,$bytes,$src_ip,$src_port,$dst_ip,$dst_port,$proto,$type,$uftime);
    	    $sth->finish;
	} else {
	    $sth = $dbh->prepare($insert);
    	    $sth->execute ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$packets,$bytes,$type,$uftime);
    	    $sth->finish;
	}
    } elsif (match_ip($dst_ip, $local_ips)) {
	if ($dst_ip eq $ip_router) {
	    $type = 6;
	    if (grep ( /^$dst_port$/, @service_ports ) ) {
		$src_port = 0;
	    } elsif ($dst_port > 20000) {
		$dst_port = 0;
	    }
	} elsif  ( grep( /^$dst_port$/, @app_ports ) ) {
	    $type = 4;
	    $src_port = 0;
	} else {
	    $type = 2;
	    if ( grep ( /^$dst_port$/, @torrent_ports ) ) {
		$src_ip = "0.0.0.0";
		$src_port = 0;
	    } else {
		$dst_port = 0;
	    }
	}
        $check = "SELECT EXISTS (SELECT * from $table_name where src_ip=INET_ATON(?) and src_port=? and dst_ip=INET_ATON(?) and dst_port=? and proto=? and type=? and utime=?)";
        $sth = $dbh->prepare($check);
        $sth->execute ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$type,$uftime);
        @row = $sth->fetchrow_array;
        $sth->finish;
        if ($row[0] eq 1) {
	    $sth = $dbh->prepare($update);
    	    $sth->execute ($packets,$bytes,$src_ip,$src_port,$dst_ip,$dst_port,$proto,$type,$uftime);
    	    $sth->finish;
	} else {
	    $sth = $dbh->prepare($insert);
    	    $sth->execute ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$packets,$bytes,$type,$uftime);
    	    $sth->finish;
	}
    } else {
	$type = 0;
	$sth = $dbh->prepare($insert);
	$sth->execute ($src_ip,$src_port,$dst_ip,$dst_port,$proto,$packets,$bytes,$type,$uftime);
	$sth->finish;
    }
}
$insert_is_loaded = "INSERT INTO loaded_files (iface_id,file,load_time) values (?,?,now())";
$sth = $dbh->prepare($insert_is_loaded);
$sth->execute ($iface_id,$frow);
$sth->finish;
$dbh->disconnect;
}

sub parse_log_file {
open (PARSFILE, "$flowfile_log");
$parse_file_empty = 0;
@flowfile_arr_in = ();
while ($line_parse=<PARSFILE>) {
    chomp $line_parse;
    $line_parse =~ s/[\s\t]+/\t/g;
    push @flowfile_arr_in, $line_parse;
}
close (PARSFILE);
if ( -z $flowfile_log ) {
    $parse_file_empty = 1;
}
truncate ("$flowfile_log",0);
}

exit(0);
