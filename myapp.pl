#!/usr/bin/env perl

use strict;
use Mojolicious::Lite -signatures;

plugin Database => {
	dsn => 'dbi:mysql:dbname=gaz',
	username => 'worker',
	password => '',
	no_disconnect => 1,
	dbi_attr => { 'AutoCommit' => 1, 'RaiseError' => 1, 'PrintError' =>1 },
	on_connect => sub {
		my $dbh = shift or die $DBI::errstr;
		#$dbh->do("DELETE FROM message");
		#$dbh->do("DELETE FROM log");
	}
};

my %flags = (
	'<=' => 2, #Отбор в таблицу message
	'=<' => 1,
	'->' => 1,
	'**' => 1,
	'==' => 1,
);

sub send_db {
	my ( $dbh, $statement, %values ) = @_;

	my %queries = (
		message => 'INSERT INTO message (int_id, created, str, id) VALUES (?, ?, ?, ?)',
		log     => 'INSERT INTO log (int_id, created, str, address) VALUES (?, ?, ?, ?)',
	);

	my $sth = $dbh->prepare($queries{$statement});
	$sth->execute( map { $values{$_} } (
			qw/ int_id created str /,
			$statement eq 'message' ? 'id' : $statement eq 'log' ? 'address' : ''
	) );
}

sub check_datetime {
	my ( $dbh, $value ) = @_;
	
	return $dbh->selectall_array( "SELECT TO_SECONDS(?) IS NOT NULL AS valid", undef, $value );
}

get '/load' => sub ($c) {
	my ( $message_count, $log_count, $total_count ) = ( 0, 0, 0 );

	open my $fh, '<', 'out';
	while (my $row = <$fh>) {
		chomp $row;
		$total_count++;
		my @portions = split ' ', $row;

		my %values;
		if ( $portions[0] =~ /^\d\d\d\d-\d\d-\d\d$/ && $portions[1] =~ /^\d\d:\d\d:\d\d$/ ) {
			$values{created} = join ' ', $portions[0], $portions[1];
		}
		unless ( check_datetime $c->db, $values{created} ) {
			app->log->error( sprintf "Not valid date: %s", $row );
			next;
		} else {
			$values{str} = join ' ', @portions[ 2 .. $#portions ];
		}

		if ( $portions[2] =~ /^\w{6}-\w{6}-\w\w$/ ) {
			$values{int_id} = $portions[2];
		}
		# 2012-02-13 15:00:55 SMTP connection from [109.70.26.4] (TCP/IP connection count = 1)
		unless ( $values{int_id} ) {
			app->log->warn( sprintf "Not valid int_id: %s", $row );
			next;
		}

		( undef, $values{id} ) = split 'id=', $portions[-1];
		if ( $values{id} && $flags{ $portions[3] } && $flags{ $portions[3] } > 1 ) {
			# 2012-02-13 14:39:22 1RwtJa-0009RI-KL <= tpxmuwr@somehost.ru H=mail.somehost.com [84.154.134.45] P=esmtp S=1716 id=120213143628.BLOCKED.453962@whois.somehost.ru

			send_db( $c->db, 'message', %values );
			$message_count++;
		} else {
			# Есть флаг
			if ( $flags{ $portions[3] } ) {
				# 2012-02-13 15:00:55 1RwteR-000Om4-65 == psqgg@yandex.ru R=dnslookup T=remote_smtp defer (-1): domain matches queue_smtp_domains, or -odqs set
				$values{address} = $portions[4];
			}

			send_db( $c->db, 'log', %values );
			$log_count++;
		}
	}
	close $fh;

	$c->db->commit;
	$c->render(template => 'load', messages => $message_count, logs => $log_count, total => $total_count );
};

get '/search' => sub ($c) {
	my $query = $c->param('query') || '';

	my @int_ids = $c->db->selectall_array(
		"SELECT DISTINCT int_id FROM log WHERE address LIKE ? ORDER BY int_id, created LIMIT 101",
		undef,
		sprintf( '%%%s%%', $query ),
	);
	my $sql = sprintf(
		"
			SELECT created, str, int_id FROM log WHERE int_id IN (%s)
			UNION ALL SELECT created, str, int_id FROM message WHERE int_id IN (%s)
			ORDER BY int_id, created LIMIT 101
		",
		( "'" . join( "','", map { $_->[0] } @int_ids ) . "'" ) x 2,
	);
	my @rows = $c->db->selectall_array($sql);
	my $is_exceeded = @rows > 100 ? 1 : 0;
	@rows = @rows[ 0 .. 99 ];
	$c->render( template => 'search', rows => \@rows, query => $query, is_exceeded => $is_exceeded );
};

get '/' => sub ($c) {
	$c->render(template => 'index');
};

app->start;
__DATA__

@@ load.html.ep
% layout 'default';
<span>Loaded messages - <%= $messages %>, logs - <%= $logs %>, total - <%= $total %></span>

@@ search.html.ep
% layout 'default';
<form action="">
<input name=query type=text placeholder="address" value="<%= $query %>">
<input type="submit">
</form>
% if ( ref $rows eq 'ARRAY' ) {
	% if ( $is_exceeded ) {
	<br><b>More than 100</b>
	% }
	<div>
		% for my $row ( @$rows ) {
		<p><%= $row->[0] %> <%= $row->[1] %></p>
		% }
	</div>
% }

@@ index.html.ep
% layout 'default';
<a href="/load">Load data</a>
<br>
<a href="/search">Search by address</a>

@@ layouts/default.html.ep
<!DOCTYPE html>
<html>
  <head><title><%= title %></title></head>
  <body><%= content %></body>
</html>
