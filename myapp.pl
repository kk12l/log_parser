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
		$dbh->do("DELETE FROM message");
		$dbh->do("DELETE FROM log");
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
	my ( $dbh, $statement, @values ) = @_;

	my %queries = (
		message => 'INSERT INTO message (id, int_id, created, str) VALUES (?, ?, ?, ?)',
		log => 'INSERT INTO log (int_id, created, str, address) VALUES (?, ?, ?, ?)',
	);

	if ( $statement eq 'message' ) {
		my @ids = $dbh->selectall_array("SELECT id FROM message ORDER BY id DESC LIMIT 1");
		my $id = @ids ? $ids[0][0]+1 : 1;
		unshift @values, sprintf( '%.16d', $id );
	}

	my $sth = $dbh->prepare($queries{$statement});
	$sth->execute(@values);
}

get '/load' => sub ($c) {
	my ( $message_count, $log_count ) = (0, 0);

	open my $fh, '<', 'out';
	while (my $row = <$fh>) {
		chomp $row;
		my @portions = split ' ', $row;

		if ( $flags{ $portions[3] } && $flags{ $portions[3] } > 1 ) {
			send_db(
				$c->db,
				'message',
				# 2012-02-13 14:39:22 1RwtJa-0009RI-KL <= tpxmuwr@somehost.ru H=mail.somehost.com [84.154.134.45] P=esmtp S=1716 id=120213143628.BLOCKED.453962@whois.somehost.ru
				$portions[2], join( ' ', $portions[0], $portions[1] ), join( ' ', @portions[4..$#portions] )
			);
			$message_count++;
		} else {
			my @values;

			# Нет ID
			if ( length($portions[2]) != 16 ) {
				# 2012-02-13 15:00:55 SMTP connection from [109.70.26.4] (TCP/IP connection count = 1)
				@values = ( '', join(' ', $portions[0], $portions[1]), join(' ', @portions[2..$#portions]), '' );
			}
			# Есть флаг
			elsif ( $flags{ $portions[3] } ) {
				# 2012-02-13 15:00:55 1RwteR-000Om4-65 == psqgg@yandex.ru R=dnslookup T=remote_smtp defer (-1): domain matches queue_smtp_domains, or -odqs set
				@values = ( $portions[2], join(' ', $portions[0], $portions[1]), join(' ', @portions[5..$#portions]), $portions[4] );
			}
			# Есть ID, но нет флага
			else {
				# 2012-02-13 15:00:57 1RwtdI-0000Ac-TM Completed
				@values = ( $portions[2], join(' ', $portions[0], $portions[1]), join(' ', @portions[3..$#portions]), '' );
			}

			send_db( $c->db, 'log', @values);
			$log_count++;
		}
	}
	close $fh;

	$c->render(template => 'load', messages => $message_count, logs => $log_count );
};

get '/search' => sub ($c) {
	my $query = $c->param('query');

	my @messages = $c->db->selectall_array("SELECT created, str, int_id FROM message WHERE SUBSTRING_INDEX(str, ' ', 1) LIKE ? ORDER BY int_id, created LIMIT 100", undef, sprintf( '%%%s%%', $query));
	my @logs = $c->db->selectall_array("SELECT created, str, int_id FROM log WHERE address LIKE ? ORDER BY int_id, created LIMIT 100", undef, sprintf( '%%%s%%', $query));
	my @rows = sort { $a->[2] cmp $b->[2] || $a->[0] cmp $b->[0] } ( @messages, @logs );
	my $is_exceeded = @rows > 100 ? 1 : 0;
	@rows = @rows[0..99];
	$c->render(template => 'search', rows => \@rows, query => $query, is_exceeded => $is_exceeded);
};

get '/' => sub ($c) {
	$c->render(template => 'index');
};

app->start;
__DATA__

@@ load.html.ep
% layout 'default';
<span>Loaded messages - <%= $messages %>, logs - <%= $logs %>, total - <%= $messages + $logs %></span>

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
