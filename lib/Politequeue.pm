package Politequeue;

use strict; use warnings;
use Carp qw(croak confess);
use DBI;
use Time::HiRes qw(time);
use Class::Tiny qw(
    conn
    maxsize
    table
    pop_func
    sqlite_cache_size_bytes
    queue_name
);
use constant {
    MESSAGE_STATUS_READY => 0,
    MESSAGE_STATUS_LOCKED => 1,
    MESSAGE_STATUS_DONE => 2,
    MESSAGE_STATUS_FAILED => 3,
};

# ABSTRACT: Politequeue - A lightweight persistent queue using SQLite

my ($_last_uuid7_time, $_last_uuid7_seq);

sub _uuid7 {
    my $ns = int(time() * 1_000_000_000);

    if (defined $_last_uuid7_time && $ns == $_last_uuid7_time) {
        $_last_uuid7_seq++;
    } else {
        $_last_uuid7_time = $ns;
        $_last_uuid7_seq = 0;
    }

    my $time_high = int($ns / (2**32));
    my $time_low = $ns % (2**32);

    my $unix_ts_ms = int($ns / 1_000_000);
    my $rand_a = $_last_uuid7_seq;

    my $urandom;
    if (open(my $fh, '<', '/dev/urandom')) {
        binmode $fh;
        read $fh, $urandom, 6;
        close $fh;
    } else {
        $urandom = pack('Nn', rand(2**32), rand(2**16));
    }

    my ($rand_b) = unpack('H*', $urandom);

    my $time_high_hex = sprintf('%08x', $unix_ts_ms >> 16);
    my $time_mid_hex = sprintf('%04x', $unix_ts_ms & 0xffff);
    my $time_low_and_version_hex = sprintf('%04x', (0x7000 | (int(($ns % 1_000_000) / 1000) & 0x0fff)));
    my $clock_seq_and_reserved_and_node_hex = sprintf('%04x', (0x8000 | ($rand_a & 0x3fff)));

    return lc(join('-', $time_high_hex, $time_mid_hex, $time_low_and_version_hex, $clock_seq_and_reserved_and_node_hex, $rand_b));
}

sub _validate_table_name {
    my ($name) = @_;
    if ($name =~ /[]['`"]/) {
        croak "Invalid table name: $name";
    }
    return $name;
}

sub BUILDARGS {
    my $class = shift;
    if (@_ == 1 && !ref($_[0])) {
        return { db => $_[0] };
    } else {
        return { @_ };
    }
}

sub BUILD {
    my ($self, $args) = @_;

    my $db = $args->{db} || $args->{filename_or_conn};
    my $memory = $args->{memory};
    $self->{maxsize} = $args->{maxsize};
    $self->{queue_name} = $args->{queue_name} || 'Queue';
    $self->{sqlite_cache_size_bytes} = $args->{sqlite_cache_size_bytes} || 256_000;

    croak "Either specify a db or pass memory=True"
        unless (defined $db && !$memory) || (!defined $db && $memory);

    croak "sqlite_cache_size_bytes must be > 0" unless $self->{sqlite_cache_size_bytes} > 0;
    my $cache_n = -1 * int($self->{sqlite_cache_size_bytes} / 1024);

    my $dbh;
    if ($memory || (defined $db && $db eq ':memory:')) {
        $dbh = DBI->connect("dbi:SQLite:dbname=:memory:", "", "", { RaiseError => 1, AutoCommit => 1 });
    } elsif (ref $db eq 'DBI::db') {
        $dbh = $db;
        $dbh->{AutoCommit} = 1;
    } else {
        $dbh = DBI->connect("dbi:SQLite:dbname=$db", "", "", { RaiseError => 1, AutoCommit => 1 });
    }

    $self->{conn} = $dbh;
    $self->{table} = '[' . _validate_table_name($self->{queue_name}) . ']';

    $self->{pop_func} = $self->_select_pop_func();

    $self->{conn}->begin_work;
    eval {
        $self->{conn}->do(<<"SQL");
            CREATE TABLE IF NOT EXISTS $self->{table}
            (
              data       TEXT NOT NULL,
              message_id TEXT NOT NULL,
              status     INTEGER NOT NULL,
              in_time    INTEGER NOT NULL,
              lock_time  INTEGER,
              done_time  INTEGER
            )
SQL
        $self->{conn}->do("CREATE INDEX IF NOT EXISTS TIdx ON $self->{table}(message_id)");
        $self->{conn}->do("CREATE INDEX IF NOT EXISTS SIdx ON $self->{table}(status)");
        $self->{conn}->commit;
    };
    if ($@) {
        $self->{conn}->rollback;
        confess $@;
    }

    $self->{conn}->do("PRAGMA journal_mode = WAL;");
    $self->{conn}->do("PRAGMA temp_store = MEMORY;");
    $self->{conn}->do("PRAGMA synchronous = NORMAL;");
    $self->{conn}->do("PRAGMA cache_size = $cache_n;");

    if (defined $self->{maxsize}) {
        my $validated_name = _validate_table_name($self->{queue_name});
        $self->{conn}->do(<<"SQL");
CREATE TRIGGER IF NOT EXISTS maxsize_control_${validated_name}
   BEFORE INSERT
   ON $self->{table}
   WHEN (SELECT COUNT(*) FROM $self->{table} WHERE status = @{[MESSAGE_STATUS_READY]}) >= $self->{maxsize}
BEGIN
    SELECT RAISE (ABORT,'Max queue length reached: $self->{maxsize}');
END;
SQL
    }
}

sub get_sqlite_version {
    my ($self) = @_;
    my $version = $self->{conn}->{sqlite_version};
    my ($major, $minor, $patch) = split /\./, $version;
    return $minor;
}

sub _select_pop_func {
    my ($self) = @_;
    my $v = $self->get_sqlite_version();
    return $v >= 35 ? \&_pop_returning : \&_pop_transaction;
}

sub put {
    my ($self, $data) = @_;
    my $message_id = _uuid7();
    my $now = int(time() * 1_000_000_000);

    my $sth = $self->{conn}->prepare(<<"SQL");
        INSERT INTO $self->{table}
               (data, message_id, status, in_time, lock_time, done_time)
        VALUES (?, ?, @{[MESSAGE_STATUS_READY]}, ?, NULL, NULL)
SQL
    $sth->execute($data, $message_id, $now);

    return Politequeue::Message->new(
        data => $data,
        message_id => $message_id,
        status => MESSAGE_STATUS_READY,
        in_time => $now,
        lock_time => undef,
        done_time => undef,
    );
}

sub pop {
    my ($self) = @_;
    return $self->{pop_func}->($self);
}

sub _pop_returning {
    my ($self) = @_;
    my $now = int(time() * 1_000_000_000);
    my $message;
    $self->{conn}->begin_work;
    eval {
        my $sth = $self->{conn}->prepare(<<"SQL");
            UPDATE $self->{table}
            SET status = @{[MESSAGE_STATUS_LOCKED]}, lock_time = ?
            WHERE rowid = (SELECT rowid
                           FROM $self->{table}
                           WHERE status = @{[MESSAGE_STATUS_READY]}
                           ORDER BY message_id
                           LIMIT 1)
            RETURNING *
SQL
        $sth->execute($now);
        $message = $sth->fetchrow_hashref;
        $self->{conn}->commit;
    };
    if ($@) {
        $self->{conn}->rollback;
        confess $@;
    }
    if ($message) {
        return Politequeue::Message->new(%$message);
    }
    return undef;
}

sub _pop_transaction {
    my ($self) = @_;
    my $message;
    $self->{conn}->begin_work;
    eval {
        my $sth = $self->{conn}->prepare(<<"SQL");
            SELECT * FROM $self->{table}
            WHERE rowid = (SELECT rowid
                           FROM $self->{table}
                           WHERE status = @{[MESSAGE_STATUS_READY]}
                           ORDER BY message_id
                           LIMIT 1)
SQL
        $sth->execute;
        $message = $sth->fetchrow_hashref;

        if ($message) {
            my $lock_time = int(time() * 1_000_000_000);
            my $upd_sth = $self->{conn}->prepare(<<"SQL");
                UPDATE $self->{table} SET
                  status = @{[MESSAGE_STATUS_LOCKED]},
                  lock_time = ?
                WHERE message_id = ? AND status = @{[MESSAGE_STATUS_READY]}
SQL
            $upd_sth->execute($lock_time, $message->{message_id});
            $message->{status} = MESSAGE_STATUS_LOCKED;
            $message->{lock_time} = $lock_time;
        }
        $self->{conn}->commit;
    };
    if ($@) {
        $self->{conn}->rollback;
        die $@;
    }
    if ($message) {
        return Politequeue::Message->new(%$message);
    }
    return undef;
}

sub peek {
    my ($self) = @_;
    my $sth = $self->{conn}->prepare("SELECT * FROM $self->{table} WHERE status = ? ORDER BY message_id LIMIT 1");
    $sth->execute(MESSAGE_STATUS_READY);
    my $value = $sth->fetchrow_hashref;
    if ($value) {
        return Politequeue::Message->new(%$value);
    }
    return undef;
}

sub get {
    my ($self, $message_id) = @_;
    my $sth = $self->{conn}->prepare("SELECT * FROM $self->{table} WHERE message_id = ?");
    $sth->execute($message_id);
    my $value = $sth->fetchrow_hashref;
    if ($value) {
        return Politequeue::Message->new(%$value);
    }
    return undef;
}

sub done {
    my ($self, $message_id) = @_;
    my $now = int(time() * 1_000_000_000);
    my $sth = $self->{conn}->prepare("UPDATE $self->{table} SET status = ?, done_time = ? WHERE message_id = ?");
    return $sth->execute(MESSAGE_STATUS_DONE, $now, $message_id);
}

sub mark_failed {
    my ($self, $message_id) = @_;
    my $now = int(time() * 1_000_000_000);
    my $sth = $self->{conn}->prepare("UPDATE $self->{table} SET status = ?, done_time = ? WHERE message_id = ?");
    return $sth->execute(MESSAGE_STATUS_FAILED, $now, $message_id);
}

sub list_locked {
    my ($self, $threshold_seconds) = @_;
    my $threshold_nanoseconds = $threshold_seconds * 1e9;
    my $time_value = int(time() * 1_000_000_000) - $threshold_nanoseconds;
    my $sth = $self->{conn}->prepare("SELECT * FROM $self->{table} WHERE status = ? AND lock_time < ?");
    $sth->execute(MESSAGE_STATUS_LOCKED, $time_value);
    my @results;
    while (my $row = $sth->fetchrow_hashref) {
        push @results, Politequeue::Message->new(%$row);
    }
    return \@results;
}

sub list_failed {
    my ($self) = @_;
    my $sth = $self->{conn}->prepare("SELECT * FROM $self->{table} WHERE status = ?");
    $sth->execute(MESSAGE_STATUS_FAILED);
    my @results;
    while (my $row = $sth->fetchrow_hashref) {
        push @results, Politequeue::Message->new(%$row);
    }
    return \@results;
}

sub retry {
    my ($self, $message_id) = @_;
    my $sth = $self->{conn}->prepare("UPDATE $self->{table} SET status = ?, done_time = NULL WHERE message_id = ?");
    return $sth->execute(MESSAGE_STATUS_READY, $message_id);
}

sub qsize {
    my ($self) = @_;
    my $sth = $self->{conn}->prepare("SELECT COUNT(*) FROM $self->{table} WHERE status NOT IN (?, ?)");
    $sth->execute(MESSAGE_STATUS_DONE, MESSAGE_STATUS_FAILED);
    return $sth->fetchrow_arrayref->[0];
}

sub empty {
    my ($self) = @_;
    my $sth = $self->{conn}->prepare("SELECT COUNT(*) as cnt FROM $self->{table} WHERE status = ?");
    $sth->execute(MESSAGE_STATUS_READY);
    return !$sth->fetchrow_hashref->{cnt};
}

sub full {
    my ($self) = @_;
    return 0 unless defined $self->{maxsize};
    my $sth = $self->{conn}->prepare("SELECT COUNT(*) as cnt FROM $self->{table} WHERE status = ?");
    $sth->execute(MESSAGE_STATUS_READY);
    return $sth->fetchrow_hashref->{cnt} >= $self->{maxsize};
}

sub prune {
    my ($self, $include_failed) = @_;
    $include_failed = 1 if !defined $include_failed;
    if ($include_failed) {
        $self->{conn}->do("DELETE FROM $self->{table} WHERE status IN (?, ?)", undef, MESSAGE_STATUS_DONE, MESSAGE_STATUS_FAILED);
    } else {
        $self->{conn}->do("DELETE FROM $self->{table} WHERE status IN (?)", undef, MESSAGE_STATUS_DONE);
    }
}

sub vacuum {
    my ($self) = @_;
    $self->{conn}->do("VACUUM;");
}

sub close {
    my ($self) = @_;
    $self->{conn}->disconnect;
}

package Politequeue::Message;

use strict;
use warnings;
use Class::Tiny qw(data message_id status in_time lock_time done_time);

use overload '""' => sub {
    my $self = shift;
    require Data::Dumper;
    return Data::Dumper::Dumper($self);
};

1;
