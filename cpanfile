requires 'DBI';
requires 'DBD::SQLite';
requires 'Class::Tiny';
requires 'Time::HiRes';

# Test dependencies
on 'test' => sub {
    requires 'Test::More';
};
