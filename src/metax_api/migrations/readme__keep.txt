migration files that have a suffix __keep will not be removed by the recreate-db
command.

during active development it is convenient to completely erase all migrations,
but some operations are best done in a migration file when the the application
is created first time.
