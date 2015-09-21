package DBIx::Class::Elasticsearch::Schema;

use strict;
use warnings;
use base qw(DBIx::Class::Elasticsearch);

sub index_all {
    my $self = shift;

    foreach my $source ($self->sources) {
        my $klass = $self->class($source);

        if ($self->resultset($source)->can("batch_index")) {
            warn "Indexing source $source\n";
            $self->resultset($source)->batch_index;
        }
    }
}

1;
