package DBIx::Class::Elasticsearch::Schema;

use strict;
use warnings;
use base qw(DBIx::Class::Elasticsearch);

sub index_all {
    my $self = shift;

    foreach my $source ( $self->sources ) {
        my $klass = $self->class($source);

        if ( $self->resultset($source)->can("batch_index") ) {
            warn "Indexing source $source\n";
            $self->resultset($source)->batch_index;
        }
    }
}

sub es_mapping {

    my $self = shift;

    my $mappings = {};
    my @sources  = $self->sources;

    for my $source (@sources) {

        my $rs = $self->resultset($source);

        next unless $rs->can('has_searchable') && $rs->has_searchable;

        $mappings->{ $rs->result_source->name } = $rs->es_mapping;
    }

    my $props = { properties => $mappings };

    use DDP;
    p $props;

    $self->es->indices->delete_mapping(
        index  => $self->settings->{index},
        type   => "item",
        ignore => 404,
    );

    $self->es->indices->put_mapping(
        index  => $self->settings->{index},
        type   => "item",
        ignore => 404,
    );
}

1;
