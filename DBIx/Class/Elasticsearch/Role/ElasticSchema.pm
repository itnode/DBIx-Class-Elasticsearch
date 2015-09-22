package DBIx::Class::Elasticsearch::Role::ElasticSchema;

use strict;
use warnings;

use Moose::Role;

with 'DBIx::Class::Elasticsearch::Role::ElasticBase';

sub es {

    my ($self) = @_;

    my $settings = $self->settings;

    $self->es_store( Search::Elasticsearch->new( nodes => sprintf( '%s:%s', $settings->{host}, $settings->{port} ) ) ) unless $self->es_store;

    return $self->es_store;
}

sub settings {
    my $self = shift;

    if ( !$self->settings_store ) {

        my $config = OASYS::Utils->config;
        $self->settings_store( $config->{elastic} );
    }

    return $self->settings_store;
}

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

        my $name = $rs->result_source->name;
        $mappings->{$name} = $rs->es_mapping;
        $mappings->{$name}{type} = 'multi_field';
    }

    my $props = { properties => $mappings };

    $self->es->indices->delete_mapping(
        index  => $self->settings->{index},
        type   => "item",
        ignore => 404,
    );

    $self->es->indices->put_mapping(
        index  => $self->settings->{index},
        type   => "item",
        body   => $props,
    );
}

sub es_dump_mappings {

    my $self = shift;

    my @sources = $self->sources;

    @sources = grep { $self->resultset($_)->can('has_searchable') && $self->resultset($_)->has_searchable } @sources;

    use DDP;
    p @sources;

    my $mappings = $self->es->indices->get_mapping(
        index => $self->settings->{index},
        type  => \@sources,
    );

    use DDP;
    p $mappings;
}

1;
