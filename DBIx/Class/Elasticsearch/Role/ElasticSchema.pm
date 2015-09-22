package DBIx::Class::Elasticsearch::Role::ElasticSchema;

use strict;
use warnings;

use Moose;

with 'DBIx::Class::Elasticsearch::ElasticBase';

sub es {

    my ($self) = @_;

    my $settings = $self->settings;

    $self->es_store( Search::Elasticsearch->new( nodes => sprintf( '%s:%s', $settings->{host}, $settings->{port} ) ) ) unless $self->es_store;

    return $self->es_store;
}

sub settings {
    my $self = shift;
    my $path = dirname(__FILE__);

    if ( !$self->settings_store ) {
        my $yml = YAML::Syck::LoadFile("$path/elastic_search.yml");
        die "Could not load settings. elastic_search.yml not found" unless $yml;
        $self->settings_store($yml);
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
