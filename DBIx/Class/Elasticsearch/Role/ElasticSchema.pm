package DBIx::Class::Elasticsearch::Role::ElasticSchema;

use strict;
use warnings;

use Moose::Role;

has es_store => (
    is  => 'rw',
    isa => 'Object'
);

has connect_elasticsearch => (
    is       => 'rw',
    isa      => 'HashRef',
    required => 0,
    default  => sub { host => "localhost", port => 9200 },
);

sub es {

    my ($self) = @_;

    my $settings = $self->connect_elasticsearch;

    #use Log::Any::Adapter qw(Stderr);

    $self->es_store( Search::Elasticsearch->new( nodes => sprintf( '%s:%s', $settings->{host}, $settings->{port} ) ), trace_to => 'Stderr', log_to => 'Stderr' ) unless $self->es_store;

    return $self->es_store;
}

sub es_index_name {

    my $self = shift;
    return $self->connect_elasticsearch->{index} || ref $self;
}

sub es_index_all {
    my $self = shift;

    foreach my $source ( $self->sources ) {
        my $klass = $self->class($source);

        if ( $self->resultset($source)->can("batch_index") ) {
            warn "Indexing source $source\n";
            $self->resultset($source)->batch_index;
        }
    }
}

sub es_create_index {

    my $self = shift;

    $self->es->indices->create( index => $self->es_index_name, );
}

sub es_create_mapping {

    my $self = shift;

    my $mappings = {};
    my @sources  = $self->sources;

    for my $source (@sources) {

        next unless $source->source_info->{es_index_type} eq 'primary';

        my $rs = $self->resultset($source);

        next unless $rs->can('es_has_searchable') && $rs->es_has_searchable;

        my $name = $rs->result_source->name;
        $mappings->{$name} = $rs->es_mapping;
    }

    for my $key ( keys %$mappings ) {

        $self->es->indices->put_mapping(
            index => $self->es_index_name,
            type  => $key,
            body  => { $key => { properties => $mappings->{$key} } },
        );
    }
}

sub es_drop_mapping {

    my $self = shift;

    my $types   = [];
    my @sources = $self->sources;

    for my $source (@sources) {

        next unless $source->source_info->{es_index_type} eq 'primary';

        my $rs = $self->resultset($source);

        next unless $rs->can('es_has_searchable') && $rs->es_has_searchable;

        $self->es->indices->delete_mapping(
            index  => $self->es_index_name,
            type   => $source,
            ignore => 404,
        );
    }

}

sub es_dump_mappings {

    my $self = shift;

    my @sources = $self->sources;

    @sources = grep { $self->resultset($_)->can('es_has_searchable') && $self->resultset($_)->es_has_searchable } @sources;

    warn "Listing sources";
    use DDP;
    p @sources;

    my $mappings = $self->es->indices->get_mapping(
        index => $self->es_index_name,
        type  => \@sources,
    );

    warn "Mapping";
    p $mappings;
}

1;
