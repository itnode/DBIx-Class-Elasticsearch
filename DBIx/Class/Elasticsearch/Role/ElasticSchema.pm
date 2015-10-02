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
    default  => sub { host => "localhost", port => 9200, cxn => undef, debug => 0 },
);

sub es {

    my ($self) = @_;

    if ( !$self->es_store ) {

        my $settings = $self->connect_elasticsearch;

        my $debug = {};

        if ( $settings->{debug} ) {
            $debug->{trace_to} = 'Stderr';

            eval "use Log::Any::Adapter qw(Stderr);";
        }

        $self->es_store( Search::Elasticsearch->new( nodes => sprintf( '%s:%s', $settings->{host}, $settings->{port} ) ), log_to => 'Stderr', cxn => $settings->{cxn}, %$debug );

    }

    return $self->es_store;
}

sub es_dispatch {

    my $self = shift;
    my $class = shift;

    return unless $class;

    return $self->dispatcher->{$class};
}

sub es_index_name {

    my $self = shift;
    return $self->connect_elasticsearch->{index} || ref $self;
}
=head2
sub es_index_all {
    my $self = shift;

    foreach my $source ( $self->sources ) {

        my $rs          = $self->resultset($source);
        my $source_info = $rs->result_source->source_info;

        next unless $source_info && $source_info->{es_index_type} eq 'primary';

        my $klass = $self->class($source);

        if ( $self->resultset($source)->can("es_batch_index") ) {
            warn "Indexing source $source\n";
            $self->resultset($source)->es_batch_index;
        }
    }

}
=cut

sub es_index_all {

    my $self = shift;

    my $registered_elastic_rs = $self->connect_elasticsearch->{registered_elastic_rs};

    foreach my $rs ( @$registered_elastic_rs ) {

        eval "use $rs";

        warn $@ if $@;

        $rs->es_batch_index;
    }

}

sub es_create_index {

    my $self = shift;

    $self->es->indices->create( index => $self->es_index_name, );
}

=head2
sub es_create_mapping {

    my $self = shift;

    my $mappings = {};
    my @sources  = $self->sources;

    for my $source (@sources) {

        my $rs          = $self->resultset($source);
        my $source_info = $rs->result_source->source_info;

        next unless $source_info && $source_info->{es_index_type} eq 'primary';

        next unless $rs->can('es_has_searchable') && $rs->es_has_searchable;

        my $name = $rs->result_source->name;
        $mappings->{$name} = $rs->es_mapping;
    }

    for my $key ( keys %$mappings ) {

        my $parent = $mappings->{$key}{_parent} ? { _parent => delete $mappings->{$key}{"_parent"} } : {};

        $self->es->indices->put_mapping(
            index => $self->es_index_name,
            type  => $key,
            body  => { $key => { properties => $mappings->{$key}, dynamic => 0, %$parent } },
        );
    }
}
=cut

sub es_collect_mappings {

    my ( $self ) = @_;

    my $registered_elastic_rs = $self->connect_elasticsearch->{registered_elastic_rs};

    foreach my $rs (@$registered_elastic_rs) {

        eval "use $rs";

        warn $@ if $@;

        $self->es->indices->create( index => $rs->type );

        my $mapping = $rs->mapping;

        $self->es->indices->put_mapping(
            index => $rs->type,
            type => $rs->type,
            body => $rs->mapping,

        );
    }

}

sub es_drop_mapping {

    my $self = shift;

    my $types   = [];
    my @sources = $self->sources;

    for my $source (@sources) {

        my $rs          = $self->resultset($source);
        my $source_info = $rs->result_source->source_info;

        next unless $source_info && $source_info->{es_index_type} eq 'primary';

        next unless $rs->can('es_has_searchable') && $rs->es_has_searchable;

        warn "delete mapping $source";

        $self->es->indices->delete_mapping(
            index  => $self->es_index_name,
            type   => $source,
            ignore => 404,
        );
    }

}

1;
