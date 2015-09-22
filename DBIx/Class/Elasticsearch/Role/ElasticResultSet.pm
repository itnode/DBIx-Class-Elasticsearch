package DBIx::Class::Elasticsearch::Role::ElasticResultSet;

use strict;
use warnings;

use Moose;

with 'DBIx::Class::Elasticsearch::Role::ElasticBase';

sub has_searchable {
    my $self = shift;

    return scalar $self->searchable_fields;
}

sub searchable_fields {
    my $self = shift;

    my $class             = $self->result_class;
    my $cols              = $class->columns_info;
    my @searchable_fields = grep { $cols->{$_}->{searchable} } keys %{$cols};

    return @searchable_fields;
}


sub batch_index {
    warn "Batch Indexing...\n";
    my $self = shift;
    my $batch_size = shift || 1000;
    my ( $data, $rows ) = ( [], 0 );

    return unless $self->has_searchable;

    my @fields = $self->searchable_fields;
    my $results = $self->search( undef, { select => \@fields } );

    my $count = $results->count;

    while ( my $row = $results->next ) {
        $rows++;

        my %result = $row->get_columns;

        $result{es_id} = $row->es_id;

        push( @$data, \%result );
        if ( $rows == $batch_size || $rows == $count ) {
            warn "Batched $rows rows\n";
            $self->es_bulk($data);

            ( $data, $rows ) = ( [], 0 );
        }
    }

    1;
}

sub es_bulk {

    my ( $self, $data ) = @_;

    my $bulk = $self->es->bulk_helper;

    for my $row (@$data) {

        my $type = $self->result_source->name;

        my $params = {
            index  => $self->settings->{index},
            id     => sprintf( "%s_%s", $type, $row->{es_id} ),
            type   => $type,
            source => $row
        };

        $bulk->index($params);
    }

    $bulk->flush;
}

sub es_mapping {

    my ($self) = @_;

    my $source = $self->result_source;

    return unless $self->has_searchable;

    my @fields = $self->searchable_fields;

    my $mapping = {};

    my $type_translations = {

        varchar => { type => "string", index => "analyzed" },
        enum    => { type => "string", index => "not_analyzed", store => "yes" },
        char    => { type => "string", index => "not_analyzed", store => "yes" },
        date => { type => "date" },
        text => { type => "string", index => "analyzed", store => "yes", "term_vector" => "with_positions_offsets" },
        integer => { type => "integer", index => "not_analyzed", store => "yes" },
        float   => { type => "float",   index => "not_analyzed", store => "yes" },
        decimal => { type => "float",   index => "not_analyzed", store => "yes" },

    };

    for my $field (@fields) {

        my $column_info = $source->column_info($field);

        $mapping->{$field} = $type_translations->{ $column_info->{data_type} } if $column_info->{data_type};
    }

    return $mapping;

}

1;
