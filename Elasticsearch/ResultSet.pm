package DBIx::Class::Elasticsearch::ResultSet;
use strict;
use warnings;
use base qw(DBIx::Class::Elasticsearch);

sub url {
    my $self = shift;

    my $url = $self->next::method;

    return $url . '_bulk';
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
        date    => { type => "date" },
        text    => { type => "string", index => "analyzed", store => "yes", "term_vector" => "with_positions_offsets" },
        integer => { type => "integer", index => "not_analyzed", store => "yes" },
        float   => { type => "float",   index => "not_analyzed", store => "yes" },

    };

    for my $field (@$fields) {

        my $column_info = $source->column_info($field);

        $mapping->{$field} = $type_translations->{ $column_info->{data_type} } if $column_info->{data_type};
    }

}

1;
