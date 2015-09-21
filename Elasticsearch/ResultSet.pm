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
    my ($data, $rows) = ([], 0);

    return unless $self->has_searchable; 
    
    my @fields = $self->searchable_fields;
    my $results = $self->search(undef, { select => \@fields });

    my $count = $results->count;

    while (my $row = $results->next) {
        $rows++;

        my %result = $row->get_columns;

        $result{es_id} = $row->es_id;

        push(@$data, \%result);
        if ($rows == $batch_size || $rows == $count) {
            warn "Batched $rows rows\n";
            $self->es_bulk($data);

            ($data, $rows) = ([], 0);
        }
    }

    1;
}

1;
