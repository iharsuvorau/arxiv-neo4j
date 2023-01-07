import itertools
from ast import literal_eval
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


def save_str_to_file(data: str, file_path: Path | str) -> None:
    with open(file_path, 'w') as f:
        f.write(data)


def save_df_to_file(
        df: pd.DataFrame,
        file_path: Path | str,
        header: bool = False,
        columns: Optional[Iterable] = None,
) -> None:
    df.to_csv(file_path, index=False, header=header, columns=columns if columns else df.columns)


def infer_separator(file_path: Path) -> str:
    return '\t' if file_path.suffix == '.tsv' else ','


def process_venue_entities(venues_path: Path, output_dir: Path) -> None:
    df = pd.read_csv(venues_path, index_col=0)

    df = df[['venue_ID', 'full_name', 'h_index_calculated']]

    df.rename(columns={
        'venue_ID': 'venue_ID:ID(Venue-ID)',
        'h_index_calculated': 'h_index_calculated:int',
    }, inplace=True)

    df[':LABEL'] = 'Venue'

    header_path = output_dir / 'venues_header.csv'
    content_path = output_dir / 'venues.csv'

    header = 'venue_ID:ID(Venue-ID),full_name,h_index_calculated:int,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'venue_ID:ID(Venue-ID)',
        'full_name',
        'h_index_calculated:int',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_author_entities(authors_path: Path, output_dir: Path):
    df = pd.read_csv(authors_path, index_col=0)

    df = df[['author_ID', 'full_name', 'h_idex_real', 'h_idex_calculated']]

    df.rename(columns={
        'author_ID': 'author_ID:ID(Author-ID)',
        'h_idex_real': 'h_index_real:int',
        'h_idex_calculated': 'h_index_calculated:int',
    }, inplace=True)

    df['h_index_real:int'] = df['h_index_real:int'].fillna(-1).astype(int)

    df[':LABEL'] = 'Author'

    header_path = output_dir / 'authors_header.csv'
    content_path = output_dir / 'authors.csv'

    header = 'author_ID:ID(Author-ID),full_name,h_index_real:int,h_index_calculated:int,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'author_ID:ID(Author-ID)',
        'full_name',
        'h_index_real:int',
        'h_index_calculated:int',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_affiliation_entities(affiliations_path: Path, output_dir: Path):
    df = pd.read_csv(affiliations_path, index_col=0)

    df = df[['affiliation_ID', 'institution_name', 'institution_place']]

    df['affiliation_ID'] = df['affiliation_ID'].astype(int)

    df.rename(columns={
        'affiliation_ID': 'affiliation_ID:ID(Affiliation-ID)',
        'institution_name': 'name',
        'institution_place': 'place',
    }, inplace=True)

    df[':LABEL'] = 'Affiliation'

    header_path = output_dir / 'affiliations_header.csv'
    content_path = output_dir / 'affiliations.csv'

    header = 'affiliation_ID:ID(Affiliation-ID),name,place,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'affiliation_ID:ID(Affiliation-ID)',
        'name',
        'place',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_publication_entities(
        publications_path: Path,
        publications_to_venues_path: Path,
        venues_path: Path,
        output_dir: Path,
):
    df = pd.read_csv(publications_path, index_col=0)

    df = df[['publication_ID', 'title', 'DOI', 'date']]

    df['title'] = df['title'].apply(lambda x: x.replace('\n', '').strip())
    df['title'] = df['title'].apply(lambda x: x.replace('  ', ' ').strip())

    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['date'] = df['date'].dt.strftime('%Y')

    publications_to_venues_df = pd.read_csv(publications_to_venues_path, index_col=0)[['publication_ID', 'venue_ID']]

    df = df.merge(publications_to_venues_df, left_on='publication_ID', right_on='publication_ID', how='left')

    venues_df = pd.read_csv(venues_path, index_col=0)[['venue_ID', 'full_name']]

    df = df.merge(venues_df, left_on='venue_ID', right_on='venue_ID', how='left')

    df = df.drop_duplicates(subset=['publication_ID'])

    df.rename(columns={
        'publication_ID': 'publication_ID:ID(Publication-ID)',
        'full_name': 'venue',
        'DOI': 'doi',
        'date': 'year:int',
    }, inplace=True)

    df[':LABEL'] = 'Publication'

    header_path = output_dir / 'publications_header.csv'
    content_path = output_dir / 'publications.csv'

    header = 'publication_ID:ID(Publication-ID),title,doi,year:int,venue,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'publication_ID:ID(Publication-ID)',
        'title',
        'doi',
        'year:int',
        'venue',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_scientific_domain_entities(domains_path: Path, output_dir: Path):
    df = pd.read_csv(domains_path)

    df = df[['arxiv_category', 'major_field', 'sub_category', 'exact_category']]

    df.rename(columns={
        'arxiv_category': 'arxiv_category:ID(Arxiv-Category-ID)',
    }, inplace=True)

    df[':LABEL'] = 'ScientificDomain'

    # TODO: we may want to reconsider dropping duplicates and create multiple references instead
    df = df.drop_duplicates(subset=['arxiv_category:ID(Arxiv-Category-ID)'])

    header_path = output_dir / 'domains_header.csv'
    content_path = output_dir / 'domains.csv'

    header = 'arxiv_category:ID(Arxiv-Category-ID),major_field,sub_category,exact_category,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'arxiv_category:ID(Arxiv-Category-ID)',
        'major_field',
        'sub_category',
        'exact_category',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_author_of_relationships(df: pd.DataFrame, output_dir: Path):
    df = df[['author_ID', 'publication_ID']]

    df = df.rename(columns={
        'author_ID': ':START_ID(Author-ID)',
        'publication_ID': ':END_ID(Publication-ID)',
    })

    df[':TYPE'] = 'AUTHOR_OF'

    header_path = output_dir / 'author_of_header.csv'
    content_path = output_dir / 'author_of.csv'

    header = ':START_ID(Author-ID),:END_ID(Publication-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Author-ID)',
        ':END_ID(Publication-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_author_collaborates_with_relationships(df: pd.DataFrame, output_dir: Path):
    df = df[['author_ID', 'publication_ID']]

    result = pd.DataFrame(columns=['author_ID_1', 'author_ID_2'])

    for publication_id, publication_df in df.groupby('publication_ID'):
        author_ids = publication_df['author_ID'].values

        author_ids_permutations = list(itertools.permutations(author_ids, 2))

        result = pd.concat([result, pd.DataFrame(author_ids_permutations, columns=['author_ID_1', 'author_ID_2'])])

    result = result.rename(columns={
        'author_ID_1': ':START_ID(Author-ID)',
        'author_ID_2': ':END_ID(Author-ID)',
    })

    result[':TYPE'] = 'COLLABORATES_WITH'

    header_path = output_dir / 'author_collaborates_with_header.csv'
    content_path = output_dir / 'author_collaborates_with.csv'

    header = ':START_ID(Author-ID),:END_ID(Author-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Author-ID)',
        ':END_ID(Author-ID)',
        ':TYPE',
    ]
    save_df_to_file(result, content_path, columns=columns)


def process_author_works_at_relationships(author_to_affiliations_path: Path, output_dir: Path):
    df = pd.read_csv(author_to_affiliations_path, index_col=0)

    df = df[['author_ID', 'affiliation_ID']]

    df = df.rename(columns={
        'author_ID': ':START_ID(Author-ID)',
        'affiliation_ID': ':END_ID(Affiliation-ID)',
    })

    df[':TYPE'] = 'WORKS_AT'

    header_path = output_dir / 'works_at_header.csv'
    content_path = output_dir / 'works_at.csv'

    header = ':START_ID(Author-ID),:END_ID(Affiliation-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Author-ID)',
        ':END_ID(Affiliation-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_publication_published_in_relationships(publications_to_venues_path: Path, output_dir: Path):
    df = pd.read_csv(publications_to_venues_path, index_col=0)

    df = df[['publication_ID', 'venue_ID']]

    df = df.rename(columns={
        'publication_ID': ':START_ID(Publication-ID)',
        'venue_ID': ':END_ID(Venue-ID)',
    })

    df[':TYPE'] = 'PUBLISHED_IN'

    header_path = output_dir / 'published_in_header.csv'
    content_path = output_dir / 'published_in.csv'

    header = ':START_ID(Publication-ID),:END_ID(Venue-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Publication-ID)',
        ':END_ID(Venue-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_publication_belongs_to_domain_relationships(
        publications_to_domains_path: Path,
        publications_path: Path,
        arxiv_categories_path: Path,
        domains_path: Path,
        output_dir: Path
):
    pub_to_domains_df = pd.read_csv(publications_to_domains_path, sep='\t')  # publication_ID, arxiv_category_ID
    arxiv_categories_df = pd.read_csv(arxiv_categories_path)  # arxiv_category_ID, arxiv_category
    domains_df = pd.read_csv(
        domains_path)  # domain_id, grouping_id, major_field, sub_category, exact_category, arxiv_category
    pub_df = pd.read_csv(publications_path, index_col=0)[['publication_ID']]

    pub_to_domains_df = pub_to_domains_df.merge(arxiv_categories_df, on='arxiv_category_ID')
    pub_to_domains_df = pub_to_domains_df.merge(domains_df, on='arxiv_category')

    pub_to_domains_df = pub_to_domains_df[['publication_ID', 'arxiv_category']]

    # Removing publications with IDs that are not in the publications.csv file, i.e., not in the database
    pub_to_domains_df = pub_to_domains_df.merge(pub_df, on='publication_ID')

    pub_to_domains_df = pub_to_domains_df.rename(columns={
        'publication_ID': ':START_ID(Publication-ID)',
        'arxiv_category': ':END_ID(Arxiv-Category-ID)',
    })

    pub_to_domains_df[':TYPE'] = 'BELONGS_TO'

    header_path = output_dir / 'belongs_to_header.csv'
    content_path = output_dir / 'belongs_to.csv'

    header = ':START_ID(Publication-ID),:END_ID(Arxiv-Category-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Publication-ID)',
        ':END_ID(Arxiv-Category-ID)',
        ':TYPE',
    ]
    save_df_to_file(pub_to_domains_df, content_path, columns=columns)


def process_publication_cited_by_relationships(citations_path: Path, publications_path: Path, output_dir: Path):
    sep = '\t' if citations_path.suffix == '.tsv' else ','
    df = pd.read_csv(citations_path, sep=sep)  # publication_ID, citing_publication_DOI (array)

    df['citing_publication_DOI'] = df['citing_publication_DOI'].apply(literal_eval)
    df = df.explode('citing_publication_DOI')

    pub_df = pd.read_csv(publications_path, index_col=0)[['publication_ID', 'DOI']]

    # Removing publications that are not in the publications.csv file
    df = df.merge(pub_df, left_on='citing_publication_DOI', right_on='DOI')

    df = df[['publication_ID_x', 'citing_publication_DOI']]

    df = df.rename(columns={
        'publication_ID_x': ':START_ID(Publication-ID)',
        'citing_publication_DOI': ':END_ID(Publication-ID)',
    })

    df[':TYPE'] = 'CITED_BY'

    header_path = output_dir / 'cited_by_header.csv'
    content_path = output_dir / 'cited_by.csv'

    header = ':START_ID(Publication-ID),:END_ID(Publication-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Publication-ID)',
        ':END_ID(Publication-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_affiliation_covers_scientific_domain_relationships(
        publications_to_affiliations_path: Path,
        publications_to_domains_path: Path,
        output_dir: Path
):
    # pub2affiliation_ID, publication_ID, affiliation_ID
    publications_to_affiliations_df = pd.read_csv(
        publications_to_affiliations_path, sep=infer_separator(publications_to_affiliations_path), index_col=0)
    publications_to_affiliations_df = publications_to_affiliations_df[['publication_ID', 'affiliation_ID']]

    # publication_ID, arxiv_category_ID
    publications_to_domains_df = pd.read_csv(
        publications_to_domains_path, sep=infer_separator(publications_to_domains_path))

    publications_to_affiliations_df = publications_to_affiliations_df.merge(
        publications_to_domains_df, on='publication_ID')

    df = publications_to_affiliations_df[['affiliation_ID', 'arxiv_category_ID']]
    df = df.drop_duplicates(['affiliation_ID', 'arxiv_category_ID'])

    df = df.rename(columns={
        'affiliation_ID': ':START_ID(Affiliation-ID)',
        'arxiv_category_ID': ':END_ID(Arxiv-Category-ID)',
    })

    df[':TYPE'] = 'COVERS'

    header_path = output_dir / 'covers_header.csv'
    content_path = output_dir / 'covers.csv'

    header = ':START_ID(Affiliation-ID),:END_ID(Arxiv-Category-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Affiliation-ID)',
        ':END_ID(Arxiv-Category-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_affiliation_collaborates_with_relationships(
        author_to_publications_path: Path,
        publications_to_affiliations_path: Path,
        output_dir: Path
):
    # author_ID, publication_ID
    author_to_publications_df = pd.read_csv(
        author_to_publications_path, sep=infer_separator(author_to_publications_path), index_col=0)
    author_to_publications_df = author_to_publications_df[['author_ID', 'publication_ID']]

    # publication_ID, affiliation_ID
    publications_to_affiliations_df = pd.read_csv(
        publications_to_affiliations_path, sep=infer_separator(publications_to_affiliations_path), index_col=0)
    publications_to_affiliations_df = publications_to_affiliations_df[['publication_ID', 'affiliation_ID']]

    # author_ID, publication_ID, affiliation_ID
    df = author_to_publications_df.merge(publications_to_affiliations_df, on='publication_ID')

    collaboration_df = pd.DataFrame(columns=[':START_ID(Affiliation-ID)', ':END_ID(Affiliation-ID)', ':TYPE'])

    for _, group in df.groupby('publication_ID'):
        collaborators = group['affiliation_ID'].unique()
        ids_permutations = list(itertools.permutations(collaborators, 2))
        rows = [[start, end, 'COLLABORATES_WITH'] for start, end in ids_permutations]
        collaboration_df = pd.concat([collaboration_df, pd.DataFrame(rows, columns=collaboration_df.columns)])

    collaboration_df = collaboration_df.drop_duplicates()

    header_path = output_dir / 'affiliation_collaborates_with_header.csv'
    content_path = output_dir / 'affiliation_collaborates_with.csv'

    header = ':START_ID(Affiliation-ID),:END_ID(Affiliation-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Affiliation-ID)',
        ':END_ID(Affiliation-ID)',
        ':TYPE',
    ]
    save_df_to_file(collaboration_df, content_path, columns=columns)


def process_affiliation_publishes_in_relationships(
        publications_to_affiliations_path: Path,
        publications_to_venues_path: Path,
        output_dir: Path
):
    # pub2affiliation_ID, publication_ID, affiliation_ID
    publications_to_affiliations_df = pd.read_csv(
        publications_to_affiliations_path, sep=infer_separator(publications_to_affiliations_path), index_col=0)
    publications_to_affiliations_df = publications_to_affiliations_df[['publication_ID', 'affiliation_ID']]

    # publication_ID, venue_ID
    publications_to_venues_df = pd.read_csv(
        publications_to_venues_path, sep=infer_separator(publications_to_venues_path))
    publications_to_venues_df = publications_to_venues_df[['publication_ID', 'venue_ID']]

    # publication_ID, affiliation_ID, venue_ID
    df = publications_to_affiliations_df.merge(publications_to_venues_df, on='publication_ID')

    # affiliation_ID, venue_ID
    df = df[['affiliation_ID', 'venue_ID']]
    df = df.drop_duplicates(['affiliation_ID', 'venue_ID'])

    df = df.rename(columns={
        'affiliation_ID': ':START_ID(Affiliation-ID)',
        'venue_ID': ':END_ID(Venue-ID)',
    })

    df[':TYPE'] = 'PUBLISHES_IN'

    header_path = output_dir / 'affiliation_publishes_in_header.csv'
    content_path = output_dir / 'affiliation_publishes_in.csv'

    header = ':START_ID(Affiliation-ID),:END_ID(Venue-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Affiliation-ID)',
        ':END_ID(Venue-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


if __name__ == '__main__':
    output_dir = Path('../import/enriched')
    output_dir.mkdir(exist_ok=True)

    venues_path = Path('../dataset/enriched/venues.csv')
    process_venue_entities(venues_path, output_dir)

    authors_path = Path('../dataset/enriched/authors.csv')
    process_author_entities(authors_path, output_dir)

    affiliations_path = Path('../dataset/enriched/affiliations.csv')
    process_affiliation_entities(affiliations_path, output_dir)

    publications_path = Path('../dataset/enriched/publications.csv')
    publications_to_venues_path = Path('../dataset/enriched/pub2venue_.csv')
    process_publication_entities(publications_path, publications_to_venues_path, venues_path, output_dir)

    domains_path = Path('../dataset/enriched/lookup_table_domains.csv')
    process_scientific_domain_entities(domains_path, output_dir)

    author_to_publications_path = Path('../dataset/enriched/author2pub.csv')
    a2p_df = pd.read_csv(author_to_publications_path, index_col=0)
    process_author_of_relationships(a2p_df, output_dir)
    process_author_collaborates_with_relationships(a2p_df, output_dir)

    author_to_affiliations_path = Path('../dataset/enriched/author2affiliation.csv')
    process_author_works_at_relationships(author_to_affiliations_path, output_dir)

    process_publication_published_in_relationships(publications_to_venues_path, output_dir)

    publications_to_domains_path = Path('../dataset/enriched/publication2arxiv_df.tsv')
    arxiv_categories_path = Path('../dataset/enriched/arxiv_categories.csv')
    process_publication_belongs_to_domain_relationships(
        publications_to_domains_path, publications_path, arxiv_categories_path, domains_path, output_dir)

    citations_path = Path('../dataset/enriched/citing_pub_df200000.tsv')
    process_publication_cited_by_relationships(citations_path, publications_path, output_dir)

    publications_to_affiliations_path = Path('../dataset/enriched/pub2affiliation.csv')
    process_affiliation_covers_scientific_domain_relationships(
        publications_to_affiliations_path, publications_to_domains_path, output_dir)

    process_affiliation_collaborates_with_relationships(
        author_to_publications_path, publications_to_affiliations_path, output_dir)

    process_affiliation_publishes_in_relationships(
        publications_to_affiliations_path, publications_to_venues_path, output_dir)
