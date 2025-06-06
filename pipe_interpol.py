import dlt

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator


base_url = "https://ws-public.interpol.int/notices/v1/un"

@dlt.source()
def src_interpol():
    """
    Interpol source for dlt pipeline.
    """
    client = RESTClient(
        base_url=base_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "Accept": "application/json"
        },
        paginator=JSONLinkPaginator(next_url_path="_links.next.href"),
    )

    @dlt.resource(file_format="jsonl")
    def notices():
        """
        Fetches notices from Interpol.
        """
        for notices in client.paginate('persons'):
            for notice in notices:
                yield notice

    @dlt.transformer(primary_key="entity_id")
    def transform_notice(item):
        """
        Transforms the notices data.
        """
        path = item['_links']['self']['href']
        path = path.replace(base_url, '')
        
        response = client.get(path).json()

        yield response

    # Returning the data as a dlt resource
    return notices | transform_notice

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="interpol_pipeline",
        destination="duckdb",
        dataset_name="interpol_dataset",
    )

    load_info = pipeline.run(src_interpol())

    print(load_info)
