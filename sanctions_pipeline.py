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
            "Accept": "application/json",
        },
        paginator=JSONLinkPaginator(next_url_path="_links.next.href"),
    )

    @dlt.resource()
    def persons():
        """
        Fetches notices from Interpol.
        """
        for persons in client.paginate("persons"):
            for person in persons:
                yield person

    @dlt.transformer(primary_key="entity_id")
    def person(item):
        """
        Transforms the notices data.
        """
        path = item["_links"]["self"]["href"]
        path = path.replace(base_url, "")

        response = client.get(path).json()

        yield response

    # Returning the data as a dlt resource
    return persons | person


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="sanctions",
        destination="duckdb",
        dataset_name="interpol",
        progress=dlt.progress.tqdm(colour="yellow"),
    )

    load_info = pipeline.run(src_interpol())

    print(load_info)
