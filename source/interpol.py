import dlt
import os

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

base_url = "https://ws-public.interpol.int/notices/v1/un"

os.environ["EXTRACT__WORKERS"] = "2"

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

    def notices(notice_type: str = "persons"):
        """
        Fetches notices from Interpol.
        """
        for notices in client.paginate(notice_type):
            for notice in notices:
                yield notice

    def transform_notices(item):
        """
        Transforms the notices data.
        """
        path = item["_links"]["self"]["href"]
        path = path.replace(base_url, "")

        response = client.get(path)
        response.raise_for_status()

        yield response.json()

    for item in ['persons', 'entities']:
        yield dlt.resource(notices(item), name=f"resource_{item}") | dlt.transformer(transform_notices, name=item, parallelized=True)
