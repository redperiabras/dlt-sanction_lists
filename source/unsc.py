import dlt
import xmltodict

from dlt.sources.helpers.rest_client import RESTClient

base_url = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"

@dlt.source(name="unsc")
def src_unsc():
    """
    United Nations Security Council Sanctions source for dlt pipeline.
    """
    client = RESTClient(
        base_url=base_url
    )

    @dlt.resource
    def consolidated_list():
        """
        Fetches the consolidated sanctions list from the UN Security Council.
        """
        response = client.get(base_url)
        response.raise_for_status()

        try:
            xml = response.content.decode("iso-8859-6")
        except UnicodeDecodeError:
            xml = response.content.decode("utf-8", errors="replace")
            print("Error decoding with detected encoding, using UTF-8 with replacement.")

        yield xmltodict.parse(xml)

    @dlt.transformer(parallelized=True)
    def individuals(data):
        """
        Transforms the individuals data from the consolidated list.
        """
        for item in data['CONSOLIDATED_LIST']['INDIVIDUALS']['INDIVIDUAL']:

            for key in ["DESIGNATION", "NATIONALITY", "LAST_DAY_UPDATED", "LIST_TYPE", "TITLE"]:
                if key in item:
                    item[key] = item[key]['VALUE']

            for key in [
                "INDIVIDUAL_ADDRESS",
                "INDIVIDUAL_ALIAS",
                "INDIVIDUAL_DATE_OF_BIRTH",
                "INDIVIDUAL_PLACE_OF_BIRTH",
                "INDIVIDUAL_DOCUMENT",
                'TITLE',
                'NATIONALITY',
                'DESIGNATION',
                'LAST_DAY_UPDATED'
            ]:
                if key in item:
                    if item[key] is None:
                        del item[key]
                    elif not isinstance(item[key], list):
                        item[key] = [item[key]]

            yield item

    @dlt.transformer(parallelized=True)
    def entities(data):
        """
        Transforms the entities data from the consolidated list.
        """
        for item in data["CONSOLIDATED_LIST"]["ENTITIES"]["ENTITY"]:

            for key in ["LIST_TYPE", "LAST_DAY_UPDATED"]:
                if key in item:
                    item[key] = item[key]["VALUE"]

            for key in [
                "ENTITY_ADDRESS",
                "ENTITY_ALIAS",
                "LAST_DAY_UPDATED",
            ]:
                if key in item:
                    if item[key] is None:
                        del item[key]
                    elif not isinstance(item[key], list):
                        item[key] = [item[key]]

            yield item

    consolidated_data = consolidated_list()

    return consolidated_data | individuals, consolidated_data | entities
