from typing import Any, List, Optional
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dlt.sources.helpers.requests import Response, Request

from datetime import datetime, timedelta


class YearMonthPathPaginator(BasePaginator):
    
    def __init__(self, initial_year: int = 2023, initial_month: int = 9):
        super().__init__()
        self.initial_date = (datetime(year=initial_year, month=initial_month, day=1) + timedelta(months=1)) - timedelta(days=1)
        self.page = self.initial_date

    def init_request(self, request: Request) -> None:
        
        # This will set the initial page number (e.g., page=1)
        request.url += f"/{self.page.year}/{self.page.month:02d}"
    
        self.update_request(request)

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        
        # Assuming the API returns an empty list when no more data is available
        if self.page.year == datetime.now().date.year and self.page.month == datetime.now().date.month:
            self._has_next_page = False
        else:
            self.page += timedelta(months=1)

    def update_request(self, request: Request) -> None:
        
        if request.params is None:
            request.params = {}
            
        request.params[self.page_param] = self.page
