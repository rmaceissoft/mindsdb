import re
import os
import datetime as dt
import ast
import base64
from collections import defaultdict
import pytz
import io

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

FILTERED_MESSAGE_HEADERS = [
    "content_type",
    "content_transfer_encoding",
    "date",
    "delivered_to",
    "from",    
    "message_id",
    "mime_version",
    "reply_to",
    "subject",
    "to",
    "x_feedback_id",
]


class GmailTable(APITable):
    def select(self, query: ast.Select) -> Response:
        conditions = extract_comparison_conditions(query.where)

        params = {}

        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError(f'OR is not supported')
            
            if arg1 in ['q', 'label_ids', 'include_spam_trash']:
                if op == '=':
                    value = arg2.split(',') if arg1 == 'label_ids' else arg2
                    # convert snake case to camel case
                    temp = re.split('_+', arg1)
                    key = temp[0] + ''.join(map(lambda x: x.title(), temp[1:]))
                    params[key] = value
                else:
                    raise NotImplementedError(f'Unknown operator: {op}')    
            else:
                raise NotImplementedError(f'Unknown clause: {arg1}')

        if query.limit is not None:
            params["maxResults"] = query.limit.value

        result = self.handler.call_gmail_api(method_name="messages", params=params)
        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            # add absent columns
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            # filter by columns
            result = result[columns]
        return result

    def get_columns(self):
        return ["id", "threadId", "labels"] + FILTERED_MESSAGE_HEADERS


class GmailHandler(APIHandler):
    """A class for handling connections and interactions with the GMail API.

    Attributes:
        service (googleapiclient.discovery.Resource): The `googleapiclient.discovery.Resource` object for interacting with GMail API.

    """
    data = []

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get("connection_data", {})

        self.connection_args = {}
        handler_config = Config().get("gmail_handler", {})
        for k in [
            "token",
            "refresh_token",
            "token_uri",
            "client_id",
            "client_secret",
            "scopes",
        ]:
            if k in args:
                self.connection_args[k] = args[k]
            elif f"GMAIL_{k.upper()}" in os.environ:
                self.connection_args[k] = os.environ[f"GMAIL_{k.upper()}"]
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.service = None
        self.is_connected = False

        messages = GmailTable(self)
        self._register_table("messages", messages)

    def connect(self):
        """Authenticate with the GMail API."""  # noqa

        if self.is_connected is True:
            return self.service
        creds = Credentials(**self.connection_args)
        self.service = build("gmail", "v1", credentials=creds)

        self.is_connected = True
        return self.service

    def check_connection(self) -> StatusResponse:

        response = StatusResponse(False)

        try:
            service = self.connect()
            service.users().getProfile(userId="me").execute()
            response.success = True

        except HttpError as e:
            response.error_message = (
                f"Error connecting to GMail api: {e.error_details}."
            )
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        method_name, params = FuncParser().from_string(query_string)

        df = self.call_gmail_api(method_name, params)

        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

    @classmethod
    def callback_get_details(cls, request_id, response, exception):
        # print(request_id, response, exception)
        if exception is None:
            print("callback_get_details without exception")
            # parsing headers
            headers = {}
            for header in response["payload"]["headers"]:
                # clean header name
                key = header["name"].lower().replace("-", "_").replace(" ", "")
                if key not in FILTERED_MESSAGE_HEADERS:
                    # skip unfiltered headers
                    continue
                headers[key] = header["value"]
            # parse and decode email content
            content = ''
            if 'data' in response['payload']['body'].keys():
                content += response['payload']['body']['data']
            else:
                for part in response['payload']['parts']:
                    content = part['body'].get('data', '') + content
            content_as_bytes = bytes(str(content),encoding='utf-8')
            body = base64.urlsafe_b64decode(content_as_bytes)

            row = {
                "id": response["id"],
                "labels": ', '.join(response.get("labelIds", [])),
                "threadId": response["threadId"],
                "body": body,
                **headers}
            cls.data.append(row)
        print(f"AFTER: data = {cls.data}")

    def call_gmail_api(self, method_name: str = None, params: dict = None):
        service = self.connect()
        method = getattr(service.users(), method_name)

        params["userId"] = "me"  # me -> authenticated user
        # pagination handle
        count_results = None
        if "maxResults" in params:
            count_results = params["maxResults"]
        # TODO: handle pagination properly
        self.__class__.data = []
        max_page_size = 500
        left = None

        while True:
            if count_results is not None:
                left = count_results - len(self.__class__.data)
                if left == 0:
                    break
                elif left < 0:
                    # got more results that we need
                    self.__class__.data = self.__class__.data[:left]
                    break

                if left > max_page_size:
                    params["maxResults"] = max_page_size
                else:
                    params["maxResults"] = left

            print(f"left = {left}")
            log.logger.debug(f">>>gmail in: {method_name}({params})")
            resp = method().list(**params).execute()
            results = resp.get(method_name, [])

            # limit output
            if left is not None:
                results = results[:left]

            #print("******************* RESPONSE **********************")
            #print(resp)
            # import pdb; pdb.set_trace()
            #print("******************* RESULTS **********************")
            #print(results)

            # use batch http request when info is missing
            # by the list method (only required for messages for now)
            if results and method_name == "messages":
                batch = service.new_batch_http_request()
                for item in results:
                    batch.add(
                        method().get(userId="me", id=item["id"]),
                        request_id=item["id"],
                        callback=self.__class__.callback_get_details,
                    )
                batch.execute()
            else:
                self.__class__.data.extend(results)

            # next page ?
            if count_results is not None and "nextPageToken" in resp:
                params["pageToken"] = resp["nextPageToken"]
            else:
                break

        df = pd.DataFrame(self.__class__.data)
        return df
