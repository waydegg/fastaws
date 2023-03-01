import json
from datetime import date
from typing import Dict, List

from fastaws.core import AwsClient
from fastaws.enums import Service


class SesClient(AwsClient):
    def __init__(
        self,
        *,
        access_key: str,
        secret_key: str,
        region: str,
    ):
        super().__init__(
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            service=Service.SES,
            host=f"email.{region}.amazonaws.com",
            version=date(year=2010, month=12, day=1),
        )

    async def list_identities(self) -> List[str]:
        """
        Get a list of all verified identities.

        https://docs.aws.amazon.com/ses/latest/APIReference/API_ListIdentities.html
        """
        res = await self._make_request(
            method="GET", endpoint="/", action="ListIdentities"
        )
        res_dict = json.loads(res.content.decode())
        res_result = res_dict["ListIdentitiesResponse"]["ListIdentitiesResult"]
        identities = res_result["Identities"]

        # TODO: handle next token

        return identities

    async def get_account(self) -> Dict:
        """
        Get account information.

        https://docs.aws.amazon.com/ses/latest/APIReference-V2/API_GetAccount.html
        """
        res = await self._make_request(
            method="GET", endpoint="/v2/email/account", action="GetAccount"
        )
        res_dict = json.loads(res.content.decode())

        return res_dict

    async def send_email(
        self, *, from_address: str, to_address: str, subject: str, body: str
    ) -> str:
        """
        Send an email.

        Returns a "MessageId".

        https://docs.aws.amazon.com/ses/latest/APIReference-V2/API_SendEmail.html
        """
        data = {
            "Content": {
                "Simple": {
                    "Subject": {"Charset": "UTF-8", "Data": subject},
                    "Body": {"Html": {"Charset": "UTF-8", "Data": body}},
                }
            },
            "Destination": {"ToAddresses": [to_address]},
            "FromEmailAddress": from_address,
        }

        res = await self._make_request(
            method="POST",
            endpoint="/v2/email/outbound-emails",
            action="SendEmail",
            data=data,
        )
        res_dict = json.loads(res.content.decode())
        message_id = res_dict["MessageId"]

        return message_id
